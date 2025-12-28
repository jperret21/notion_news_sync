import os
import yaml
import requests
import xml.etree.ElementTree as ET
from notion_client import Client
from datetime import datetime, timedelta, timezone
from typing import List, Dict
import re
import time

# =====================
# Configuration
# =====================
NOTION_TOKEN = os.environ["NOTION_TOKEN"]
DATABASE_ID = os.environ["DATABASE_ID"]

# Load config
try:
    with open('config.yaml', 'r') as f:
        config = yaml.safe_load(f)
except:
    config = {
        'keywords': {
            'high_priority': ['gravitational', 'black hole', 'neutron star'],
            'medium_priority': ['cosmology', 'relativity'],
            'low_priority': []
        },
        'arxiv_categories': ['gr-qc'],
        'max_articles': 10,
        'min_relevance': 0  # 0 = accept all
    }

notion = Client(auth=NOTION_TOKEN)

# =====================
# Core Functions
# =====================

def calculate_relevance(title: str, abstract: str) -> tuple:
    """Score article 1-5 stars based on keywords."""
    text = (title + " " + abstract).lower()
    keywords = []
    score = 0
    
    for kw in config['keywords'].get('high_priority', []):
        if kw.lower() in text:
            score = max(score, 5)
            keywords.append(kw)
    
    for kw in config['keywords'].get('medium_priority', []):
        if kw.lower() in text:
            score = max(score, 3)
            keywords.append(kw)
    
    for kw in config['keywords'].get('low_priority', []):
        if kw.lower() in text:
            score = max(score, 1)
            keywords.append(kw)
    
    return max(score, 1), keywords  # Minimum 1 star

def get_stars_emoji(score: int) -> str:
    """Convert score to emoji."""
    return {5: "🔥🔥🔥🔥🔥", 4: "⭐⭐⭐⭐", 3: "⭐⭐⭐", 2: "⭐⭐", 1: "⭐"}.get(score, "⭐")

def fetch_arxiv_articles(categories: List[str], days: int = 7) -> List[Dict]:
    """Fetch recent ArXiv articles."""
    articles = []
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    headers = {'User-Agent': 'ArXiv-Dashboard/1.0'}
    
    for i, cat in enumerate(categories):
        print(f"📡 Fetching {cat}...")
        
        url = "http://export.arxiv.org/api/query"
        params = {
            "search_query": f"cat:{cat}",
            "sortBy": "submittedDate",
            "sortOrder": "descending",
            "max_results": 50
        }
        
        try:
            response = requests.get(url, params=params, headers=headers, timeout=30)
            root = ET.fromstring(response.content)
            ns = {'atom': 'http://www.w3.org/2005/Atom'}
            
            for entry in root.findall('atom:entry', ns):
                title_elem = entry.find('atom:title', ns)
                link_elem = entry.find('atom:id', ns)
                date_elem = entry.find('atom:published', ns)
                abstract_elem = entry.find('atom:summary', ns)
                authors_elem = entry.findall('atom:author/atom:name', ns)
                
                if not all([title_elem, link_elem, date_elem, abstract_elem]):
                    continue
                
                title = ' '.join(title_elem.text.split())
                abstract = ' '.join(abstract_elem.text.split())
                link = link_elem.text
                pub_date = datetime.fromisoformat(date_elem.text.replace('Z', '+00:00'))
                
                # Filter by date
                if pub_date < cutoff:
                    continue
                
                # Calculate relevance
                score, keywords = calculate_relevance(title, abstract)
                min_score = config.get('min_relevance', 0)
                
                if score < min_score:
                    continue
                
                # Extract authors
                authors = ', '.join([a.text for a in authors_elem[:5]])
                if len(authors_elem) > 5:
                    authors += f" et al. ({len(authors_elem)} total)"
                
                # Get PDF URL
                arxiv_id = re.search(r'(\d{4}\.\d{4,5})', link)
                pdf_url = f"https://arxiv.org/pdf/{arxiv_id.group(1)}.pdf" if arxiv_id else link
                
                articles.append({
                    'title': title,
                    'link': link,
                    'pdf_url': pdf_url,
                    'published': pub_date,
                    'abstract': abstract[:2000],
                    'authors': authors,
                    'category': cat,
                    'score': score,
                    'stars': get_stars_emoji(score),
                    'keywords': ', '.join(keywords[:5])
                })
            
            print(f"  ✅ Found {len([a for a in articles if a['category'] == cat])} articles")
            
        except Exception as e:
            print(f"  ❌ Error: {e}")
        
        # Rate limit
        if i < len(categories) - 1:
            time.sleep(3)
    
    # Sort by score then date
    articles.sort(key=lambda x: (x['score'], x['published']), reverse=True)
    return articles

def add_to_notion(article: Dict) -> bool:
    """Add article to Notion."""
    try:
        notion.pages.create(
            parent={"database_id": DATABASE_ID},
            properties={
                "Title": {"title": [{"text": {"content": article['title']}}]},
                "URL": {"url": article['link']},
                "PDF": {"url": article['pdf_url']},
                "Date": {"date": {"start": article['published'].isoformat()}},
                "Category": {"rich_text": [{"text": {"content": article['category']}}]},
                "Authors": {"rich_text": [{"text": {"content": article['authors']}}]},
                "Relevance": {"select": {"name": article['stars']}},
                "Keywords": {"rich_text": [{"text": {"content": article['keywords']}}]},
            },
            children=[
                {"object": "block", "type": "heading_2", "heading_2": {"rich_text": [{"text": {"content": "Abstract"}}]}},
                {"object": "block", "type": "paragraph", "paragraph": {"rich_text": [{"text": {"content": article['abstract']}}]}},
                {"object": "block", "type": "divider", "divider": {}},
                {"object": "block", "type": "paragraph", "paragraph": {"rich_text": [
                    {"text": {"content": "📄 "}},
                    {"text": {"content": "Download PDF", "link": {"url": article['pdf_url']}}}
                ]}}
            ]
        )
        return True
    except Exception as e:
        print(f"  ❌ Error adding: {e}")
        return False

def get_existing_titles() -> set:
    """Get titles already in Notion."""
    titles = set()
    cursor = None
    
    while True:
        params = {"database_id": DATABASE_ID, "page_size": 100}
        if cursor:
            params["start_cursor"] = cursor
        
        response = notion.databases.query(**params)
        
        for page in response.get("results", []):
            title_prop = page["properties"].get("Title", {}).get("title", [])
            if title_prop:
                titles.add(title_prop[0]["text"]["content"])
        
        if not response.get("has_more"):
            break
        cursor = response.get("next_cursor")
    
    return titles

def cleanup_old_articles(max_keep: int):
    """Archive old articles."""
    response = notion.databases.query(
        database_id=DATABASE_ID,
        sorts=[{"property": "Date", "direction": "ascending"}],
        page_size=100
    )
    
    pages = response.get("results", [])
    to_archive = len(pages) - max_keep
    
    if to_archive > 0:
        print(f"🧹 Archiving {to_archive} old articles...")
        for page in pages[:to_archive]:
            notion.pages.update(page_id=page["id"], archived=True)

# =====================
# Main
# =====================

def main():
    print("🌌 ArXiv Research Dashboard Sync\n")
    
    # Config
    categories = config.get('arxiv_categories', ['gr-qc'])
    max_articles = config.get('max_articles', 10)
    
    print(f"📚 Categories: {', '.join(categories)}")
    print(f"🎯 Keep top {max_articles} articles")
    print(f"⭐ Min relevance: {config.get('min_relevance', 0)} stars\n")
    
    # Fetch articles
    articles = fetch_arxiv_articles(categories, days=7)
    print(f"\n📊 Total: {len(articles)} relevant articles found\n")
    
    if not articles:
        print("⚠️  No articles found")
        return
    
    # Get existing
    existing = get_existing_titles()
    print(f"📋 {len(existing)} articles already in Notion\n")
    
    # Add new ones
    print(f"✨ Adding top {max_articles}...\n")
    added = 0
    
    for article in articles[:max_articles]:
        if article['title'] not in existing:
            print(f"  {article['stars']} {article['title'][:70]}...")
            if add_to_notion(article):
                added += 1
    
    print(f"\n🎉 Added {added} new articles!")
    
    # Cleanup
    cleanup_old_articles(max_articles)
    print("\n✅ Done!\n")

if __name__ == "__main__":
    main()
