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

try:
    with open('config.yaml', 'r') as f:
        config = yaml.safe_load(f)
except:
    config = {
        'keywords': {
            'high_priority': ['gravitational', 'black hole', 'neutron star', 'LIGO', 'merger'],
            'medium_priority': ['cosmology', 'relativity', 'dark matter', 'spacetime'],
            'low_priority': ['numerical', 'metric']
        },
        'arxiv_categories': ['gr-qc', 'astro-ph.CO'],
        'days_lookback': 7,
        'max_articles': 20,  # Keep last 20 articles
        'top_n': 5  # Mark top 5 as priority
    }

notion = Client(auth=NOTION_TOKEN)

# =====================
# Functions
# =====================

def calculate_relevance(title: str, abstract: str) -> tuple:
    """Score 1-5 based on keywords."""
    text = (title + " " + abstract).lower()
    keywords = []
    score = 1  # Default
    
    for kw in config['keywords'].get('high_priority', []):
        if kw.lower() in text:
            score = 5
            keywords.append(kw)
    
    if score < 5:
        for kw in config['keywords'].get('medium_priority', []):
            if kw.lower() in text:
                score = max(score, 3)
                keywords.append(kw)
    
    if score < 3:
        for kw in config['keywords'].get('low_priority', []):
            if kw.lower() in text:
                score = max(score, 2)
                keywords.append(kw)
    
    return score, keywords

def fetch_arxiv(categories: List[str], days: int) -> List[Dict]:
    """Fetch ArXiv articles."""
    articles = []
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    headers = {'User-Agent': 'ArXiv-Dashboard/1.0'}
    
    print(f"📅 Looking for articles from last {days} days (since {cutoff.strftime('%Y-%m-%d')})\n")
    
    for i, cat in enumerate(categories):
        print(f"📡 {cat}...", end=" ")
        
        try:
            response = requests.get(
                "http://export.arxiv.org/api/query",
                params={
                    "search_query": f"cat:{cat}",
                    "sortBy": "submittedDate",
                    "sortOrder": "descending",
                    "max_results": 100  # Get more to ensure we have recent ones
                },
                headers=headers,
                timeout=30
            )
            
            root = ET.fromstring(response.content)
            ns = {'atom': 'http://www.w3.org/2005/Atom'}
            
            found_in_range = 0
            
            for entry in root.findall('atom:entry', ns):
                title = entry.find('atom:title', ns)
                link = entry.find('atom:id', ns)
                date = entry.find('atom:published', ns)
                abstract = entry.find('atom:summary', ns)
                authors = entry.findall('atom:author/atom:name', ns)
                
                if not all([title, link, date, abstract]):
                    continue
                
                pub_date = datetime.fromisoformat(date.text.replace('Z', '+00:00'))
                
                # Check date range
                if pub_date < cutoff:
                    continue
                
                found_in_range += 1
                
                title_text = ' '.join(title.text.split())
                abstract_text = ' '.join(abstract.text.split())
                
                score, keywords = calculate_relevance(title_text, abstract_text)
                
                arxiv_id = re.search(r'(\d{4}\.\d{4,5})', link.text)
                pdf = f"https://arxiv.org/pdf/{arxiv_id.group(1)}.pdf" if arxiv_id else link.text
                
                authors_str = ', '.join([a.text for a in authors[:3]])
                if len(authors) > 3:
                    authors_str += f" et al."
                
                articles.append({
                    'title': title_text,
                    'link': link.text,
                    'pdf': pdf,
                    'date': pub_date,
                    'abstract': abstract_text[:2000],
                    'authors': authors_str,
                    'category': cat,
                    'score': score,
                    'keywords': keywords
                })
            
            print(f"✅ {found_in_range} articles")
            
        except Exception as e:
            print(f"❌ Error: {e}")
        
        if i < len(categories) - 1:
            time.sleep(3)
    
    # Sort by score then date
    articles.sort(key=lambda x: (x['score'], x['date']), reverse=True)
    return articles

def add_to_notion(article: Dict, is_top: bool = False):
    """Add article to Notion."""
    stars = {5: "🔥🔥🔥🔥🔥", 4: "⭐⭐⭐⭐", 3: "⭐⭐⭐", 2: "⭐⭐", 1: "⭐"}[article['score']]
    priority = "🏆 TOP 5" if is_top else "📚 Reading List"
    
    try:
        notion.pages.create(
            parent={"database_id": DATABASE_ID},
            properties={
                "Title": {"title": [{"text": {"content": article['title']}}]},
                "URL": {"url": article['link']},
                "PDF": {"url": article['pdf']},
                "Date": {"date": {"start": article['date'].isoformat()}},
                "Category": {"rich_text": [{"text": {"content": article['category']}}]},
                "Authors": {"rich_text": [{"text": {"content": article['authors']}}]},
                "Relevance": {"select": {"name": stars}},
                "Priority": {"select": {"name": priority}},
                "Keywords": {"rich_text": [{"text": {"content": ', '.join(article['keywords'][:5])}}]},
            },
            children=[
                {"object": "block", "type": "heading_2", "heading_2": {"rich_text": [{"text": {"content": "Abstract"}}]}},
                {"object": "block", "type": "paragraph", "paragraph": {"rich_text": [{"text": {"content": article['abstract']}}]}},
                {"object": "block", "type": "divider", "divider": {}},
                {"object": "block", "type": "paragraph", "paragraph": {"rich_text": [
                    {"text": {"content": "📄 "}},
                    {"text": {"content": "Download PDF", "link": {"url": article['pdf']}}}
                ]}}
            ]
        )
        return True
    except Exception as e:
        print(f"    ❌ {e}")
        return False

def get_existing_titles():
    """Get existing titles."""
    titles = set()
    cursor = None
    
    while True:
        params = {"database_id": DATABASE_ID, "page_size": 100}
        if cursor:
            params["start_cursor"] = cursor
        
        response = notion.databases.query(**params)
        
        for page in response["results"]:
            title_prop = page["properties"].get("Title", {}).get("title", [])
            if title_prop:
                titles.add(title_prop[0]["text"]["content"])
        
        if not response.get("has_more"):
            break
        cursor = response["next_cursor"]
    
    return titles

def cleanup(max_keep: int):
    """Keep only most recent articles."""
    response = notion.databases.query(
        database_id=DATABASE_ID,
        sorts=[{"property": "Date", "direction": "ascending"}],
        page_size=100
    )
    
    pages = response["results"]
    if len(pages) > max_keep:
        for page in pages[:len(pages) - max_keep]:
            notion.pages.update(page_id=page["id"], archived=True)
        print(f"🧹 Archived {len(pages) - max_keep} old articles")

# =====================
# Main
# =====================

def main():
    print("\n" + "=" * 70)
    print("🌌 ArXiv Research Dashboard")
    print("=" * 70 + "\n")
    
    days = config.get('days_lookback', 7)
    max_articles = config.get('max_articles', 20)
    top_n = config.get('top_n', 5)
    
    # Fetch
    articles = fetch_arxiv(config['arxiv_categories'], days)
    
    print(f"\n📊 Found {len(articles)} total articles")
    
    if not articles:
        print("\n⚠️  No articles found. Try increasing days_lookback in config.yaml")
        return
    
    # Show top articles
    print(f"\n🏆 TOP {top_n} Articles:")
    for i, a in enumerate(articles[:top_n], 1):
        print(f"  [{i}] Score {a['score']}/5: {a['title'][:80]}...")
    
    # Get existing
    existing = get_existing_titles()
    print(f"\n📋 {len(existing)} articles already in Notion")
    
    # Add new ones
    print(f"\n✨ Adding new articles...\n")
    added = 0
    
    for i, article in enumerate(articles[:max_articles]):
        is_top = i < top_n
        
        if article['title'] not in existing:
            priority_marker = "🏆" if is_top else "  "
            print(f"  {priority_marker} {article['title'][:70]}...")
            if add_to_notion(article, is_top):
                added += 1
    
    print(f"\n🎉 Added {added} new articles")
    
    # Cleanup
    cleanup(max_articles)
    
    print("\n" + "=" * 70)
    print("✅ Done! Check Notion for your reading dashboard")
    print("=" * 70 + "\n")

if __name__ == "__main__":
    main()
