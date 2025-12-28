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
print("Loading config...")
NOTION_TOKEN = os.environ["NOTION_TOKEN"]
DATABASE_ID = os.environ["DATABASE_ID"]

try:
    with open('config.yaml', 'r') as f:
        config = yaml.safe_load(f)
    print("✅ Config loaded")
except:
    print("⚠️ Using default config")
    config = {
        'keywords': {
            'high_priority': ['gravitational', 'black hole', 'neutron star'],
            'medium_priority': ['cosmology', 'relativity'],
            'low_priority': []
        },
        'arxiv_categories': ['gr-qc', 'astro-ph.CO'],
        'days_lookback': 14,
        'max_articles': 20,
        'top_n': 5
    }

notion = Client(auth=NOTION_TOKEN)

def calculate_relevance(title: str, abstract: str) -> tuple:
    """Score 1-5 based on keywords."""
    text = (title + " " + abstract).lower()
    keywords = []
    score = 1
    
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
    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(days=days)
    headers = {'User-Agent': 'ArXiv-Dashboard/1.0'}
    
    for cat in categories:
        try:
            response = requests.get(
                "http://export.arxiv.org/api/query",
                params={
                    "search_query": f"cat:{cat}",
                    "sortBy": "submittedDate",
                    "sortOrder": "descending",
                    "max_results": 50
                },
                headers=headers,
                timeout=30
            )
            
            root = ET.fromstring(response.content)
            ns = {'': 'http://www.w3.org/2005/Atom'}
            entries = root.findall('entry', ns)
            
            count = 0
            for entry in entries:
                title_elem = entry.find('title', ns)
                link_elem = entry.find('id', ns)
                date_elem = entry.find('published', ns)
                abstract_elem = entry.find('summary', ns)
                author_elems = entry.findall('author/name', ns)
                
                if not all([title_elem is not None, link_elem is not None, 
                           date_elem is not None, abstract_elem is not None]):
                    continue
                
                pub_date = datetime.fromisoformat(date_elem.text.replace('Z', '+00:00'))
                
                if pub_date < cutoff:
                    continue
                
                title_text = ' '.join(title_elem.text.split())
                abstract_text = ' '.join(abstract_elem.text.split())
                
                score, keywords = calculate_relevance(title_text, abstract_text)
                
                arxiv_id = re.search(r'(\d{4}\.\d{4,5})', link_elem.text)
                pdf = f"https://arxiv.org/pdf/{arxiv_id.group(1)}.pdf" if arxiv_id else link_elem.text
                
                authors_str = ', '.join([a.text for a in author_elems[:3]])
                if len(author_elems) > 3:
                    authors_str += " et al."
                
                articles.append({
                    'title': title_text,
                    'link': link_elem.text,
                    'pdf': pdf,
                    'date': pub_date,
                    'abstract': abstract_text[:2000],
                    'authors': authors_str,
                    'category': cat,
                    'score': score,
                    'keywords': keywords
                })
                count += 1
            
            print(f"   {cat}: {count} articles")
            
        except Exception as e:
            print(f"   ❌ {cat}: Error - {e}")
        
        time.sleep(3)
    
    articles.sort(key=lambda x: (x['score'], x['date']), reverse=True)
    return articles

def add_to_notion(article: Dict, is_top: bool = False):
    """Add article to Notion with all properties."""
    stars = {5: "🔥🔥🔥🔥🔥", 4: "⭐⭐⭐⭐", 3: "⭐⭐⭐", 2: "⭐⭐", 1: "⭐"}[article['score']]
    
    # Add trophy to title for TOP 5
    title_text = f"🏆 {article['title']}" if is_top else article['title']
    
    try:
        notion.pages.create(
            parent={"database_id": DATABASE_ID},
            properties={
                "Title": {"title": [{"text": {"content": title_text}}]},
                "URL": {"url": article['link']},
                "Date": {"date": {"start": article['date'].isoformat()}},
                "Source": {"rich_text": [{"text": {"content": article['category']}}]},
                "PDF": {"url": article['pdf']},
                "Keywords": {"rich_text": [{"text": {"content": ', '.join(article['keywords'][:5]) if article['keywords'] else 'None'}}]},
                "Authors": {"rich_text": [{"text": {"content": article['authors']}}]},
                "Relevance": {"select": {"name": stars}},
            },
            children=[
                {"object": "block", "type": "callout", "callout": {
                    "icon": {"emoji": "🏆" if is_top else "📚"},
                    "rich_text": [{"text": {"content": f"{'TOP 5 - READ FIRST!' if is_top else 'Reading List'} | Score: {article['score']}/5"}}]
                }},
                {"object": "block", "type": "heading_2", "heading_2": {"rich_text": [{"text": {"content": "Abstract"}}]}},
                {"object": "block", "type": "paragraph", "paragraph": {"rich_text": [{"text": {"content": article['abstract']}}]}},
            ]
        )
        return True
    except Exception as e:
        print(f"      ❌ {e}")
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

def main():
    print("\n🌌 ArXiv Research Dashboard\n")
    
    days = config.get('days_lookback', 7)
    max_articles = config.get('max_articles', 20)
    top_n = config.get('top_n', 5)
    
    print(f"📡 Fetching last {days} days...")
    articles = fetch_arxiv(config['arxiv_categories'], days)
    print(f"✅ Found {len(articles)} articles\n")
    
    if not articles:
        print("⚠️  No articles found")
        return
    
    print(f"🏆 TOP {top_n}:")
    for i, a in enumerate(articles[:top_n], 1):
        print(f"  {i}. {a['title'][:70]}...")
    
    existing = get_existing_titles()
    
    print(f"\n✨ Adding {max_articles} articles ({len(existing)} already exist)...")
    added = 0
    
    for i, article in enumerate(articles[:max_articles], 1):
        is_top = i <= top_n
        
        if article['title'] not in existing:
            if add_to_notion(article, is_top):
                added += 1
    
    print(f"✅ Added {added} new articles")
    cleanup(max_articles)
    print()

if __name__ == "__main__":
    main()
