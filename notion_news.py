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
    print(f"\n📡 Fetching articles (last {days} days)...")
    
    articles = []
    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(days=days)
    
    print(f"Current time: {now}")
    print(f"Cutoff: {cutoff}")
    print()
    
    headers = {'User-Agent': 'ArXiv-Dashboard/1.0'}
    
    for cat in categories:
        print(f"Category: {cat}")
        
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
            
            print(f"  HTTP status: {response.status_code}")
            
            root = ET.fromstring(response.content)
            
            # Use default namespace
            ns = {'': 'http://www.w3.org/2005/Atom'}
            entries = root.findall('entry', ns)
            
            print(f"  Found {len(entries)} entries")
            
            count = 0
            for idx, entry in enumerate(entries, 1):
                
                # ⭐ FIX: Extract elements correctly with namespace
                title_elem = entry.find('title', ns)
                link_elem = entry.find('id', ns)
                date_elem = entry.find('published', ns)
                abstract_elem = entry.find('summary', ns)
                author_elems = entry.findall('author/name', ns)
                
                if idx <= 3:
                    print(f"\n  Entry #{idx}: {title_elem.text[:50] if title_elem is not None and title_elem.text else 'NO TITLE'}...")
                    print(f"    Title: {'✅' if title_elem is not None else '❌'}")
                    print(f"    Link: {'✅' if link_elem is not None else '❌'}")
                    print(f"    Date: {'✅' if date_elem is not None else '❌'}")
                    print(f"    Abstract: {'✅' if abstract_elem is not None else '❌'}")
                
                # ⭐ Check if all elements exist
                if not all([title_elem is not None, link_elem is not None, 
                           date_elem is not None, abstract_elem is not None]):
                    if idx <= 3:
                        print(f"    ❌ Skipping: missing elements")
                    continue
                
                # Parse date
                pub_date = datetime.fromisoformat(date_elem.text.replace('Z', '+00:00'))
                days_old = (now - pub_date).days
                
                if idx <= 3:
                    print(f"    Published: {pub_date.strftime('%Y-%m-%d')}")
                    print(f"    Days old: {days_old}")
                    print(f"    Pass cutoff: {pub_date >= cutoff}")
                
                if pub_date < cutoff:
                    if idx <= 3:
                        print(f"    ❌ Filtered: too old")
                    continue
                
                if idx <= 3:
                    print(f"    ✅ In date range!")
                
                title_text = ' '.join(title_elem.text.split())
                abstract_text = ' '.join(abstract_elem.text.split())
                
                score, keywords = calculate_relevance(title_text, abstract_text)
                
                if idx <= 3:
                    print(f"    Score: {score}/5 | Keywords: {keywords}")
                
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
                
                if idx <= 3:
                    print(f"    ✅ ADDED!")
            
            print(f"\n  Summary: {count} articles in date range\n")
            
        except Exception as e:
            print(f"  ❌ ERROR: {e}")
            import traceback
            traceback.print_exc()
        
        time.sleep(3)
    
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
    
    days = config.get('days_lookback', 14)
    max_articles = config.get('max_articles', 20)
    top_n = config.get('top_n', 5)
    
    articles = fetch_arxiv(config['arxiv_categories'], days)
    
    print(f"\n📊 Total: {len(articles)} articles\n")
    
    if not articles:
        print("⚠️  No articles found")
        return
    
    print(f"🏆 TOP {top_n}:")
    for i, a in enumerate(articles[:top_n], 1):
        print(f"  [{i}] {a['score']}/5 | {a['date'].strftime('%Y-%m-%d')} | {a['title'][:70]}...")
    
    existing = get_existing_titles()
    print(f"\n📋 {len(existing)} already in Notion")
    
    print(f"\n✨ Adding new articles...\n")
    added = 0
    
    for i, article in enumerate(articles[:max_articles], 1):
        is_top = i <= top_n
        
        if article['title'] not in existing:
            marker = "🏆" if is_top else "📚"
            print(f"  {marker} {article['title'][:65]}...")
            if add_to_notion(article, is_top):
                added += 1
    
    print(f"\n🎉 Added {added} new articles")
    cleanup(max_articles)
    print("\n✅ Done!\n")

if __name__ == "__main__":
    main()
