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
    print(f"\n📡 Fetching articles (last {days} days)...\n")
    
    articles = []
    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(days=days)
    headers = {'User-Agent': 'ArXiv-Dashboard/1.0'}
    
    for cat in categories:
        print(f"   {cat}...", end=" ")
        
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
            
            # ⭐ FIX: Use default namespace (no prefix)
            # ArXiv uses Atom which is the default namespace
            ns = {'': 'http://www.w3.org/2005/Atom'}
            
            # ⭐ Find entries WITHOUT namespace prefix
            entries = root.findall('entry', ns)
            
            # If that doesn't work, try with explicit namespace
            if not entries:
                ns_explicit = {'atom': 'http://www.w3.org/2005/Atom'}
                entries = root.findall('atom:entry', ns_explicit)
            
            # If still nothing, try without any namespace
            if not entries:
                entries = root.findall('.//{http://www.w3.org/2005/Atom}entry')
            
            print(f"{len(entries)} entries", end=" ")
            
            count = 0
            for entry in entries:
                # ⭐ Try multiple ways to find elements
                title = (entry.find('title', ns) or 
                        entry.find('{http://www.w3.org/2005/Atom}title') or
                        entry.find('atom:title', {'atom': 'http://www.w3.org/2005/Atom'}))
                
                link = (entry.find('id', ns) or 
                       entry.find('{http://www.w3.org/2005/Atom}id'))
                
                date = (entry.find('published', ns) or 
                       entry.find('{http://www.w3.org/2005/Atom}published'))
                
                abstract = (entry.find('summary', ns) or 
                           entry.find('{http://www.w3.org/2005/Atom}summary'))
                
                authors = (entry.findall('author/name', ns) or 
                          entry.findall('{http://www.w3.org/2005/Atom}author/{http://www.w3.org/2005/Atom}name'))
                
                if not all([title, link, date, abstract]):
                    continue
                
                pub_date = datetime.fromisoformat(date.text.replace('Z', '+00:00'))
                
                if pub_date < cutoff:
                    continue
                
                title_text = ' '.join(title.text.split())
                abstract_text = ' '.join(abstract.text.split())
                
                score, keywords = calculate_relevance(title_text, abstract_text)
                
                arxiv_id = re.search(r'(\d{4}\.\d{4,5})', link.text)
                pdf = f"https://arxiv.org/pdf/{arxiv_id.group(1)}.pdf" if arxiv_id else link.text
                
                authors_str = ', '.join([a.text for a in authors[:3]])
                if len(authors) > 3:
                    authors_str += " et al."
                
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
                count += 1
            
            print(f"→ {count} in range")
            
        except Exception as e:
            print(f"ERROR: {e}")
        
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
