import os
import yaml
import requests
import xml.etree.ElementTree as ET
from notion_client import Client
from datetime import datetime, timedelta, timezone
from typing import List, Dict
import re
import time

print("=" * 70)
print("🚀 STARTING ArXiv Research Dashboard")
print("=" * 70)

# =====================
# Configuration
# =====================
print("\n📋 Step 1: Loading configuration...")

NOTION_TOKEN = os.environ.get("NOTION_TOKEN")
DATABASE_ID = os.environ.get("DATABASE_ID")

print(f"   NOTION_TOKEN: {'✅ Set' if NOTION_TOKEN else '❌ Missing'}")
print(f"   DATABASE_ID: {'✅ Set' if DATABASE_ID else '❌ Missing'}")

try:
    with open('config.yaml', 'r') as f:
        config = yaml.safe_load(f)
    print("   ✅ config.yaml loaded successfully")
except Exception as e:
    print(f"   ⚠️  config.yaml not found, using defaults: {e}")
    config = {
        'keywords': {
            'high_priority': ['gravitational', 'black hole', 'neutron star'],
            'medium_priority': ['cosmology', 'relativity'],
            'low_priority': []
        },
        'arxiv_categories': ['gr-qc'],
        'days_lookback': 14,
        'max_articles': 20,
        'top_n': 5
    }

print(f"   Categories: {config['arxiv_categories']}")
print(f"   Days lookback: {config['days_lookback']}")
print(f"   Max articles: {config['max_articles']}")
print(f"   Top N: {config['top_n']}")

notion = Client(auth=NOTION_TOKEN)

# =====================
# Functions
# =====================

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
    """Fetch ArXiv articles with detailed debugging."""
    print(f"\n📡 Step 2: Fetching articles from ArXiv API")
    print(f"   Categories to fetch: {categories}")
    print(f"   Looking back: {days} days")
    
    articles = []
    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(days=days)
    
    print(f"   Current UTC time: {now.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"   Cutoff date: {cutoff.strftime('%Y-%m-%d %H:%M:%S')}")
    
    headers = {'User-Agent': 'ArXiv-Dashboard/1.0'}
    
    for cat_idx, cat in enumerate(categories, 1):
        print(f"\n   [{cat_idx}/{len(categories)}] Fetching category: {cat}")
        
        url = "http://export.arxiv.org/api/query"
        params = {
            "search_query": f"cat:{cat}",
            "sortBy": "submittedDate",
            "sortOrder": "descending",
            "max_results": 50
        }
        
        print(f"      URL: {url}")
        print(f"      Params: {params}")
        print(f"      Making HTTP request...")
        
        try:
            response = requests.get(url, params=params, headers=headers, timeout=30)
            print(f"      ✅ HTTP {response.status_code}")
            print(f"      Response size: {len(response.content)} bytes")
            
            print(f"      Parsing XML...")
            root = ET.fromstring(response.content)
            ns = {'atom': 'http://www.w3.org/2005/Atom'}
            
            entries = root.findall('atom:entry', ns)
            print(f"      ✅ Found {len(entries)} entries in XML")
            
            if len(entries) == 0:
                print(f"      ⚠️  WARNING: No entries found in API response!")
                continue
            
            processed = 0
            passed_date_filter = 0
            passed_relevance_filter = 0
            
            print(f"      Processing entries...")
            
            for entry_idx, entry in enumerate(entries, 1):
                title_elem = entry.find('atom:title', ns)
                link_elem = entry.find('atom:id', ns)
                date_elem = entry.find('atom:published', ns)
                abstract_elem = entry.find('atom:summary', ns)
                authors_elem = entry.findall('atom:author/atom:name', ns)
                
                if not all([title_elem, link_elem, date_elem, abstract_elem]):
                    print(f"         [{entry_idx}] ⚠️  Skipping: missing elements")
                    continue
                
                processed += 1
                
                # Parse date
                pub_date = datetime.fromisoformat(date_elem.text.replace('Z', '+00:00'))
                days_old = (now - pub_date).days
                
                # Show first 3 articles in detail
                if entry_idx <= 3:
                    title_preview = ' '.join(title_elem.text.split())[:70]
                    print(f"\n         [{entry_idx}] Article details:")
                    print(f"            Title: {title_preview}...")
                    print(f"            Published: {pub_date.strftime('%Y-%m-%d %H:%M:%S')}")
                    print(f"            Days old: {days_old}")
                    print(f"            Cutoff check: pub_date ({pub_date}) >= cutoff ({cutoff}) = {pub_date >= cutoff}")
                
                # Date filter
                if pub_date < cutoff:
                    if entry_idx <= 3:
                        print(f"            ❌ FILTERED: Too old (before cutoff)")
                    continue
                
                passed_date_filter += 1
                if entry_idx <= 3:
                    print(f"            ✅ Passed date filter")
                
                # Extract data
                title_text = ' '.join(title_elem.text.split())
                abstract_text = ' '.join(abstract_elem.text.split())
                
                # Calculate relevance
                score, keywords = calculate_relevance(title_text, abstract_text)
                
                if entry_idx <= 3:
                    print(f"            Relevance score: {score}/5")
                    print(f"            Keywords found: {keywords if keywords else 'None'}")
                
                # Relevance filter (if min_relevance exists in config)
                min_relevance = config.get('min_relevance', 0)
                if score < min_relevance:
                    if entry_idx <= 3:
                        print(f"            ❌ FILTERED: Score {score} < min {min_relevance}")
                    continue
                
                passed_relevance_filter += 1
                if entry_idx <= 3:
                    print(f"            ✅ Passed relevance filter")
                
                # Extract authors
                authors_str = ', '.join([a.text for a in authors_elem[:3]])
                if len(authors_elem) > 3:
                    authors_str += f" et al."
                
                # Get PDF URL
                arxiv_id = re.search(r'(\d{4}\.\d{4,5})', link_elem.text)
                pdf = f"https://arxiv.org/pdf/{arxiv_id.group(1)}.pdf" if arxiv_id else link_elem.text
                
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
                
                if entry_idx <= 3:
                    print(f"            ✅ ADDED to results")
            
            print(f"\n      Summary for {cat}:")
            print(f"         Total entries in XML: {len(entries)}")
            print(f"         Processed (valid structure): {processed}")
            print(f"         Passed date filter: {passed_date_filter}")
            print(f"         Passed relevance filter: {passed_relevance_filter}")
            print(f"         Added to results: {len([a for a in articles if a['category'] == cat])}")
            
        except Exception as e:
            print(f"      ❌ ERROR: {e}")
            import traceback
            traceback.print_exc()
        
        # Rate limit
        if cat_idx < len(categories):
            print(f"      ⏳ Waiting 3 seconds before next category...")
            time.sleep(3)
    
    print(f"\n   Sorting articles by score and date...")
    articles.sort(key=lambda x: (x['score'], x['date']), reverse=True)
    
    print(f"   ✅ Total articles collected: {len(articles)}")
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
        print(f"         ❌ Error: {e}")
        return False

def get_existing_titles():
    """Get existing titles."""
    print(f"\n📋 Step 3: Checking existing articles in Notion...")
    titles = set()
    cursor = None
    page_count = 0
    
    while True:
        params = {"database_id": DATABASE_ID, "page_size": 100}
        if cursor:
            params["start_cursor"] = cursor
        
        response = notion.databases.query(**params)
        page_count += 1
        
        for page in response["results"]:
            title_prop = page["properties"].get("Title", {}).get("title", [])
            if title_prop:
                titles.add(title_prop[0]["text"]["content"])
        
        if not response.get("has_more"):
            break
        cursor = response["next_cursor"]
    
    print(f"   ✅ Found {len(titles)} existing articles (fetched {page_count} pages)")
    return titles

def cleanup(max_keep: int):
    """Keep only most recent articles."""
    print(f"\n🧹 Step 5: Cleaning up old articles...")
    print(f"   Max to keep: {max_keep}")
    
    response = notion.databases.query(
        database_id=DATABASE_ID,
        sorts=[{"property": "Date", "direction": "ascending"}],
        page_size=100
    )
    
    pages = response["results"]
    print(f"   Current total: {len(pages)} articles")
    
    if len(pages) > max_keep:
        to_archive = len(pages) - max_keep
        print(f"   Archiving {to_archive} oldest articles...")
        for page in pages[:to_archive]:
            notion.pages.update(page_id=page["id"], archived=True)
        print(f"   ✅ Archived {to_archive} articles")
    else:
        print(f"   ✅ No cleanup needed")

def main():
    days = config.get('days_lookback', 14)
    max_articles = config.get('max_articles', 20)
    top_n = config.get('top_n', 5)
    
    # Fetch
    articles = fetch_arxiv(config['arxiv_categories'], days)
    
    print(f"\n📊 RESULTS SUMMARY:")
    print(f"   Total articles found: {len(articles)}")
    
    if not articles:
        print(f"\n⚠️  NO ARTICLES FOUND!")
        print(f"   Possible reasons:")
        print(f"   1. ArXiv hasn't published in the last {days} days (holidays/weekends)")
        print(f"   2. API request failed")
        print(f"   3. All articles filtered out by date or relevance")
        print(f"\n💡 Try:")
        print(f"   - Increase days_lookback in config.yaml")
        print(f"   - Check the debug output above")
        return
    
    # Show top
    print(f"\n🏆 TOP {top_n} Articles by relevance:")
    for i, a in enumerate(articles[:top_n], 1):
        print(f"   [{i}] Score {a['score']}/5 | {a['date'].strftime('%Y-%m-%d')} | {a['title'][:60]}...")
    
    # Get existing
    existing = get_existing_titles()
    
    # Add new
    print(f"\n✨ Step 4: Adding new articles to Notion...")
    print(f"   Will add up to {max_articles} articles")
    added = 0
    skipped = 0
    
    for i, article in enumerate(articles[:max_articles], 1):
        is_top = i <= top_n
        marker = "🏆" if is_top else "📚"
        
        if article['title'] not in existing:
            print(f"   [{i}/{max_articles}] {marker} Adding: {article['title'][:60]}...")
            if add_to_notion(article, is_top):
                added += 1
        else:
            print(f"   [{i}/{max_articles}] ⏭️  Skipping (exists): {article['title'][:60]}...")
            skipped += 1
    
    print(f"\n   Added: {added} new articles")
    print(f"   Skipped: {skipped} existing articles")
    
    # Cleanup
    cleanup(max_articles)
    
    print("\n" + "=" * 70)
    print("✅ SYNC COMPLETE!")
    print("=" * 70 + "\n")

if __name__ == "__main__":
    main()
