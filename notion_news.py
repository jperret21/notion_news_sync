import os
import yaml
import requests
import xml.etree.ElementTree as ET
from notion_client import Client
from datetime import datetime, timedelta
from typing import List, Dict
import re

# =====================
# Configuration
# =====================
NOTION_TOKEN = os.environ["NOTION_TOKEN"]
DATABASE_ID = os.environ["DATABASE_ID"]

# Load config (or use default values)
try:
    with open('config.yaml', 'r') as f:
        config = yaml.safe_load(f)
except:
    # Default configuration if config.yaml doesn't exist
    config = {
        'keywords': {
            'high_priority': ['gravitational waves', 'black hole'],
            'medium_priority': ['cosmology', 'dark matter'],
            'low_priority': []
        },
        'arxiv_categories': ['gr-qc'],
        'max_articles': 10,
        'min_relevance': 1
    }

notion = Client(auth=NOTION_TOKEN)

# =====================
# Utility Functions
# =====================

def calculate_relevance(title: str, abstract: str) -> tuple:
    """
    Calculate relevance score (1-5 stars) and found tags.
    
    Returns:
        (score, matching_keywords)
    """
    text = (title + " " + abstract).lower()
    matching_keywords = []
    score = 0
    
    # Check high priority keywords
    for keyword in config['keywords'].get('high_priority', []):
        if keyword.lower() in text:
            score = max(score, 5)
            matching_keywords.append(keyword)
    
    # Check medium priority keywords
    for keyword in config['keywords'].get('medium_priority', []):
        if keyword.lower() in text:
            score = max(score, 3)
            matching_keywords.append(keyword)
    
    # Check low priority keywords
    for keyword in config['keywords'].get('low_priority', []):
        if keyword.lower() in text:
            score = max(score, 1)
            matching_keywords.append(keyword)
    
    return score if score > 0 else 1, matching_keywords

def get_stars_emoji(score: int) -> str:
    """Convert score to star emojis."""
    stars = {
        5: "🔥🔥🔥🔥🔥",
        4: "⭐⭐⭐⭐",
        3: "⭐⭐⭐",
        2: "⭐⭐",
        1: "⭐"
    }
    return stars.get(score, "⭐")

def extract_arxiv_id(url: str) -> str:
    """Extract ArXiv ID from URL."""
    match = re.search(r'(\d{4}\.\d{4,5})', url)
    return match.group(1) if match else ""

def get_pdf_url(arxiv_url: str) -> str:
    """Convert ArXiv URL to PDF URL."""
    arxiv_id = extract_arxiv_id(arxiv_url)
    return f"https://arxiv.org/pdf/{arxiv_id}.pdf" if arxiv_id else arxiv_url

# =====================
# Main Functions
# =====================

def test_database_connection():
    """Check Notion connection."""
    try:
        db = notion.databases.retrieve(database_id=DATABASE_ID)
        print("✅ Connected to database:", db["id"])
        return True
    except Exception as e:
        print("❌ Cannot connect to database:", e)
        return False

def fetch_arxiv_articles(categories: List[str], max_results: int = 50) -> List[Dict]:
    """
    Fetch ArXiv articles from the last 7 days.
    """
    all_entries = []
    
    # Cutoff date: 7 days ago
    cutoff_date = datetime.now() - timedelta(days=7)
    
    for category in categories:
        print(f"📡 Querying ArXiv for: {category} (last 7 days)")
        
        base_url = "http://export.arxiv.org/api/query"
        params = {
            "search_query": f"cat:{category}",
            "sortBy": "submittedDate",
            "sortOrder": "descending",
            "max_results": max_results
        }
        
        try:
            response = requests.get(base_url, params=params, timeout=30)
            response.raise_for_status()
            
            root = ET.fromstring(response.content)
            ns = {
                'atom': 'http://www.w3.org/2005/Atom',
                'arxiv': 'http://arxiv.org/schemas/atom'
            }
            
            for entry in root.findall('atom:entry', ns):
                title_elem = entry.find('atom:title', ns)
                link_elem = entry.find('atom:id', ns)
                published_elem = entry.find('atom:published', ns)
                summary_elem = entry.find('atom:summary', ns)
                authors = entry.findall('atom:author/atom:name', ns)
                
                if not all([title_elem, link_elem, published_elem, summary_elem]):
                    continue
                
                # Clean title and abstract
                title = ' '.join(title_elem.text.split())
                abstract = ' '.join(summary_elem.text.split())
                link = link_elem.text
                
                # Parse date
                published_date = datetime.fromisoformat(
                    published_elem.text.replace('Z', '+00:00')
                )
                
                # ⭐ Filter by date (last 7 days)
                if published_date < cutoff_date:
                    continue
                
                # Extract authors (max 5)
                author_list = [a.text for a in authors[:5]]
                authors_str = ', '.join(author_list)
                if len(authors) > 5:
                    authors_str += f" et al. ({len(authors)} authors)"
                
                # Calculate relevance
                relevance_score, keywords = calculate_relevance(title, abstract)
                
                # Filter by minimum threshold
                if relevance_score < config.get('min_relevance', 1):
                    continue
                
                all_entries.append({
                    'title': title,
                    'link': link,
                    'pdf_url': get_pdf_url(link),
                    'published': published_date,
                    'abstract': abstract[:2000],
                    'authors': authors_str,
                    'category': category,
                    'relevance': relevance_score,
                    'stars': get_stars_emoji(relevance_score),
                    'keywords': ', '.join(keywords[:5]) if keywords else ""
                })
            
            print(f"  ✅ Found {len([e for e in all_entries if e['category'] == category])} relevant articles in last 7 days")
            
        except Exception as e:
            print(f"  ❌ Error fetching {category}: {e}")
    
    # Sort by relevance then date
    all_entries.sort(key=lambda x: (x['relevance'], x['published']), reverse=True)
    
    print(f"\n📊 Total: {len(all_entries)} relevant articles")
    return all_entries

def fetch_existing_titles() -> set:
    """Fetch existing titles from Notion."""
    titles = set()
    has_more = True
    start_cursor = None
    
    try:
        while has_more:
            query_params = {"database_id": DATABASE_ID, "page_size": 100}
            if start_cursor:
                query_params["start_cursor"] = start_cursor
            
            response = notion.databases.query(**query_params)
            results = response.get("results", [])
            
            for r in results:
                title_prop = r["properties"].get("Title", {}).get("title", [])
                if title_prop:
                    titles.add(title_prop[0]["text"]["content"])
            
            has_more = response.get("has_more", False)
            start_cursor = response.get("next_cursor")
        
        print(f"📋 Found {len(titles)} existing articles in Notion")
    except Exception as e:
        print(f"❌ Error fetching existing titles: {e}")
    
    return titles

def add_entry(entry: Dict) -> bool:
    """Add article to Notion with all metadata."""
    try:
        properties = {
            "Title": {"title": [{"text": {"content": entry['title']}}]},
            "URL": {"url": entry['link']},
            "PDF": {"url": entry['pdf_url']},
            "Date": {"date": {"start": entry['published'].isoformat()}},
            "Category": {"rich_text": [{"text": {"content": entry['category']}}]},
            "Authors": {"rich_text": [{"text": {"content": entry['authors']}}]},
            "Relevance": {"select": {"name": entry['stars']}},
            "Keywords": {"rich_text": [{"text": {"content": entry['keywords']}}]},
        }
        
        # Add abstract in page body
        children = [
            {
                "object": "block",
                "type": "heading_2",
                "heading_2": {
                    "rich_text": [{"type": "text", "text": {"content": "Abstract"}}]
                }
            },
            {
                "object": "block",
                "type": "paragraph",
                "paragraph": {
                    "rich_text": [{"type": "text", "text": {"content": entry['abstract']}}]
                }
            },
            {
                "object": "block",
                "type": "divider",
                "divider": {}
            },
            {
                "object": "block",
                "type": "paragraph",
                "paragraph": {
                    "rich_text": [
                        {"type": "text", "text": {"content": "📄 "}},
                        {
                            "type": "text",
                            "text": {"content": "Download PDF", "link": {"url": entry['pdf_url']}}
                        }
                    ]
                }
            }
        ]
        
        notion.pages.create(
            parent={"database_id": DATABASE_ID},
            properties=properties,
            children=children
        )
        
        print(f"  ✅ {entry['stars']} {entry['title'][:70]}...")
        return True
        
    except Exception as e:
        print(f"  ❌ Failed to add: {e}")
        return False

def trim_database(max_articles: int):
    """Archive oldest articles."""
    try:
        response = notion.databases.query(
            database_id=DATABASE_ID,
            sorts=[{"property": "Date", "direction": "ascending"}],
            page_size=100
        )
        
        pages = response.get("results", [])
        to_archive = len(pages) - max_articles
        
        if to_archive > 0:
            print(f"🧹 Archiving {to_archive} old articles...")
            for page in pages[:to_archive]:
                notion.pages.update(page_id=page["id"], archived=True)
            print(f"  ✅ Archived {to_archive} articles")
        else:
            print(f"✅ Database size OK ({len(pages)}/{max_articles})")
            
    except Exception as e:
        print(f"❌ Error trimming database: {e}")

# =====================
# Main
# =====================

def main():
    print("=" * 70)
    print("🌌 ArXiv Research Dashboard Sync")
    print("=" * 70)
    
    # Test connection
    if not test_database_connection():
        return
    
    # Configuration
    categories = config.get('arxiv_categories', ['gr-qc'])
    max_articles = config.get('max_articles', 10)
    
    print(f"\n📚 Monitoring categories: {', '.join(categories)}")
    print(f"🎯 Max articles to keep: {max_articles}")
    print(f"⭐ Minimum relevance: {config.get('min_relevance', 1)} stars\n")
    
    # Fetch articles
    articles = fetch_arxiv_articles(categories, max_results=50)
    
    if not articles:
        print("⚠️  No relevant articles found")
        return
    
    # Fetch existing titles
    existing = fetch_existing_titles()
    
    # Add new articles
    print(f"\n✨ Adding new articles (top {max_articles})...\n")
    new_count = 0
    
    for i, article in enumerate(articles[:max_articles], 1):
        if article['title'] not in existing:
            print(f"[{i}/{max_articles}]", end=" ")
            if add_entry(article):
                new_count += 1
        else:
            print(f"[{i}/{max_articles}] ⏭️  Already exists: {article['title'][:60]}...")
    
    # Statistics
    print(f"\n{'=' * 70}")
    if new_count > 0:
        print(f"🎉 Added {new_count} new articles!")
    else:
        print(f"✅ No new articles (all up to date)")
    
    # Cleanup
    print()
    trim_database(max_articles)
    
    print(f"{'=' * 70}")
    print("✅ Sync complete! Open Notion to see your research dashboard 🚀")
    print(f"{'=' * 70}\n")

if __name__ == "__main__":
    main()
