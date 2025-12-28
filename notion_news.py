import os
import requests
import xml.etree.ElementTree as ET
from notion_client import Client
from datetime import datetime, timedelta

# =====================
# Configuration
# =====================
NOTION_TOKEN = os.environ["NOTION_TOKEN"]
DATABASE_ID = os.environ["DATABASE_ID"]
ARXIV_CATEGORY = os.getenv("ARXIV_CATEGORY", "gr-qc")
K = int(os.getenv("K", 5))
SOURCE = os.getenv("SOURCE", f"arXiv {ARXIV_CATEGORY}")

notion = Client(auth=NOTION_TOKEN)

# =====================
# Functions
# =====================
def test_database_connection():
    """Check if we can access the Notion database."""
    try:
        db = notion.databases.retrieve(database_id=DATABASE_ID)
        print("✅ YES - Connected to database:", db["id"])
        response = notion.databases.query(database_id=DATABASE_ID, page_size=1)
        count = len(response.get("results", []))
        print(f"📊 Database currently has {count}+ pages")
        return True
    except Exception as e:
        print("❌ NO - Cannot connect to database:", e)
        import traceback
        traceback.print_exc()
        return False

def fetch_arxiv_articles(category, max_results=10):
    """Fetch recent articles from ArXiv API."""
    base_url = "http://export.arxiv.org/api/query"
    
    # Chercher les articles des 7 derniers jours
    params = {
        "search_query": f"cat:{category}",
        "sortBy": "submittedDate",
        "sortOrder": "descending",
        "max_results": max_results
    }
    
    print(f"📡 Querying ArXiv API for category: {category}")
    
    try:
        response = requests.get(base_url, params=params, timeout=30)
        response.raise_for_status()
        
        # Parse XML
        root = ET.fromstring(response.content)
        
        # Namespace pour ArXiv
        ns = {
            'atom': 'http://www.w3.org/2005/Atom',
            'arxiv': 'http://arxiv.org/schemas/atom'
        }
        
        entries = []
        for entry in root.findall('atom:entry', ns):
            title_elem = entry.find('atom:title', ns)
            link_elem = entry.find('atom:id', ns)
            published_elem = entry.find('atom:published', ns)
            
            if title_elem is not None and link_elem is not None and published_elem is not None:
                # Clean title (remove extra whitespace)
                title = ' '.join(title_elem.text.split())
                link = link_elem.text
                
                # Parse date
                published_str = published_elem.text
                published_date = datetime.fromisoformat(published_str.replace('Z', '+00:00'))
                
                entries.append({
                    'title': title,
                    'link': link,
                    'published': published_date
                })
        
        print(f"✅ Found {len(entries)} articles from ArXiv API")
        return entries
        
    except Exception as e:
        print(f"❌ Error fetching from ArXiv API: {e}")
        import traceback
        traceback.print_exc()
        return []

def fetch_existing_titles():
    """Fetch existing titles from the Notion database."""
    titles = set()
    has_more = True
    start_cursor = None
    
    try:
        while has_more:
            query_params = {
                "database_id": DATABASE_ID,
                "page_size": 100
            }
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
            
        print(f"📋 Found {len(titles)} existing titles in database")
            
    except Exception as e:
        print(f"❌ Error querying database: {e}")
        import traceback
        traceback.print_exc()
    
    return titles

def add_entry(entry):
    """Add a new article to the Notion database."""
    try:
        notion.pages.create(
            parent={"database_id": DATABASE_ID},
            properties={
                "Title": {"title": [{"text": {"content": entry['title']}}]},
                "URL": {"url": entry['link']},
                "Date": {"date": {"start": entry['published'].isoformat()}},
                "Source": {"rich_text": [{"text": {"content": SOURCE}}]},
,
            },
        )
        print(f"  ✅ Added: {entry['title'][:80]}...")
        return True
    except Exception as e:
        print(f"  ❌ Failed to add entry: {e}")
        return False

def trim_database():
    """Archive oldest entries if more than K."""
    try:
        response = notion.databases.query(
            database_id=DATABASE_ID,
            sorts=[{"property": "Date", "direction": "ascending"}],
            page_size=100
        )
    except Exception as e:
        print(f"❌ Error querying database for trim: {e}")
        return
    
    pages = response.get("results", [])
    to_archive = len(pages) - K
    
    if to_archive > 0:
        print(f"🧹 Archiving {to_archive} old entries...")
        for i in range(to_archive):
            try:
                page = pages[i]
                title_prop = page["properties"].get("Title", {}).get("title", [])
                title = title_prop[0]["text"]["content"] if title_prop else "Unknown"
                
                notion.pages.update(page_id=page["id"], archived=True)
                print(f"  📦 Archived: {title[:60]}...")
            except Exception as e:
                print(f"  ❌ Failed to archive page: {e}")
    else:
        print(f"✅ Database has {len(pages)} entries (max: {K}), no archiving needed")

def main():
    print("=" * 60)
    print(f"🚀 Starting Notion News Sync (K={K})")
    print("=" * 60)
    
    connected = test_database_connection()
    if not connected:
        print("❌ Stopping execution")
        return
    
    # Fetch articles from ArXiv API
    entries = fetch_arxiv_articles(ARXIV_CATEGORY, max_results=K*2)
    
    if not entries:
        print("⚠️  No entries found")
        return
    
    print(f"✅ Found {len(entries)} entries")
    
    print("\n📋 Fetching existing titles...")
    existing = fetch_existing_titles()
    
    print(f"\n✨ Processing top {K} entries...")
    new_count = 0
    processed = 0
    
    for entry in entries:
        if processed >= K:
            break
            
        if entry['title'] not in existing:
            print(f"[{processed+1}/{K}] New entry:")
            if add_entry(entry):
                new_count += 1
                processed += 1
        else:
            print(f"[{processed+1}/{K}] Already exists: {entry['title'][:60]}...")
            processed += 1
    
    if new_count > 0:
        print(f"\n🎉 Added {new_count} new entries!")
    else:
        print(f"\n✅ No new entries")
    
    print("\n🧹 Trimming database...")
    trim_database()
    
    print("\n✅ Done!")

if __name__ == "__main__":
    main()
