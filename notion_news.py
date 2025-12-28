import os
import feedparser
from notion_client import Client
from datetime import datetime

# =====================
# Configuration
# =====================
NOTION_TOKEN = os.environ["NOTION_TOKEN"]
DATABASE_ID = os.environ["DATABASE_ID"]
RSS_URL = os.getenv("RSS_URL", "http://export.arxiv.org/rss/astro-ph.CO")
K = int(os.getenv("K", 5))
SOURCE = os.getenv("SOURCE", "arXiv astro-ph.CO")

notion = Client(auth=NOTION_TOKEN)

# =====================
# Functions
# =====================
def test_database_connection():
    """Check if we can access the Notion database and print YES or NO."""
    try:
        db = notion.databases.retrieve(database_id=DATABASE_ID)
        print("✅ YES - Connected to database:", db["id"])
        # Also print number of pages currently in the database
        response = notion.databases.query(
            **{"database_id": DATABASE_ID, "page_size": 1}
        )
        count = len(response.get("results", []))
        print(f"Database currently has {count} pages (showing sample)")
        return True
    except Exception as e:
        print("❌ NO - Cannot connect to database:", e)
        print(f"Error type: {type(e).__name__}")
        import traceback
        traceback.print_exc()
        return False

def fetch_existing_titles():
    """Fetch existing titles from the Notion database."""
    titles = set()
    has_more = True
    start_cursor = None
    
    try:
        while has_more:
            # Construire les paramètres de requête
            query_kwargs = {
                "database_id": DATABASE_ID,
                "page_size": 100
            }
            if start_cursor:
                query_kwargs["start_cursor"] = start_cursor
            
            # Appeler avec **kwargs pour être compatible avec toutes versions
            response = notion.databases.query(**query_kwargs)
            results = response.get("results", [])
            
            for r in results:
                title_prop = r["properties"].get("Title", {}).get("title", [])
                if title_prop:
                    titles.add(title_prop[0]["text"]["content"])
            
            has_more = response.get("has_more", False)
            start_cursor = response.get("next_cursor")
            
    except AttributeError as e:
        print(f"❌ AttributeError in fetch_existing_titles: {e}")
        print(f"notion.databases type: {type(notion.databases)}")
        print(f"Available methods: {dir(notion.databases)}")
        import traceback
        traceback.print_exc()
    except Exception as e:
        print(f"❌ Error querying database: {e}")
        import traceback
        traceback.print_exc()
    
    return titles

def add_entry(entry):
    """Add a new article to the Notion database."""
    try:
        notion.pages.create(
            **{
                "parent": {"database_id": DATABASE_ID},
                "properties": {
                    "Title": {"title": [{"text": {"content": entry.title}}]},
                    "URL": {"url": entry.link},
                    "Date": {"date": {"start": datetime(*entry.published_parsed[:6]).isoformat()}},
                    "Source": {"select": {"name": SOURCE}},
                },
            }
        )
        print(f"✅ Added: {entry.title}")
    except Exception as e:
        print(f"❌ Failed to add entry: {e}")

def trim_database():
    """Archive oldest entries if more than K."""
    try:
        response = notion.databases.query(
            **{
                "database_id": DATABASE_ID,
                "sorts": [{"property": "Date", "direction": "ascending"}],
                "page_size": 100
            }
        )
    except Exception as e:
        print("Error querying database for trim:", e)
        return
    
    pages = response.get("results", [])
    to_archive = len(pages) - K
    
    if to_archive > 0:
        print(f"🧹 Archiving {to_archive} old entries...")
        for i in range(to_archive):
            try:
                notion.pages.update(
                    **{"page_id": pages[i]["id"], "archived": True}
                )
                print(f"  ✅ Archived page {i+1}/{to_archive}")
            except Exception as e:
                print(f"  ❌ Failed to archive page: {e}")

def main():
    print(f"🚀 Starting Notion News Sync (K={K})")
    print(f"📦 Using notion-client version: {notion.client_info if hasattr(notion, 'client_info') else 'unknown'}")
    
    connected = test_database_connection()
    if not connected:
        print("❌ Stopping execution because database connection failed.")
        return
    
    print(f"📰 Fetching RSS feed: {RSS_URL}")
    feed = feedparser.parse(RSS_URL)
    print(f"Found {len(feed.entries)} entries in feed")
    
    print("📋 Fetching existing titles from Notion...")
    existing = fetch_existing_titles()
    print(f"Found {len(existing)} existing titles")
    
    new_count = 0
    for entry in feed.entries[:K]:
        if entry.title not in existing:
            add_entry(entry)
            new_count += 1
    
    print(f"✨ Added {new_count} new entries")
    
    print("🧹 Trimming database...")
    trim_database()
    
    print("✅ Done!")

if __name__ == "__main__":
    main()
