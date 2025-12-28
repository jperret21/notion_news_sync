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
        response = notion.databases.query(database_id=DATABASE_ID, page_size=1)
        count = len(response.get("results", []))
        print(f"Database currently has {count} pages (showing sample)")
        return True
    except Exception as e:
        print("❌ NO - Cannot connect to database:", e)
        return False

def fetch_existing_titles():
    """Fetch existing titles from the Notion database (v2.x compatible)."""
    try:
        response = notion.databases.query(database_id=DATABASE_ID)
    except Exception as e:
        print("Error querying database:", e)
        return set()

    results = response.get("results", [])
    titles = set()
    for r in results:
        title_prop = r["properties"].get("Title", {}).get("title", [])
        if title_prop:
            titles.add(title_prop[0]["text"]["content"])
    return titles

def add_entry(entry):
    """Add a new article to the Notion database."""
    notion.pages.create(
        parent={"database_id": DATABASE_ID},
        properties={
            "Title": {"title": [{"text": {"content": entry.title}}]},
            "URL": {"url": entry.link},
            "Date": {"date": {"start": datetime(*entry.published_parsed[:6]).isoformat()}},
            "Source": {"select": {"name": SOURCE}},
        },
    )

def trim_database():
    """Archive oldest entries if more than K."""
    try:
        response = notion.databases.query(
            database_id=DATABASE_ID,
            sorts=[{"property": "Date", "direction": "ascending"}],
            page_size=100
        )
    except Exception as e:
        print("Error querying database for trim:", e)
        return

    pages = response.get("results", [])

    while len(pages) > K:
        notion.pages.update(page_id=pages[0]["id"], archived=True)
        pages.pop(0)

def main():
    connected = test_database_connection()
    if not connected:
        print("Stopping execution because database connection failed.")
        return

    feed = feedparser.parse(RSS_URL)
    existing = fetch_existing_titles()

    for entry in feed.entries[:K]:
        if entry.title not in existing:
            add_entry(entry)

    trim_database()

if __name__ == "__main__":
    main()
