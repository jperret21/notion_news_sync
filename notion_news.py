import os
import feedparser
from notion_client import Client
from datetime import datetime

NOTION_TOKEN = os.environ["NOTION_TOKEN"]
DATABASE_ID = os.environ["DATABASE_ID"]

RSS_URL = os.getenv(
    "RSS_URL",
    "http://export.arxiv.org/rss/astro-ph.CO"
)
K = int(os.getenv("K", 5))
SOURCE = os.getenv("SOURCE", "arXiv astro-ph.CO")

notion = Client(auth=NOTION_TOKEN)

def fetch_existing_titles():
    results = notion.databases.query(database_id=DATABASE_ID)["results"]
    return {
        r["properties"]["Title"]["title"][0]["plain_text"]
        for r in results if r["properties"]["Title"]["title"]
    }

def add_entry(entry):
    notion.pages.create(
        parent={"database_id": DATABASE_ID},
        properties={
            "Title": {
                "title": [{"text": {"content": entry.title}}]
            },
            "URL": {"url": entry.link},
            "Date": {
                "date": {
                    "start": datetime(*entry.published_parsed[:6]).isoformat()
                }
            },
            "Source": {
                "select": {"name": SOURCE}
            },
        },
    )

def trim_database():
    pages = notion.databases.query(
        database_id=DATABASE_ID,
        sorts=[{"property": "Date", "direction": "ascending"}],
    )["results"]

    while len(pages) > K:
        notion.pages.update(page_id=pages[0]["id"], archived=True)
        pages.pop(0)

def main():
    feed = feedparser.parse(RSS_URL)
    existing = fetch_existing_titles()

    for entry in feed.entries[:K]:
        if entry.title not in existing:
            add_entry(entry)

    trim_database()

if __name__ == "__main__":
    main()
