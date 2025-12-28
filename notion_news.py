import os
import yaml
import requests
import xml.etree.ElementTree as ET
from notion_client import Client
from datetime import datetime
from typing import List, Dict
import re

# =====================
# Configuration
# =====================
NOTION_TOKEN = os.environ["NOTION_TOKEN"]
DATABASE_ID = os.environ["DATABASE_ID"]

# Charger la config (ou utiliser des valeurs par défaut)
try:
    with open('config.yaml', 'r') as f:
        config = yaml.safe_load(f)
except:
    # Configuration par défaut si config.yaml n'existe pas
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
# Fonctions utilitaires
# =====================

def calculate_relevance(title: str, abstract: str) -> tuple:
    """
    Calcule le score de pertinence (1-5 étoiles) et les tags trouvés.
    
    Returns:
        (score, matching_keywords)
    """
    text = (title + " " + abstract).lower()
    matching_keywords = []
    score = 0
    
    # Vérifier les mots-clés haute priorité
    for keyword in config['keywords'].get('high_priority', []):
        if keyword.lower() in text:
            score = max(score, 5)
            matching_keywords.append(keyword)
    
    # Vérifier les mots-clés moyenne priorité
    for keyword in config['keywords'].get('medium_priority', []):
        if keyword.lower() in text:
            score = max(score, 3)
            matching_keywords.append(keyword)
    
    # Vérifier les mots-clés basse priorité
    for keyword in config['keywords'].get('low_priority', []):
        if keyword.lower() in text:
            score = max(score, 1)
            matching_keywords.append(keyword)
    
    return score if score > 0 else 1, matching_keywords

def get_stars_emoji(score: int) -> str:
    """Convertit un score en émojis étoiles."""
    stars = {
        5: "🔥🔥🔥🔥🔥",
        4: "⭐⭐⭐⭐",
        3: "⭐⭐⭐",
        2: "⭐⭐",
        1: "⭐"
    }
    return stars.get(score, "⭐")

def extract_arxiv_id(url: str) -> str:
    """Extrait l'ID ArXiv de l'URL."""
    match = re.search(r'(\d{4}\.\d{4,5})', url)
    return match.group(1) if match else ""

def get_pdf_url(arxiv_url: str) -> str:
    """Convertit l'URL ArXiv en URL PDF."""
    arxiv_id = extract_arxiv_id(arxiv_url)
    return f"https://arxiv.org/pdf/{arxiv_id}.pdf" if arxiv_id else arxiv_url

# =====================
# Fonctions principales
# =====================

def test_database_connection():
    """Vérifie la connexion à Notion."""
    try:
        db = notion.databases.retrieve(database_id=DATABASE_ID)
        print("✅ Connected to database:", db["id"])
        return True
    except Exception as e:
        print("❌ Cannot connect to database:", e)
        return False

def fetch_arxiv_articles(categories: List[str], max_results: int = 50) -> List[Dict]:
    """
    Récupère les articles ArXiv des 7 derniers jours.
    """
    all_entries = []
    
    # Date limite : 7 jours en arrière
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
                
                # Nettoyer le titre et l'abstract
                title = ' '.join(title_elem.text.split())
                abstract = ' '.join(summary_elem.text.split())
                link = link_elem.text
                
                # Parser la date
                published_date = datetime.fromisoformat(
                    published_elem.text.replace('Z', '+00:00')
                )
                
                # ⭐ Filtrer par date (7 derniers jours)
                if published_date < cutoff_date:
                    continue
                
                # Extraire les auteurs (max 5)
                author_list = [a.text for a in authors[:5]]
                authors_str = ', '.join(author_list)
                if len(authors) > 5:
                    authors_str += f" et al. ({len(authors)} authors)"
                
                # Calculer la pertinence
                relevance_score, keywords = calculate_relevance(title, abstract)
                
                # Filtrer selon le seuil minimum
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
    
    # Trier par pertinence puis par date
    all_entries.sort(key=lambda x: (x['relevance'], x['published']), reverse=True)
    
    print(f"\n📊 Total: {len(all_entries)} relevant articles")
    return all_entries

def fetch_existing_titles() -> set:
    """Récupère les titres existants dans Notion."""
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
    """Ajoute un article à Notion avec toutes les métadonnées."""
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
        
        # Ajouter l'abstract dans le corps de la page
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
    """Archive les articles les plus anciens."""
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
    
    # Test connexion
    if not test_database_connection():
        return
    
    # Configuration
    categories = config.get('arxiv_categories', ['gr-qc'])
    max_articles = config.get('max_articles', 10)
    
    print(f"\n📚 Monitoring categories: {', '.join(categories)}")
    print(f"🎯 Max articles to keep: {max_articles}")
    print(f"⭐ Minimum relevance: {config.get('min_relevance', 1)} stars\n")
    
    # Récupérer les articles
    articles = fetch_arxiv_articles(categories, max_results=30)
    
    if not articles:
        print("⚠️  No relevant articles found")
        return
    
    # Récupérer les titres existants
    existing = fetch_existing_titles()
    
    # Ajouter les nouveaux articles
    print(f"\n✨ Adding new articles (top {max_articles})...\n")
    new_count = 0
    
    for i, article in enumerate(articles[:max_articles], 1):
        if article['title'] not in existing:
            print(f"[{i}/{max_articles}]", end=" ")
            if add_entry(article):
                new_count += 1
        else:
            print(f"[{i}/{max_articles}] ⏭️  Already exists: {article['title'][:60]}...")
    
    # Statistiques
    print(f"\n{'=' * 70}")
    if new_count > 0:
        print(f"🎉 Added {new_count} new articles!")
    else:
        print(f"✅ No new articles (all up to date)")
    
    # Nettoyage
    print()
    trim_database(max_articles)
    
    print(f"{'=' * 70}")
    print("✅ Sync complete! Open Notion to see your research dashboard 🚀")
    print(f"{'=' * 70}\n")

if __name__ == "__main__":
    main()
