"""
collect.py
Collecte les articles des 5 médias et les accumule dans data/articles.json.
À être lancé régulièrement par GitHub Actions (ou manuellement).

À chaque lancement :
  1. Charge les articles déjà collectés (data/articles.json)
  2. Récupère les nouveaux depuis les flux RSS
  3. Fusionne et déduplique par GUID/lien
  4. Sauvegarde dans data/articles.json

Dépendance : pip install feedparser
"""

import json
from pathlib import Path
from datetime import datetime
from dataclasses import asdict

from rss_fetcher import fetch_all

DATA_DIR = Path("data")
ARTICLES_FILE = DATA_DIR / "articles.json"


def load_existing() -> list[dict]:
    """Charge les articles déjà collectés."""
    if ARTICLES_FILE.exists():
        with open(ARTICLES_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return []


def save(articles: list[dict]) -> None:
    """Sauvegarde les articles en JSON."""
    DATA_DIR.mkdir(exist_ok=True)
    with open(ARTICLES_FILE, "w", encoding="utf-8") as f:
        json.dump(articles, f, ensure_ascii=False, indent=2)


def deduplicate(all_articles: list[dict]) -> list[dict]:
    """Déduplique par GUID ou lien (garde la dernière version de chaque article)."""
    seen: dict[str, dict] = {}
    for art in all_articles:
        key = art.get("guid") or art.get("link") or art.get("title")
        if key not in seen:
            seen[key] = art
        else:
            # Si l'article est apparu deux fois, garde la version la plus récente
            # (au cas où le résumé aurait été mis à jour)
            if art.get("fetched_at", "") > seen[key].get("fetched_at", ""):
                seen[key] = art
    return list(seen.values())


def main():
    timestamp = datetime.now().isoformat()
    print(f"\n{'='*60}")
    print(f"Collecte RSS - {timestamp}")
    print(f"{'='*60}\n")

    # Charge les articles existants
    existing = load_existing()
    print(f"✓ Articles existants chargés : {len(existing)}")

    # Récupère les nouveaux
    print("\nRécupération des flux...")
    new_articles = fetch_all(with_full_text=False)
    print(f"✓ Nouveaux articles récupérés : {len(new_articles)}")

    # Convertit les Article dataclass en dict (pour le JSON)
    new_dicts = [asdict(a) for a in new_articles]

    # Fusionne et déduplique
    all_articles = existing + new_dicts
    unique = deduplicate(all_articles)

    # Trie par date décroissante (plus récents d'abord)
    unique.sort(key=lambda a: a.get("published") or "", reverse=True)

    # Sauvegarde
    save(unique)
    print(f"\n✓ Articles uniques : {len(unique)}")
    print(f"✓ Sauvegardé dans : {ARTICLES_FILE.resolve()}")
    print(f"\nRépartition par source :")

    by_source: dict[str, int] = {}
    for art in unique:
        src = art.get("source", "?")
        by_source[src] = by_source.get(src, 0) + 1

    for src, count in sorted(by_source.items()):
        print(f"  {src:20s} {count:4d} articles")

    print(f"\n{'='*60}\n")


if __name__ == "__main__":
    main()
