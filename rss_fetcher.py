"""
rss_fetcher.py
Récupère les articles publiés dans les flux RSS de 5 grands médias français,
avec leurs métadonnées (titre, lien, résumé, date, auteurs, catégories, image).

Dépendance requise :
    pip install feedparser

Dépendances optionnelles :
    pip install trafilatura   # pour extraire le texte intégral des articles

Usage rapide :
    python rss_fetcher.py            # récupère tout et écrit articles.json
ou, en bibliothèque :
    from rss_fetcher import fetch_all
    articles = fetch_all()
"""

from __future__ import annotations

import concurrent.futures
import hashlib
import html
import json
import re
import socket
import time
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from typing import Optional

import feedparser

# ---------------------------------------------------------------------------
# Configuration des flux.
# Plusieurs URL candidates par média : on essaie la 1re, et on se rabat sur les
# suivantes si elle ne répond pas. Les URL RSS bougent souvent, d'où ce filet.
# ---------------------------------------------------------------------------
FEEDS: dict[str, list[str]] = {
    "Le Monde": [
        "https://www.lemonde.fr/rss/une.xml",
        "https://www.lemonde.fr/actualite-en-continu/rss_full.xml",
    ],
    "Le Figaro": [
        "https://www.lefigaro.fr/rss/figaro_actualites.xml",
        "https://www.lefigaro.fr/rss/figaro_actualite-france.xml",
    ],
    "Libération": [
        "https://www.liberation.fr/arc/outboundfeeds/rss-all/?outputType=xml",
        "https://www.liberation.fr/arc/outboundfeeds/rss/?outputType=xml",
    ],
    "Mediapart": [
        "https://www.mediapart.fr/articles/feed",
    ],
    "Valeurs actuelles": [
        "https://www.valeursactuelles.com/feed/",
        "https://www.valeursactuelles.com/feed",
    ],
}

# Mets une URL/email de contact réels en production (politesse + déblocage).
USER_AGENT = (
    "Mozilla/5.0 (compatible; RevueDePresseBot/0.1; +https://example.org/contact)"
)

_TAG_RE = re.compile(r"<[^>]+>")
_WS_RE = re.compile(r"\s+")


def strip_html(raw: str) -> str:
    """Retire les balises HTML et décode les entités pour un texte propre."""
    if not raw:
        return ""
    text = _TAG_RE.sub(" ", raw)
    text = html.unescape(text)
    return _WS_RE.sub(" ", text).strip()


# ---------------------------------------------------------------------------
# Modèle de données
# ---------------------------------------------------------------------------
@dataclass
class Article:
    source: str
    title: str
    link: str
    summary: str
    published: Optional[str]                       # ISO 8601 (UTC)
    authors: list[str] = field(default_factory=list)
    categories: list[str] = field(default_factory=list)
    guid: str = ""
    image: Optional[str] = None
    full_text: Optional[str] = None
    fetched_at: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )

    @property
    def id(self) -> str:
        """Identifiant stable, basé sur le guid (sinon le lien, sinon le titre)."""
        base = self.guid or self.link or self.title
        return hashlib.sha1(base.encode("utf-8")).hexdigest()[:16]


# ---------------------------------------------------------------------------
# Extraction d'une entrée RSS -> Article
# ---------------------------------------------------------------------------
def _to_iso(entry) -> Optional[str]:
    for key in ("published_parsed", "updated_parsed"):
        t = entry.get(key)
        if t:
            return datetime(*t[:6], tzinfo=timezone.utc).isoformat()
    return None


def _extract_image(entry) -> Optional[str]:
    media = entry.get("media_content") or entry.get("media_thumbnail")
    if media and isinstance(media, list) and media[0].get("url"):
        return media[0]["url"]
    for link in entry.get("links", []):
        if link.get("rel") == "enclosure" and "image" in (link.get("type") or ""):
            return link.get("href")
    return None


def _authors(entry) -> list[str]:
    if entry.get("authors"):
        return [a.get("name", "").strip() for a in entry.authors if a.get("name")]
    if entry.get("author"):
        return [entry.author.strip()]
    return []


def _categories(entry) -> list[str]:
    return [t.get("term", "").strip() for t in entry.get("tags", []) if t.get("term")]


def parse_entry(source: str, entry) -> Article:
    return Article(
        source=source,
        title=strip_html(entry.get("title", "")),
        link=entry.get("link", ""),
        summary=strip_html(entry.get("summary", "")),
        published=_to_iso(entry),
        authors=_authors(entry),
        categories=_categories(entry),
        guid=entry.get("id", "") or entry.get("link", ""),
        image=_extract_image(entry),
    )


# ---------------------------------------------------------------------------
# Récupération
# ---------------------------------------------------------------------------
def fetch_feed(source: str, urls: list[str]) -> list[Article]:
    """Essaie chaque URL candidate jusqu'à en trouver une qui renvoie des entrées."""
    for url in urls:
        try:
            parsed = feedparser.parse(
                url,
                agent=USER_AGENT,
                request_headers={"Cache-Control": "no-cache"},
            )
        except Exception as exc:                       # noqa: BLE001
            print(f"[{source}] erreur réseau sur {url}: {exc}")
            continue

        if parsed.entries:
            print(f"[{source}] {len(parsed.entries)} entrées via {url}")
            return [parse_entry(source, e) for e in parsed.entries]

        reason = getattr(parsed, "bozo_exception", "flux vide")
        print(f"[{source}] flux inexploitable ({reason}) : {url}")

    print(f"[{source}] aucun flux exploitable.")
    return []


def fetch_all(
    feeds: dict[str, list[str]] = FEEDS,
    max_workers: int = 5,
    timeout: int = 15,
    with_full_text: bool = False,
) -> list[Article]:
    """Récupère tous les flux en parallèle, dédoublonne et trie par date."""
    socket.setdefaulttimeout(timeout)

    articles: list[Article] = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {
            pool.submit(fetch_feed, source, urls): source
            for source, urls in feeds.items()
        }
        for fut in concurrent.futures.as_completed(futures):
            articles.extend(fut.result())

    # Déduplication (un article peut revenir via guid + lien, ou doublons de flux)
    seen: set[str] = set()
    unique: list[Article] = []
    for art in articles:
        if art.id in seen:
            continue
        seen.add(art.id)
        unique.append(art)

    if with_full_text:
        enrich_full_text(unique)

    unique.sort(key=lambda a: a.published or "", reverse=True)
    return unique


def enrich_full_text(articles: list[Article], pause: float = 0.5) -> None:
    """Texte intégral via trafilatura (optionnel : plus lent, plus complet)."""
    try:
        import trafilatura
    except ImportError:
        print("trafilatura absent : on conserve uniquement les résumés RSS.")
        return

    for art in articles:
        if not art.link:
            continue
        downloaded = trafilatura.fetch_url(art.link)
        if downloaded:
            art.full_text = trafilatura.extract(downloaded, include_comments=False)
        time.sleep(pause)   # politesse : ne pas marteler les serveurs


def save_json(articles: list[Article], path: str = "articles.json") -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump([asdict(a) for a in articles], f, ensure_ascii=False, indent=2)


if __name__ == "__main__":
    arts = fetch_all(with_full_text=False)
    print(f"\n{len(arts)} articles récupérés au total\n")
    by_source: dict[str, int] = {}
    for a in arts:
        by_source[a.source] = by_source.get(a.source, 0) + 1
    for src, n in sorted(by_source.items()):
        print(f"  {src:20s} {n:3d} articles")
    save_json(arts)
    print("\n-> articles.json écrit.")
