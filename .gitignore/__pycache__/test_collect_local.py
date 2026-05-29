#!/usr/bin/env python3
"""
test_collect_local.py

Lance une collecte locale immédiatement (sans attendre GitHub Actions).
Utile pour tester avant de mettre en place l'automation.

Usage :
    python test_collect_local.py
"""

import sys
from pathlib import Path

# Ajoute le dossier courant au chemin pour importer rss_fetcher
sys.path.insert(0, str(Path(__file__).parent))

# Importe et lance la fonction principale de collect
from collect import main

if __name__ == "__main__":
    main()
