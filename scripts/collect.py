#!/usr/bin/env python3
"""
Convenience wrapper: python scripts/collect.py [args]

Equivalent to running `rent-collect [args]` after pip install -e .
"""

import sys
from pathlib import Path

# Ensure the src/ directory is on the path when running without installing
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from rent_collector.cli import main

if __name__ == "__main__":
    main()
