"""Pytest configuration for backend integration tests."""
import sys
from pathlib import Path

# Ensure backend directory is on the path
backend_dir = Path(__file__).resolve().parent.parent
if str(backend_dir) not in sys.path:
    sys.path.insert(0, str(backend_dir))