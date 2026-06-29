"""Add project root to sys.path so `from traceset.xxx import ...` works."""
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
