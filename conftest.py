"""
Pytest configuration for Clotho Simulation Engine.

This file ensures that the 'core' package is importable when running pytest
from the project root directory. No more sys.path.insert hacks needed!
"""

import sys
from pathlib import Path

# Add project root to Python path so 'core' package is importable
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))
