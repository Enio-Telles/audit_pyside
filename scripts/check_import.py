import importlib
import traceback
import sys
import os
from pathlib import Path

# Make the project's `src` directory importable when running this script
ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
sys.path.insert(0, str(SRC))

try:
    importlib.import_module('interface_grafica.services.aggregation_service')
    print('import-ok')
except Exception:
    traceback.print_exc()
    print('import-failed')
    # Return non-zero so CI / callers detect import failure
    sys.exit(1)
