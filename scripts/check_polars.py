import polars as pl
import sys
import json

print('POLARS_VERSION=' + getattr(pl, '__version__', 'unknown'))
print('POLARS_FILE=' + getattr(pl, '__file__', 'builtin or unknown'))
print('PYTHON_PATH:')
print(json.dumps(sys.path, indent=2))
