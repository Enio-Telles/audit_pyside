from pathlib import Path
import importlib
pm = importlib.import_module('src.utilitarios.perf_monitor')
print('pm._root_dir():', pm._root_dir())
print('project paths PROJECT_ROOT:', importlib.import_module('src.utilitarios.project_paths').PROJECT_ROOT)
