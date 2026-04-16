from importlib import reload
import src.utilitarios.perf_monitor as pm
print('orig _root_dir:', pm._root_dir())
from types import SimpleNamespace

def fake_root():
    from pathlib import Path
    return Path('C:/tmp_fake_root')

# monkeypatch replacement
pm._root_dir = fake_root
print('patched _root_dir returns:', pm._root_dir())
print('caminho_log_performance:', pm.caminho_log_performance())
