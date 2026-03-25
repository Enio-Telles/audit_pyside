from __future__ import annotations

from importlib import util
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent
STUB_PATH = ROOT_DIR / "03_descricao_produtos.py"

if not STUB_PATH.exists():
    raise ImportError(f"Nao foi possivel localizar o modulo {STUB_PATH.name}")

_spec = util.spec_from_file_location("descricao_produtos_impl", STUB_PATH)
if _spec is None or _spec.loader is None:
    raise ImportError(f"Nao foi possivel carregar o modulo {STUB_PATH.name}")

_module = util.module_from_spec(_spec)
_spec.loader.exec_module(_module)

descricao_produtos = _module.descricao_produtos
gerar_descricao_produtos = _module.gerar_descricao_produtos

__all__ = ["descricao_produtos", "gerar_descricao_produtos"]
