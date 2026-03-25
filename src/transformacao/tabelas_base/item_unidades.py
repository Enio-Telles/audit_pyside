from __future__ import annotations

from importlib import util
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent
STUB_PATH = ROOT_DIR / "01_item_unidades.py"

if not STUB_PATH.exists():
    raise ImportError(f"Não foi possível localizar o módulo {STUB_PATH.name}")

_spec = util.spec_from_file_location("item_unidades_impl", STUB_PATH)
if _spec is None or _spec.loader is None:
    raise ImportError(f"Não foi possível carregar o módulo {STUB_PATH.name}")

_module = util.module_from_spec(_spec)
_spec.loader.exec_module(_module)

item_unidades = _module.item_unidades
gerar_item_unidades = _module.gerar_item_unidades

__all__ = ["item_unidades", "gerar_item_unidades"]
