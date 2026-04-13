"""
main_prod.py — Wrapper de produção que monta o build React como arquivos estáticos.
Usado por app_react.py --prod via: uvicorn main_prod:app
"""
from __future__ import annotations

import os
from pathlib import Path

# Herda o app FastAPI já configurado com todas as rotas
from main import app  # noqa: F401 (re-exporta)

_dist = Path(os.environ.get("FISCAL_DIST", Path(__file__).resolve().parents[1] / "frontend" / "dist"))

if _dist.exists():
    from fastapi.responses import FileResponse
    from fastapi.staticfiles import StaticFiles

    _assets = _dist / "assets"
    if _assets.exists():
        app.mount("/assets", StaticFiles(directory=str(_assets)), name="assets")

    # Fallback SPA — qualquer rota desconhecida devolve index.html
    @app.get("/{full_path:path}", include_in_schema=False)
    def _spa_fallback(full_path: str):  # noqa: ARG001
        return FileResponse(str(_dist / "index.html"))
