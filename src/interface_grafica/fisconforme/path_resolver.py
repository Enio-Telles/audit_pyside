"""
Resolução de caminhos para o módulo integrado Fisconforme.

Localização: src/interface_grafica/fisconforme/path_resolver.py
Hierarquia:  parents[0] = fisconforme/
             parents[1] = interface_grafica/
             parents[2] = src/
             parents[3] = Sistema_pysisde/  <- ROOT_DIR
"""
from __future__ import annotations

import sys
from pathlib import Path


def get_resource_path(rel_path: str) -> Path:
    """Retorna caminho absoluto para um recurso, compatível com PyInstaller."""
    if getattr(sys, "_MEIPASS", None):
        base = Path(sys._MEIPASS)  # type: ignore[attr-defined]
    else:
        # Em desenvolvimento, a raiz do projeto fica em parents[3]
        base = Path(__file__).resolve().parents[3]
    return base / rel_path


def get_root_dir() -> Path:
    """Retorna o diretório raiz do projeto Sistema_pysisde."""
    return get_resource_path("")


def get_env_path() -> Path:
    """Retorna o caminho para o arquivo .env na raiz do projeto."""
    return get_resource_path(".env")


def get_modelo_path() -> Path:
    """Retorna o caminho para o modelo de notificação (embutido no pacote)."""
    return Path(__file__).parent / "modelo_notificacao_fisconforme_n_atendido.txt"


def get_logo_path() -> Path:
    """Retorna o caminho para o logo SEFIN (fallback para assets no pacote)."""
    logo = get_resource_path("assets/logo_sefin.png")
    if logo.exists():
        return logo
    return logo
