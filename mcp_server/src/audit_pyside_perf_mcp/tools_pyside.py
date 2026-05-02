"""
Tools para detectar padrões de performance ruins em PySide.
"""

from pathlib import Path
import re
from .config import Config


def register_pyside_tools(mcp):
    """Registra tools para auditoria PySide."""

    @mcp.tool()
    def detect_pyside_performance_smells() -> dict:
        """Detecta padrões comuns que degradam performance em apps PySide."""
        patterns = {
            "qtablewidget": ("QTableWidget", "Usar QTableView + modelo é mais escalável"),
            "fetchall": (".fetchall()", "fetchall() carrega tudo em memória; prefira iterator ou límite"),
            "select_star": ("SELECT *", "Trazer todas as colunas; especifique apenas as necessárias"),
            "processEvents": ("processEvents(", "Evita travamentos, mas melhor usar workers"),
            "sleep_in_ui": ("time.sleep(", "Bloqueia a thread principal; use QTimer ou workers"),
            "large_list_widget": ("QListWidget", "Sem paginação; prefira QListView + modelo"),
        }

        findings = []

        for path in Config.PROJECT_ROOT.rglob("*.py"):
            if any(part in Config.BLOCKED_DIRS for part in path.parts):
                continue

            try:
                text = path.read_text(encoding="utf-8", errors="ignore")
            except Exception:
                continue

            for name, (pattern, hint) in patterns.items():
                if pattern.lower() in text.lower():
                    # Encontra linhas aproximadas
                    for i, line in enumerate(text.split("\n"), 1):
                        if pattern.lower() in line.lower():
                            findings.append({
                                "file": str(path.relative_to(Config.PROJECT_ROOT)),
                                "line": i,
                                "pattern": name,
                                "code": line.strip()[:80],
                                "hint": hint,
                            })
                            break  # Uma linha por arquivo por padrão

        return {
            "ok": True,
            "findings_count": len(findings),
            "findings": findings,
        }

    @mcp.tool()
    def detect_qtableview_models() -> dict:
        """Lista classes derivadas de QAbstractTableModel."""
        models = []

        for path in Config.PROJECT_ROOT.rglob("*.py"):
            if any(part in Config.BLOCKED_DIRS for part in path.parts):
                continue

            try:
                text = path.read_text(encoding="utf-8", errors="ignore")
            except Exception:
                continue

            # Procura derivadas de QAbstractTableModel
            for match in re.finditer(r"class\s+(\w+)\s*\(\s*.*QAbstractTableModel", text):
                models.append({
                    "file": str(path.relative_to(Config.PROJECT_ROOT)),
                    "class": match.group(1),
                })

        return {
            "ok": True,
            "models_count": len(models),
            "models": models,
        }

    @mcp.tool()
    def detect_fetchall_in_ui_thread() -> dict:
        """Procura .fetchall() perto de código de UI (potencial gargalo)."""
        findings = []

        for path in Config.PROJECT_ROOT.rglob("*.py"):
            if any(part in Config.BLOCKED_DIRS for part in path.parts):
                continue

            try:
                text = path.read_text(encoding="utf-8", errors="ignore")
            except Exception:
                continue

            lines = text.split("\n")
            for i, line in enumerate(lines):
                if ".fetchall()" in line:
                    context = "\n".join(lines[max(0, i - 2) : min(len(lines), i + 3)])
                    findings.append({
                        "file": str(path.relative_to(Config.PROJECT_ROOT)),
                        "line": i + 1,
                        "code": line.strip()[:100],
                        "context": context,
                    })

        return {
            "ok": True,
            "findings_count": len(findings),
            "findings": findings,
        }

    @mcp.tool()
    def inspect_table_models() -> dict:
        """Analisa estrutura de QAbstractTableModel do projeto."""
        models_info = {}

        for path in Config.PROJECT_ROOT.rglob("*.py"):
            if any(part in Config.BLOCKED_DIRS for part in path.parts):
                continue

            try:
                text = path.read_text(encoding="utf-8", errors="ignore")
            except Exception:
                continue

            # Procura métodos críticos de modelo
            for match in re.finditer(r"class\s+(\w+)\s*\(\s*.*QAbstractTableModel", text):
                class_name = match.group(1)
                has_fetchmore = "fetchMore" in text
                has_canfetchmore = "canFetchMore" in text
                has_rowcount = "rowCount" in text

                models_info[f"{path.relative_to(Config.PROJECT_ROOT)}::{class_name}"] = {
                    "has_fetchMore": has_fetchmore,
                    "has_canFetchMore": has_canfetchmore,
                    "has_rowCount": has_rowcount,
                    "supports_lazy_loading": has_fetchmore and has_canfetchmore,
                }

        return {
            "ok": True,
            "models_count": len(models_info),
            "models": models_info,
        }
