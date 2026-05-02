"""
Tools para auditoria da qualidade do projeto: ruff, pytest, typecheck, estrutura.
"""

import subprocess
from pathlib import Path
from .config import Config


def run_cmd(args: list[str], timeout: int = 120) -> str:
    """Executa comando no projeto de forma segura."""
    try:
        result = subprocess.run(
            args,
            cwd=Config.PROJECT_ROOT,
            text=True,
            capture_output=True,
            timeout=timeout,
            shell=False,
        )
        output = []
        if result.stdout:
            output.append(result.stdout)
        if result.stderr:
            output.append(result.stderr)
        return "\n".join(output) or f"Exit code: {result.returncode}"
    except subprocess.TimeoutExpired:
        return f"Comando expirou após {timeout}s"
    except Exception as e:
        return f"Erro: {e}"


def register_project_tools(mcp):
    """Registra tools de qualidade do projeto."""

    @mcp.tool()
    def run_ruff() -> str:
        """Executa ruff check no projeto audit_pyside."""
        return run_cmd(["python", "-m", "ruff", "check", "."])

    @mcp.tool()
    def run_pytest(marker: str = "") -> str:
        """Executa testes com pytest. marker: opcional (ex: 'not gui_smoke')."""
        args = ["python", "-m", "pytest", "-q"]
        if marker:
            args.extend(["-m", marker])
        return run_cmd(args, timeout=180)

    @mcp.tool()
    def run_typecheck() -> str:
        """Executa type checking (pyright ou mypy se existir)."""
        # Tenta pyright primeiro
        result = subprocess.run(
            ["pyright", "."],
            cwd=Config.PROJECT_ROOT,
            capture_output=True,
            text=True,
            timeout=60,
        )
        if result.returncode == 0 or "error" in result.stderr.lower():
            return result.stdout or result.stderr or "No issues"

        # Fallback para mypy
        return run_cmd(["python", "-m", "mypy", "src"], timeout=60)

    @mcp.tool()
    def project_tree(max_files: int = 200) -> str:
        """Lista estrutura de arquivos do projeto (ignorando cache/venv)."""
        ignored = {".git", ".venv", "__pycache__", ".pytest_cache", ".mypy_cache", "dist", "build"}
        files = []

        for path in Config.PROJECT_ROOT.rglob("*"):
            if any(part in ignored for part in path.parts):
                continue
            if path.is_file():
                files.append(str(path.relative_to(Config.PROJECT_ROOT)))
            if len(files) >= max_files:
                break

        return "\n".join(sorted(files))

    @mcp.tool()
    def detect_large_files(min_mb: float = 1.0) -> dict:
        """Detecta arquivos grandes suspeitos."""
        large_files = []

        for path in Config.PROJECT_ROOT.rglob("*"):
            if any(part in {".venv", "__pycache__", ".git"} for part in path.parts):
                continue
            if path.is_file():
                size_mb = path.stat().st_size / (1024 * 1024)
                if size_mb >= min_mb:
                    large_files.append({
                        "file": str(path.relative_to(Config.PROJECT_ROOT)),
                        "size_mb": round(size_mb, 2),
                    })

        return {
            "ok": True,
            "count": len(large_files),
            "files": sorted(large_files, key=lambda x: x["size_mb"], reverse=True),
        }
