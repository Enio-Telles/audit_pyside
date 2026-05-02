"""
Tools para profiling e medição de performance.
"""

import subprocess
from .config import Config


def register_perf_tools(mcp):
    """Registra tools de profiling e performance."""

    @mcp.tool()
    def run_pyinstrument_entrypoint(command: str = "app.py") -> str:
        """Mede performance de um script usando pyinstrument."""
        try:
            result = subprocess.run(
                ["python", "-m", "pyinstrument", command],
                cwd=Config.PROJECT_ROOT,
                capture_output=True,
                text=True,
                timeout=60,
            )
            return result.stdout or result.stderr or "No output"
        except subprocess.TimeoutExpired:
            return "Pyinstrument expirou após 60s"
        except FileNotFoundError:
            return "Pyinstrument não instalado (pip install pyinstrument)"
        except Exception as e:
            return f"Erro: {e}"

    @mcp.tool()
    def run_pytest_benchmark() -> str:
        """Executa benchmarks versionáveis com pytest-benchmark."""
        try:
            result = subprocess.run(
                ["python", "-m", "pytest", "--benchmark-only", "-q"],
                cwd=Config.PROJECT_ROOT,
                capture_output=True,
                text=True,
                timeout=180,
            )
            return result.stdout or result.stderr or "No benchmarks found"
        except subprocess.TimeoutExpired:
            return "Benchmark expirou após 180s"
        except Exception as e:
            return f"Erro: {e}"

    @mcp.tool()
    def profile_import_time() -> str:
        """Mede custo de importação/inicialização do projeto."""
        try:
            result = subprocess.run(
                ["python", "-X", "importtime", "-c", "import audit_pyside; import src"],
                cwd=Config.PROJECT_ROOT,
                capture_output=True,
                text=True,
                timeout=30,
            )
            return result.stderr or result.stdout or "No output"
        except Exception as e:
            return f"Erro: {e}"
