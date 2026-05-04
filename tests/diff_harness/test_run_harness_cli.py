"""Testa o CLI run_harness_cli.py em modo --dry-run."""
import subprocess
import sys
from pathlib import Path

CLI = Path(__file__).resolve().parents[2] / "tests" / "diff_harness" / "run_harness_cli.py"


def test_dry_run_exit_zero() -> None:
    result = subprocess.run(
        [sys.executable, str(CLI), "--dry-run", "--cnpj", "99999999999999"],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, f"stderr: {result.stderr}\nstdout: {result.stdout}"
    assert "APROVADO" in result.stdout or "dry-run" in result.stdout.lower()


def test_dry_run_gera_relatorio(tmp_path: Path) -> None:
    result = subprocess.run(
        [
            sys.executable, str(CLI),
            "--dry-run",
            "--cnpj", "99999999999999",
            "--out", str(tmp_path),
        ],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0
    relatorios = list(tmp_path.glob("*.txt"))
    assert len(relatorios) >= 1
