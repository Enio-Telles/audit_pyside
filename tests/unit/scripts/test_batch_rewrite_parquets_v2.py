from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path

import polars as pl
import pytest


SCRIPT_PATH = Path(__file__).resolve().parents[3] / "scripts" / "batch_rewrite_parquets_v2.py"
SPEC = importlib.util.spec_from_file_location("batch_rewrite_parquets_v2", SCRIPT_PATH)
assert SPEC and SPEC.loader
batch_cli = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = batch_cli
SPEC.loader.exec_module(batch_cli)


def _write_parquet(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame({"cfop": ["5102"], "id_agrupado": ["id_1"]}).write_parquet(path)


def test_discover_parquets_preserva_arvore_relativa(tmp_path: Path) -> None:
    input_root = tmp_path / "v1"
    output_root = tmp_path / "v2"
    _write_parquet(input_root / "analises" / "produtos" / "produtos_final.parquet")
    _write_parquet(input_root / "arquivos_parquet" / "c170_xml.parquet")

    items = batch_cli.discover_parquets(input_root, output_root)

    assert [Path(item.output_path).relative_to(output_root) for item in items] == [
        Path("analises/produtos/produtos_final.parquet"),
        Path("arquivos_parquet/c170_xml.parquet"),
    ]


def test_discover_rejeita_saida_igual_entrada(tmp_path: Path) -> None:
    input_root = tmp_path / "v1"
    input_root.mkdir()

    with pytest.raises(ValueError, match="output_root deve ser diferente"):
        batch_cli.discover_parquets(input_root, input_root)


def test_main_dry_run_gera_relatorios_sem_escrever_v2(tmp_path: Path) -> None:
    input_root = tmp_path / "v1"
    output_root = tmp_path / "v2"
    report_json = tmp_path / "reports" / "plan.json"
    report_md = tmp_path / "reports" / "plan.md"
    _write_parquet(input_root / "c170_xml.parquet")

    exit_code = batch_cli.main(
        [
            "--input-root",
            str(input_root),
            "--output-root",
            str(output_root),
            "--dry-run",
            "--report-json",
            str(report_json),
            "--report-md",
            str(report_md),
        ]
    )

    assert exit_code == 0
    assert not output_root.exists()
    summary = json.loads(report_json.read_text(encoding="utf-8"))
    assert summary["dry_run"] is True
    assert summary["planned_count"] == 1
    assert summary["ok_count"] is None
    assert "Batch rewrite Parquets v2" in report_md.read_text(encoding="utf-8")


def test_main_executa_batch_com_funcao_injetada(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    input_root = tmp_path / "v1"
    output_root = tmp_path / "v2"
    report_json = tmp_path / "plan.json"
    parquet_path = input_root / "c170_xml.parquet"
    _write_parquet(parquet_path)

    def fake_batch(
        input_arg: str,
        output_arg: str,
        **kwargs: object,
    ) -> list[dict[str, object]]:
        assert Path(input_arg) == input_root.resolve()
        assert Path(output_arg) == output_root.resolve()
        assert kwargs["strict_cast"] is True
        # file_list deve conter o mesmo path do plano
        fl = kwargs.get("file_list")
        assert fl is not None
        assert len(fl) == 1
        assert Path(fl[0]) == parquet_path.resolve()
        return [
            {"input_path": str(parquet_path), "output_path": str(output_root / "c170_xml.parquet")}
        ]

    monkeypatch.setattr(batch_cli, "batch_rewrite_parquets", fake_batch)

    exit_code = batch_cli.main(
        [
            "--input-root",
            str(input_root),
            "--output-root",
            str(output_root),
            "--report-json",
            str(report_json),
        ]
    )

    assert exit_code == 0
    summary = json.loads(report_json.read_text(encoding="utf-8"))
    assert summary["planned_count"] == 1
    assert summary["ok_count"] == 1
    assert summary["failed_or_skipped_count"] == 0


# ---------------------------------------------------------------------------
# Testes #288 — flags operacionais
# ---------------------------------------------------------------------------


def test_main_min_size_mb_exclui_arquivos_pequenos(tmp_path: Path) -> None:
    """--min-size-mb exclui arquivos menores que o limite."""
    input_root = tmp_path / "v1"
    output_root = tmp_path / "v2"
    report_json = tmp_path / "plan.json"

    # Arquivo pequeno (~200 bytes)
    _write_parquet(input_root / "pequeno.parquet")
    # Arquivo maior (~7 KB com 5000 strings unicas)
    rng = pl.Series("_rng", range(5000)).cast(pl.String)
    pl.DataFrame(
        {
            "cfop": rng,
            "id_agrupado": rng,
        }
    ).write_parquet(input_root / "grande.parquet")

    exit_code = batch_cli.main(
        [
            "--input-root",
            str(input_root),
            "--output-root",
            str(output_root),
            "--dry-run",
            "--min-size-mb",
            "0.001",
            "--report-json",
            str(report_json),
        ]
    )

    assert exit_code == 0
    summary = json.loads(report_json.read_text(encoding="utf-8"))
    assert summary["planned_count"] == 1
    # Apenas o arquivo grande deve estar no plano
    assert "grande.parquet" in summary["files"][0]["input_path"]


def test_main_max_files_limita_quantidade_planejada(tmp_path: Path) -> None:
    """--max-files limita deterministicamente a quantidade planejada."""
    input_root = tmp_path / "v1"
    output_root = tmp_path / "v2"
    report_json = tmp_path / "plan.json"

    for i in range(5):
        _write_parquet(input_root / f"arquivo_{i}.parquet")

    exit_code = batch_cli.main(
        [
            "--input-root",
            str(input_root),
            "--output-root",
            str(output_root),
            "--dry-run",
            "--max-files",
            "3",
            "--report-json",
            str(report_json),
        ]
    )

    assert exit_code == 0
    summary = json.loads(report_json.read_text(encoding="utf-8"))
    assert summary["planned_count"] == 3


def test_main_no_strict_cast_nao_altera_descoberta(tmp_path: Path) -> None:
    """--no-strict-cast altera apenas a politica de cast, nao a descoberta."""
    input_root = tmp_path / "v1"
    output_root = tmp_path / "v2"
    report_json = tmp_path / "plan.json"

    _write_parquet(input_root / "c170_xml.parquet")

    exit_code = batch_cli.main(
        [
            "--input-root",
            str(input_root),
            "--output-root",
            str(output_root),
            "--dry-run",
            "--no-strict-cast",
            "--report-json",
            str(report_json),
        ]
    )

    assert exit_code == 0
    summary = json.loads(report_json.read_text(encoding="utf-8"))
    assert summary["strict_cast"] is False
    assert summary["planned_count"] == 1


def test_main_combinacao_flags_nao_promove_v2(tmp_path: Path) -> None:
    """Combinacao de flags nao promove v2 como padrao (dry-run)."""
    input_root = tmp_path / "v1"
    output_root = tmp_path / "v2"
    report_json = tmp_path / "plan.json"

    for i in range(10):
        _write_parquet(input_root / f"arquivo_{i}.parquet")

    exit_code = batch_cli.main(
        [
            "--input-root",
            str(input_root),
            "--output-root",
            str(output_root),
            "--dry-run",
            "--min-size-mb",
            "0.0001",
            "--max-files",
            "4",
            "--no-strict-cast",
            "--report-json",
            str(report_json),
        ]
    )

    assert exit_code == 0
    assert not output_root.exists()
    summary = json.loads(report_json.read_text(encoding="utf-8"))
    assert summary["dry_run"] is True
    assert summary["strict_cast"] is False
    assert summary["planned_count"] == 4
    assert summary["ok_count"] is None
