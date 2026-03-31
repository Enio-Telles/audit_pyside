from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
import sys

import polars as pl

sys.path.insert(0, str(Path("src").resolve()))

from interface_grafica.services.export_service import ExportService
from utilitarios.text import display_cell


def test_display_cell_formata_data_date():
    assert display_cell(date(2026, 3, 27), "data_emissao") == "27/03/2026"


def test_display_cell_formata_data_iso_string():
    assert display_cell("2026-03-27", "dt_emissao") == "27/03/2026"


def test_display_cell_formata_datetime_iso_string():
    assert display_cell("2026-03-27 14:35:10", "dh_evento") == "27/03/2026 14:35:10"


def test_display_cell_preserva_ano_sem_mascara_decimal():
    assert display_cell(2026, "ano") == "2026"
    assert display_cell(Decimal("2026"), "ano_efd") == "2026"


def test_display_cell_formata_numeros_em_padrao_brasileiro():
    assert display_cell(1234567, "quantidade") == "1.234.567"
    assert display_cell(1234567.8, "valor_total") == "1.234.567,80"


def test_display_cell_formata_listas_recursivamente():
    valores = [datetime(2026, 3, 27, 10, 5, 0), 1500.25]
    assert display_cell(valores, "valor_total") == "27/03/2026 10:05:00, 1.500,25"


def test_export_service_html_aplica_formatacao_por_coluna():
    df = pl.DataFrame(
        {
            "ano": [2026],
            "data_emissao": [date(2026, 3, 27)],
            "valor_total": [1234.5],
        }
    )

    html = ExportService().build_html_report(
        title="Teste",
        cnpj="12345678000190",
        table_name="mov_estoque",
        df=df,
        filters_text="",
        visible_columns=df.columns,
    )

    assert "27/03/2026" in html
    assert ">2026<" in html
    assert "1.234,50" in html
