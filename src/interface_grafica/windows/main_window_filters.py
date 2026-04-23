from __future__ import annotations

import polars as pl
from PySide6.QtCore import QDate
from PySide6.QtWidgets import QComboBox


class MainWindowFiltersMixin:
    def _popular_combo_texto(
        self,
        combo: QComboBox,
        valores: list[str],
        valor_atual: str = "",
        primeiro_item: str = "",
    ) -> None:
        combo.blockSignals(True)
        combo.clear()
        if primeiro_item is not None:
            combo.addItem(primeiro_item)
        combo.addItems([str(v) for v in valores])
        if valor_atual:
            combo.setCurrentText(valor_atual)
        combo.blockSignals(False)
    def _filtrar_texto_em_colunas(self, df: pl.DataFrame, texto: str) -> pl.DataFrame:
        texto = (texto or "").strip().lower()
        if not texto or df.is_empty():
            return df

        colunas_busca = [
            c for c in df.columns if df.schema[c] in [pl.Utf8, pl.Categorical]
        ]
        if not colunas_busca:
            return df

        expr = None
        for col in colunas_busca:
            atual = (
                pl.col(col)
                .cast(pl.Utf8, strict=False)
                .fill_null("")
                .str.to_lowercase()
                .str.contains(texto, literal=True)
            )
            expr = atual if expr is None else (expr | atual)
        return df.filter(expr) if expr is not None else df
    def _valor_qdate_ativo(self, value: QDate) -> QDate | None:
        return None if not value.isValid() or value == QDate(1900, 1, 1) else value
    def _parse_numero_filtro(self, valor: str) -> float | None:
        bruto = (valor or "").strip()
        if not bruto:
            return None
        try:
            return float(bruto.replace(",", "."))
        except Exception:
            return None
    def _filtrar_intervalo_numerico(
        self,
        df: pl.DataFrame,
        coluna: str | None,
        valor_min: str,
        valor_max: str,
    ) -> pl.DataFrame:
        if not coluna or coluna not in df.columns:
            return df

        minimo = self._parse_numero_filtro(valor_min)
        maximo = self._parse_numero_filtro(valor_max)
        if minimo is None and maximo is None:
            return df

        expr_col = pl.col(coluna).cast(pl.Float64, strict=False)
        if minimo is not None:
            df = df.filter(expr_col >= minimo)
        if maximo is not None:
            df = df.filter(expr_col <= maximo)
        return df
    def _filtrar_intervalo_data(
        self,
        df: pl.DataFrame,
        coluna: str | None,
        data_ini: QDate | None,
        data_fim: QDate | None,
    ) -> pl.DataFrame:
        if (
            not coluna
            or coluna not in df.columns
            or (data_ini is None and data_fim is None)
        ):
            return df

        col_data = (
            pl.col(coluna)
            .cast(pl.Utf8, strict=False)
            .fill_null("")
            .str.replace_all(r"[^0-9]", "")
            .str.slice(0, 8)
            .str.strptime(pl.Date, format="%Y%m%d", strict=False)
        )
        if data_ini is not None:
            df = df.filter(col_data >= pl.lit(data_ini.toPython()))
        if data_fim is not None:
            df = df.filter(col_data <= pl.lit(data_fim.toPython()))
        return df
