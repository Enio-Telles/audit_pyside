from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import polars as pl

from interface_grafica.config import CNPJ_ROOT as CONSULTAS_ROOT, DEFAULT_PAGE_SIZE


@dataclass
class FilterCondition:
    column: str
    operator: str
    value: str = ""


@dataclass
class PageResult:
    total_rows: int
    df_all_columns: pl.DataFrame
    df_visible: pl.DataFrame
    columns: list[str]
    visible_columns: list[str]


class ParquetService:
    ANALISES_PREFIXOS_PERMITIDOS = (
        "tb_documentos_",
        "item_unidades_",
        "itens_",
        "descricao_produtos_",
        "produtos_agrupados_",
        "map_produto_agrupado_",
        "produtos_final_",
        "c170_agr_",
        "bloco_h_agr_",
        "nfe_agr_",
        "nfce_agr_",
        "fatores_conversao_",
        "log_sem_preco_medio_compra_",
        "mov_estoque_",
        "c176_xml_",
    )

    def __init__(self, root: Path = CONSULTAS_ROOT) -> None:
        self.root = root
        self.root.mkdir(parents=True, exist_ok=True)

    def list_cnpjs(self) -> list[str]:
        if not self.root.exists():
            return []
        rows = [p.name for p in self.root.iterdir() if p.is_dir() and (p.name.isdigit() and len(p.name) >= 11)]
        return sorted(rows)

    def cnpj_dir(self, cnpj: str) -> Path:
        return self.root / cnpj

    def list_parquet_files(self, cnpj: str) -> list[Path]:
        base = self.cnpj_dir(cnpj)
        if not base.exists():
            return []
        
        # New structure
        brutos = base / "arquivos_parquet"
        analises = base / "analises" / "produtos"
        # Old structure fallback
        old_prod = base / "produtos"
        
        files = []
        if brutos.exists():
            files.extend(brutos.glob("*.parquet"))
        if analises.exists():
            files.extend(analises.glob("*.parquet"))
        if old_prod.exists():
            files.extend(old_prod.glob("*.parquet"))
        
        # Also check root of CNPJ folder for any loose parquets
        files.extend(base.glob("*.parquet"))
        
        filtrados: list[Path] = []
        for path in set(files):
            parent_str = str(path.parent)
            if "arquivos_parquet" in parent_str:
                if any(tag in path.name for tag in ("_produtos_", "_enriquecido_", "_sem_id_agrupado_")):
                    continue
                filtrados.append(path)
                continue
            if "analises" in parent_str or "produtos" in parent_str:
                if path.name.startswith(self.ANALISES_PREFIXOS_PERMITIDOS):
                    filtrados.append(path)
                continue
            filtrados.append(path)
        
        return sorted(filtrados, key=lambda p: (str(p.parent), p.name))

    def get_schema(self, parquet_path: Path) -> list[str]:
        return list(pl.scan_parquet(parquet_path).collect_schema().names())

    @staticmethod
    def _normalize_operator(op: str) -> str:
        # Aceita variantes com encoding corrompido e sem acentos.
        op_l = (op or "").strip().lower()
        # Heuristicas tolerantes a texto corrompido (ex.: "cont?m").
        if op_l.startswith("cont"):
            return "contem"
        if op_l.startswith("come"):
            return "comeca_com"
        if op_l.startswith("termina"):
            return "termina_com"
        if "nulo" in op_l:
            if "não" in op_l or "nao" in op_l or "nÃ" in op_l:
                return "nao_e_nulo"
            return "e_nulo"
        aliases = {
            "contem": {"contém", "contÃ©m", "contem"},
            "igual": {"igual"},
            "comeca_com": {"começa com", "comeÃ§a com", "comeca com"},
            "termina_com": {"termina com"},
            "maior": {">"},
            "maior_igual": {">="},
            "menor": {"<"},
            "menor_igual": {"<="},
            "e_nulo": {"é nulo", "Ã© nulo", "e nulo"},
            "nao_e_nulo": {"não é nulo", "nÃ£o Ã© nulo", "nao e nulo"},
        }
        for canonical, opts in aliases.items():
            if op_l in opts:
                return canonical
        return op_l

    def _build_expr(self, cond: FilterCondition) -> pl.Expr:
        col = pl.col(cond.column)
        value = cond.value or ""
        op = self._normalize_operator(cond.operator)

        if op == "contem":
            return col.cast(pl.Utf8, strict=False).fill_null("").str.to_lowercase().str.contains(value.lower(), literal=True)
        if op == "igual":
            return col.cast(pl.Utf8, strict=False).fill_null("") == value
        if op == "comeca_com":
            return col.cast(pl.Utf8, strict=False).fill_null("").str.to_lowercase().str.starts_with(value.lower())
        if op == "termina_com":
            return col.cast(pl.Utf8, strict=False).fill_null("").str.to_lowercase().str.ends_with(value.lower())
        if op == "e_nulo":
            return col.is_null() | (col.cast(pl.Utf8, strict=False).fill_null("") == "")
        if op == "nao_e_nulo":
            return ~(col.is_null() | (col.cast(pl.Utf8, strict=False).fill_null("") == ""))

        numeric_col = col.cast(pl.Float64, strict=False)
        try:
            numeric_value = float(value.replace(",", "."))
        except Exception:
            numeric_value = None

        if op in {"maior", "maior_igual", "menor", "menor_igual"} and numeric_value is not None:
            mapping = {
                "maior": numeric_col > numeric_value,
                "maior_igual": numeric_col >= numeric_value,
                "menor": numeric_col < numeric_value,
                "menor_igual": numeric_col <= numeric_value,
            }
            return mapping[op]

        return col.cast(pl.Utf8, strict=False).fill_null("") == value

    def apply_filters(self, lf: pl.LazyFrame, conditions: Iterable[FilterCondition]) -> pl.LazyFrame:
        filtered = lf
        try:
            available_columns = set(filtered.collect_schema().names())
        except Exception:
            available_columns = set()

        for cond in conditions:
            if not cond.column:
                continue
            if available_columns and cond.column not in available_columns:
                # Evita erro quando filtros antigos apontam para colunas que nao existem no parquet atual.
                continue
            op_norm = self._normalize_operator(cond.operator)
            if op_norm not in {"e_nulo", "nao_e_nulo"} and cond.value == "":
                continue
            filtered = filtered.filter(self._build_expr(cond))
        return filtered

    def build_lazyframe(self, parquet_path: Path, conditions: Iterable[FilterCondition] | None = None) -> pl.LazyFrame:
        lf = pl.scan_parquet(parquet_path)
        if conditions:
            lf = self.apply_filters(lf, conditions)
        return lf

    def get_page(
        self,
        parquet_path: Path,
        conditions: list[FilterCondition],
        visible_columns: list[str] | None,
        page: int,
        page_size: int = DEFAULT_PAGE_SIZE,
    ) -> PageResult:
        page = max(page, 1)
        lf_all = self.build_lazyframe(parquet_path, conditions)
        total_rows = int(lf_all.select(pl.len().alias("n")).collect().item())
        all_columns = self.get_schema(parquet_path)
        if not visible_columns:
            visible_columns = all_columns[:]
        offset = (page - 1) * page_size
        df_all = lf_all.slice(offset, page_size).collect()
        df_visible = df_all.select([c for c in visible_columns if c in df_all.columns])
        return PageResult(
            total_rows=total_rows,
            df_all_columns=df_all,
            df_visible=df_visible,
            columns=all_columns,
            visible_columns=visible_columns,
        )

    def load_dataset(self, parquet_path: Path, conditions: list[FilterCondition] | None = None, columns: list[str] | None = None) -> pl.DataFrame:
        lf = self.build_lazyframe(parquet_path, conditions or [])
        if columns:
            lf = lf.select(columns)
        return lf.collect()

    def save_dataset(self, parquet_path: Path, df: pl.DataFrame) -> None:
        parquet_path.parent.mkdir(parents=True, exist_ok=True)
        df.write_parquet(parquet_path, compression="snappy")

