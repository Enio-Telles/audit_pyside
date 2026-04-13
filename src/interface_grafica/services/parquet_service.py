from __future__ import annotations

from collections import OrderedDict
from dataclasses import dataclass
from pathlib import Path
from time import perf_counter
from typing import Iterable

import polars as pl

from interface_grafica.config import CNPJ_ROOT as CONSULTAS_ROOT, DEFAULT_PAGE_SIZE
from utilitarios.perf_monitor import registrar_evento_performance


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
        self._schema_cache: dict[tuple[str, int, int], list[str]] = {}
        self._count_cache: dict[tuple[str, int, int, tuple[tuple[str, str, str], ...]], int] = {}
        self._page_cache: OrderedDict[
            tuple[str, int, int, tuple[tuple[str, str, str], ...], tuple[str, ...], int, int],
            PageResult,
        ] = OrderedDict()
        self._page_cache_limit = 10
        self._dataset_cache: OrderedDict[
            tuple[str, int, int, tuple[tuple[str, str, str], ...], tuple[str, ...]],
            pl.DataFrame,
        ] = OrderedDict()
        self._dataset_cache_limit = 6

    @staticmethod
    def _path_signature(parquet_path: Path) -> tuple[str, int, int]:
        stat = parquet_path.stat()
        return (str(parquet_path.resolve()), stat.st_mtime_ns, stat.st_size)

    @staticmethod
    def _conditions_key(conditions: Iterable[FilterCondition] | None) -> tuple[tuple[str, str, str], ...]:
        if not conditions:
            return ()
        return tuple((c.column or "", c.operator or "", c.value or "") for c in conditions)

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
        inicio = perf_counter()
        key = self._path_signature(parquet_path)
        cached = self._schema_cache.get(key)
        if cached is not None:
            registrar_evento_performance(
                "parquet_service.get_schema",
                perf_counter() - inicio,
                {
                    "parquet_path": parquet_path,
                    "cache_hit": True,
                    "colunas": len(cached),
                },
            )
            return cached[:]
        schema = list(pl.read_parquet_schema(parquet_path).names())
        self._schema_cache[key] = schema[:]
        registrar_evento_performance(
            "parquet_service.get_schema",
            perf_counter() - inicio,
            {
                "parquet_path": parquet_path,
                "cache_hit": False,
                "colunas": len(schema),
            },
        )
        return schema

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

    @staticmethod
    def _is_list_dtype(dtype) -> bool:
        if dtype is None:
            return False
        try:
            return dtype.base_type() == pl.List
        except Exception:
            return str(dtype).startswith("List")

    def _text_expr(self, column: str, dtype) -> pl.Expr:
        col = pl.col(column)
        if self._is_list_dtype(dtype):
            return (
                col.cast(pl.List(pl.Utf8), strict=False)
                .list.join(" | ")
                .fill_null("")
            )
        return col.cast(pl.Utf8, strict=False).fill_null("")

    def _build_expr(self, cond: FilterCondition, dtype=None) -> pl.Expr:
        col = pl.col(cond.column)
        value = cond.value or ""
        op = self._normalize_operator(cond.operator)
        text_col = self._text_expr(cond.column, dtype)

        if op == "contem":
            return text_col.str.to_lowercase().str.contains(value.lower(), literal=True)
        if op == "igual":
            return text_col == value
        if op == "comeca_com":
            return text_col.str.to_lowercase().str.starts_with(value.lower())
        if op == "termina_com":
            return text_col.str.to_lowercase().str.ends_with(value.lower())
        if op == "e_nulo":
            return col.is_null() | (text_col == "")
        if op == "nao_e_nulo":
            return ~(col.is_null() | (text_col == ""))

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

        return text_col == value

    def apply_filters(
        self,
        lf: pl.LazyFrame,
        conditions: Iterable[FilterCondition],
        available_columns: dict[str, object] | None = None,
    ) -> pl.LazyFrame:
        filtered = lf
        if available_columns is None:
            try:
                schema = filtered.collect_schema()
                available_columns = {name: schema[name] for name in schema.names()}
            except Exception:
                available_columns = {}

        for cond in conditions:
            if not cond.column:
                continue
            if available_columns and cond.column not in available_columns:
                # Evita erro quando filtros antigos apontam para colunas que nao existem no parquet atual.
                continue
            op_norm = self._normalize_operator(cond.operator)
            if op_norm not in {"e_nulo", "nao_e_nulo"} and cond.value == "":
                continue
            filtered = filtered.filter(self._build_expr(cond, available_columns.get(cond.column)))
        return filtered

    def build_lazyframe(self, parquet_path: Path, conditions: Iterable[FilterCondition] | None = None) -> pl.LazyFrame:
        lf = pl.scan_parquet(parquet_path)
        if conditions:
            schema = pl.read_parquet_schema(parquet_path)
            lf = self.apply_filters(lf, conditions, available_columns={name: schema[name] for name in schema.names()})
        return lf

    def get_page(
        self,
        parquet_path: Path,
        conditions: list[FilterCondition],
        visible_columns: list[str] | None,
        page: int,
        page_size: int = DEFAULT_PAGE_SIZE,
        sort_by: str | None = None,
        sort_desc: bool = False,
    ) -> PageResult:
        inicio_total = perf_counter()
        page = max(page, 1)
        path_signature = self._path_signature(parquet_path)
        page_cache_key = (
            *path_signature,
            self._conditions_key(conditions),
            tuple(visible_columns or ()),
            sort_by or "",
            sort_desc,
            page,
            page_size,
        )
        cached_page = self._page_cache.get(page_cache_key)
        if cached_page is not None:
            self._page_cache.move_to_end(page_cache_key)
            registrar_evento_performance(
                "parquet_service.get_page.total",
                perf_counter() - inicio_total,
                {
                    "parquet_path": parquet_path,
                    "page": page,
                    "page_size": page_size,
                    "total_rows": cached_page.total_rows,
                    "linhas_pagina": cached_page.df_all_columns.height,
                    "colunas_visiveis": len(cached_page.df_visible.columns),
                    "cache_hit": True,
                },
            )
            return cached_page
        inicio_lf = perf_counter()
        lf_all = self.build_lazyframe(parquet_path, conditions)
        registrar_evento_performance(
            "parquet_service.get_page.build_lazyframe",
            perf_counter() - inicio_lf,
            {
                "parquet_path": parquet_path,
                "page": page,
                "page_size": page_size,
                "quantidade_filtros": len(conditions or []),
            },
        )
        count_key = (*path_signature, self._conditions_key(conditions))
        total_rows = self._count_cache.get(count_key)
        count_cache_hit = total_rows is not None
        if total_rows is None:
            inicio_count = perf_counter()
            total_rows = int(lf_all.select(pl.len().alias("n")).collect().item())
            self._count_cache[count_key] = total_rows
            registrar_evento_performance(
                "parquet_service.get_page.count_rows",
                perf_counter() - inicio_count,
                {
                    "parquet_path": parquet_path,
                    "cache_hit": False,
                    "total_rows": total_rows,
                    "quantidade_filtros": len(conditions or []),
                },
            )
        else:
            registrar_evento_performance(
                "parquet_service.get_page.count_rows",
                0.0,
                {
                    "parquet_path": parquet_path,
                    "cache_hit": True,
                    "total_rows": total_rows,
                    "quantidade_filtros": len(conditions or []),
                },
            )
        all_columns = self.get_schema(parquet_path)
        if not visible_columns:
            visible_columns = all_columns[:]
        offset = (page - 1) * page_size
        if sort_by and sort_by in all_columns:
            lf_all = lf_all.sort(sort_by, descending=sort_desc)
        inicio_collect = perf_counter()
        df_all = lf_all.slice(offset, page_size).collect()
        registrar_evento_performance(
            "parquet_service.get_page.collect_page",
            perf_counter() - inicio_collect,
            {
                "parquet_path": parquet_path,
                "page": page,
                "page_size": page_size,
                "linhas_pagina": df_all.height,
                "cache_hit_count": count_cache_hit,
            },
        )
        df_visible = df_all.select([c for c in visible_columns if c in df_all.columns])
        registrar_evento_performance(
            "parquet_service.get_page.total",
            perf_counter() - inicio_total,
            {
                "parquet_path": parquet_path,
                "page": page,
                "page_size": page_size,
                "total_rows": total_rows,
                "linhas_pagina": df_all.height,
                "colunas_visiveis": len(df_visible.columns),
                "cache_hit": False,
            },
        )
        result = PageResult(
            total_rows=total_rows,
            df_all_columns=df_all,
            df_visible=df_visible,
            columns=all_columns,
            visible_columns=visible_columns,
        )
        self._page_cache[page_cache_key] = result
        self._page_cache.move_to_end(page_cache_key)
        while len(self._page_cache) > self._page_cache_limit:
            self._page_cache.popitem(last=False)
        return result

    def load_dataset(self, parquet_path: Path, conditions: list[FilterCondition] | None = None, columns: list[str] | None = None) -> pl.DataFrame:
        inicio = perf_counter()
        cache_key = (
            *self._path_signature(parquet_path),
            self._conditions_key(conditions),
            tuple(columns or ()),
        )
        cached = self._dataset_cache.get(cache_key)
        if cached is not None:
            self._dataset_cache.move_to_end(cache_key)
            registrar_evento_performance(
                "parquet_service.load_dataset",
                perf_counter() - inicio,
                {
                    "parquet_path": parquet_path,
                    "quantidade_filtros": len(conditions or []),
                    "colunas_solicitadas": len(columns or []),
                    "linhas": cached.height,
                    "colunas": cached.width,
                    "cache_hit": True,
                },
            )
            return cached
        lf = self.build_lazyframe(parquet_path, conditions or [])
        if columns:
            lf = lf.select(columns)
        df = lf.collect()
        self._dataset_cache[cache_key] = df
        self._dataset_cache.move_to_end(cache_key)
        while len(self._dataset_cache) > self._dataset_cache_limit:
            self._dataset_cache.popitem(last=False)
        registrar_evento_performance(
            "parquet_service.load_dataset",
            perf_counter() - inicio,
            {
                "parquet_path": parquet_path,
                "quantidade_filtros": len(conditions or []),
                "colunas_solicitadas": len(columns or []),
                "linhas": df.height,
                "colunas": df.width,
                "cache_hit": False,
            },
        )
        return df

    def save_dataset(self, parquet_path: Path, df: pl.DataFrame) -> None:
        parquet_path.parent.mkdir(parents=True, exist_ok=True)
        df.write_parquet(parquet_path, compression="snappy")
        target = str(parquet_path.resolve())
        self._schema_cache = {k: v for k, v in self._schema_cache.items() if k[0] != target}
        self._count_cache = {k: v for k, v in self._count_cache.items() if k[0] != target}
        self._page_cache = OrderedDict((k, v) for k, v in self._page_cache.items() if k[0] != target)
        self._dataset_cache = OrderedDict((k, v) for k, v in self._dataset_cache.items() if k[0] != target)

    paginate = get_page

