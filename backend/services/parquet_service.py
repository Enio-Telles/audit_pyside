"""
Serviço de manipulação de arquivos Parquet para o backend FastAPI.
Versão desacoplada de interface_grafica: sem cache, sem PySide6, sem perf_monitor.
"""
from __future__ import annotations

from pathlib import Path

import polars as pl

from utilitarios.project_paths import CNPJ_ROOT

_ANALISES_PREFIXOS_PERMITIDOS = (
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


class ParquetService:
    def __init__(self, root: Path = CNPJ_ROOT) -> None:
        self.root = root
        self.root.mkdir(parents=True, exist_ok=True)

    def list_cnpjs(self) -> list[str]:
        if not self.root.exists():
            return []
        return sorted(
            p.name
            for p in self.root.iterdir()
            if p.is_dir() and p.name.isdigit() and len(p.name) >= 11
        )

    def cnpj_dir(self, cnpj: str) -> Path:
        return self.root / cnpj

    def list_parquet_files(self, cnpj: str) -> list[Path]:
        base = self.cnpj_dir(cnpj)
        if not base.exists():
            return []

        brutos = base / "arquivos_parquet"
        analises = base / "analises" / "produtos"
        old_prod = base / "produtos"

        files: list[Path] = []
        if brutos.exists():
            files.extend(brutos.glob("*.parquet"))
        if analises.exists():
            files.extend(analises.glob("*.parquet"))
        if old_prod.exists():
            files.extend(old_prod.glob("*.parquet"))
        files.extend(base.glob("*.parquet"))

        filtrados: list[Path] = []
        for path in set(files):
            parent_str = str(path.parent)
            if "arquivos_parquet" in parent_str:
                if any(
                    tag in path.name
                    for tag in ("_produtos_", "_enriquecido_", "_sem_id_agrupado_")
                ):
                    continue
                filtrados.append(path)
                continue
            if "analises" in parent_str or "produtos" in parent_str:
                if path.name.startswith(_ANALISES_PREFIXOS_PERMITIDOS):
                    filtrados.append(path)
                continue
            filtrados.append(path)

        return sorted(filtrados, key=lambda p: (str(p.parent), p.name))

    def get_schema(self, parquet_path: Path) -> list[str]:
        return list(pl.read_parquet_schema(parquet_path).names())

    def save_dataset(self, parquet_path: Path, df: pl.DataFrame) -> None:
        parquet_path.parent.mkdir(parents=True, exist_ok=True)
        df.write_parquet(parquet_path)
