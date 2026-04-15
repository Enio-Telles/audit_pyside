from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path

import polars as pl
from fastapi import APIRouter, HTTPException
from filelock import FileLock, Timeout
from pydantic import BaseModel

from interface_grafica.config import CNPJ_ROOT
from routers._common import sanitize_cnpj, df_to_response, resposta_vazia

router = APIRouter()


def _pasta_produtos(cnpj: str) -> Path:
    return CNPJ_ROOT / cnpj / "analises" / "produtos"


def _path_bloco_h(cnpj: str) -> Path:
    base_cnpj = CNPJ_ROOT / cnpj
    candidatos = [
        _pasta_produtos(cnpj) / f"bloco_h_{cnpj}.parquet",
        base_cnpj / "arquivos_parquet" / f"bloco_h_{cnpj}.parquet",
        base_cnpj / "arquivos_parquet" / "fiscal" / "efd" / f"bloco_h_{cnpj}.parquet",
    ]

    for path in candidatos:
        if path.exists():
            return path

    # Fallback para variações de nome/local do artefato.
    # Evita pegar arquivos agregados quando existir o detalhado.
    parquet_root = base_cnpj / "arquivos_parquet"
    if parquet_root.exists():
        encontrados = sorted(parquet_root.rglob(f"bloco_h*{cnpj}*.parquet"))
        detalhados = [p for p in encontrados if "_agr_" not in p.name.lower()]
        if detalhados:
            return detalhados[0]
        if encontrados:
            return encontrados[0]

    return candidatos[0]


def _resposta_paginada_vazia(page: int = 1, page_size: int = 500) -> dict:
    """
    Retorna uma estrutura vazia compatível com o contrato esperado pelo frontend.

    Isso evita erro de leitura quando o parquet analítico ainda não foi gerado.
    """
    return resposta_vazia(page, page_size)


def _ler_tabela_estoque_ou_vazia(path: Path, page: int = 1, page_size: int = 500) -> dict:
    """
    Lê o parquet solicitado ou devolve uma resposta vazia quando o artefato não existe.
    """
    if not path.exists():
        return _resposta_paginada_vazia(page, page_size)
    df = pl.read_parquet(path)
    return df_to_response(df, page, page_size)


@contextmanager
def _fatores_conversao_lock(path: Path):
    lock_path = path.with_suffix(".lock")
    try:
        with FileLock(str(lock_path), timeout=10):
            yield
    except Timeout as exc:
        raise HTTPException(423, "Arquivo de fatores de conversão está em uso") from exc


def _aplicar_filtros_bloco_h(
    df: pl.DataFrame,
    dt_inv: str | None = None,
    cod_mot_inv: str | None = None,
    indicador_propriedade: str | None = None,
) -> pl.DataFrame:
    if dt_inv and "dt_inv" in df.columns:
        dt_inv_norm = dt_inv.strip()
        if dt_inv_norm:
            df = df.filter(pl.col("dt_inv").cast(pl.Utf8).str.contains(dt_inv_norm, literal=True))

    if cod_mot_inv and "cod_mot_inv" in df.columns:
        cod_mot_inv_norm = cod_mot_inv.strip()
        if cod_mot_inv_norm:
            df = df.filter(pl.col("cod_mot_inv").cast(pl.Utf8) == cod_mot_inv_norm)

    if indicador_propriedade and "indicador_propriedade" in df.columns:
        indicador_norm = indicador_propriedade.strip()
        if indicador_norm:
            df = df.filter(pl.col("indicador_propriedade").cast(pl.Utf8) == indicador_norm)

    return df


@router.get("/{cnpj}/mov_estoque")
def get_mov_estoque(cnpj: str, page: int = 1, page_size: int = 500):
    cnpj = sanitize_cnpj(cnpj)
    path = _pasta_produtos(cnpj) / f"mov_estoque_{cnpj}.parquet"
    return _ler_tabela_estoque_ou_vazia(path, page, page_size)


@router.get("/{cnpj}/tabela_mensal")
def get_tabela_mensal(cnpj: str, page: int = 1, page_size: int = 500):
    cnpj = sanitize_cnpj(cnpj)
    path = _pasta_produtos(cnpj) / f"aba_mensal_{cnpj}.parquet"
    return _ler_tabela_estoque_ou_vazia(path, page, page_size)


@router.get("/{cnpj}/tabela_anual")
def get_tabela_anual(cnpj: str, page: int = 1, page_size: int = 500):
    cnpj = sanitize_cnpj(cnpj)
    path = _pasta_produtos(cnpj) / f"aba_anual_{cnpj}.parquet"
    return _ler_tabela_estoque_ou_vazia(path, page, page_size)


@router.get("/{cnpj}/id_agrupados")
def get_id_agrupados(cnpj: str, page: int = 1, page_size: int = 500):
    cnpj = sanitize_cnpj(cnpj)
    path = _pasta_produtos(cnpj) / f"produtos_final_{cnpj}.parquet"
    return _ler_tabela_estoque_ou_vazia(path, page, page_size)


@router.get("/{cnpj}/fatores_conversao")
def get_fatores_conversao(cnpj: str, page: int = 1, page_size: int = 500):
    cnpj = sanitize_cnpj(cnpj)
    path = _pasta_produtos(cnpj) / f"fatores_conversao_{cnpj}.parquet"
    return _ler_tabela_estoque_ou_vazia(path, page, page_size)


@router.get("/{cnpj}/bloco_h")
def get_bloco_h(
    cnpj: str,
    page: int = 1,
    page_size: int = 500,
    dt_inv: str | None = None,
    cod_mot_inv: str | None = None,
    indicador_propriedade: str | None = None,
):
    cnpj = sanitize_cnpj(cnpj)
    path = _path_bloco_h(cnpj)
    if not path.exists():
        return _resposta_paginada_vazia(page, page_size)

    df = pl.read_parquet(path)
    df = _aplicar_filtros_bloco_h(df, dt_inv, cod_mot_inv, indicador_propriedade)
    return df_to_response(df, page, page_size)


@router.get("/{cnpj}/bloco_h_h005")
def get_bloco_h_h005(
    cnpj: str,
    page: int = 1,
    page_size: int = 500,
    dt_inv: str | None = None,
    cod_mot_inv: str | None = None,
    indicador_propriedade: str | None = None,
):
    cnpj = sanitize_cnpj(cnpj)
    path = _path_bloco_h(cnpj)
    if not path.exists():
        return _resposta_paginada_vazia(page, page_size)

    df = pl.read_parquet(path)
    df = _aplicar_filtros_bloco_h(df, dt_inv, cod_mot_inv, indicador_propriedade)

    if "dt_inv" not in df.columns:
        return _resposta_paginada_vazia(page, page_size)

    if "mot_inv_desc" not in df.columns:
        df = df.with_columns(pl.lit(None).alias("mot_inv_desc"))
    if "valor_total_inventario_h005" not in df.columns:
        df = df.with_columns(pl.lit(0.0).alias("valor_total_inventario_h005"))

    agg_exprs: list[pl.Expr] = [
        pl.len().alias("qtd_linhas_h010"),
    ]
    if "codigo_produto" in df.columns:
        agg_exprs.append(pl.col("codigo_produto").n_unique().alias("qtd_produtos_codigo_produto"))
    else:
        agg_exprs.append(pl.lit(0).alias("qtd_produtos_codigo_produto"))

    if "quantidade" in df.columns:
        agg_exprs.append(pl.col("quantidade").cast(pl.Float64).fill_null(0).sum().alias("quantidade_total"))
    else:
        agg_exprs.append(pl.lit(0.0).alias("quantidade_total"))

    if "valor_item" in df.columns:
        agg_exprs.append(pl.col("valor_item").cast(pl.Float64).fill_null(0).sum().alias("valor_total_itens_h010"))
    else:
        agg_exprs.append(pl.lit(0.0).alias("valor_total_itens_h010"))

    resumo = (
        df.group_by(["dt_inv", "cod_mot_inv", "mot_inv_desc", "valor_total_inventario_h005"])
        .agg(agg_exprs)
        .sort("dt_inv", descending=True)
    )

    return df_to_response(resumo, page, page_size)


@router.get("/{cnpj}/bloco_h_resumo")
def get_bloco_h_resumo(
    cnpj: str,
    dt_inv: str | None = None,
    cod_mot_inv: str | None = None,
    indicador_propriedade: str | None = None,
):
    cnpj = sanitize_cnpj(cnpj)
    path = _path_bloco_h(cnpj)

    if not path.exists():
        return {
            "inventarios_h005": 0,
            "total_produtos_codigo_produto": 0,
            "total_linhas_h010": 0,
            "valor_total_itens": 0.0,
            "motivos": [],
            "propriedade": [],
        }

    df = pl.read_parquet(path)
    df = _aplicar_filtros_bloco_h(df, dt_inv, cod_mot_inv, indicador_propriedade)
    total_linhas = df.height

    inventarios_h005 = 0
    if "dt_inv" in df.columns:
        inventarios_h005 = df.select(pl.col("dt_inv").n_unique()).item()

    total_produtos = 0
    if "codigo_produto" in df.columns:
        total_produtos = (
            df.filter(
                pl.col("codigo_produto").is_not_null()
                & (pl.col("codigo_produto").cast(pl.Utf8).str.len_chars() > 0)
            )
            .select(pl.col("codigo_produto").n_unique())
            .item()
        )

    valor_total_itens = 0.0
    if "valor_item" in df.columns:
        valor_total_itens = float(
            df.select(pl.col("valor_item").cast(pl.Float64).fill_null(0).sum()).item()
            or 0.0
        )

    motivos = []
    if "cod_mot_inv" in df.columns:
        motivos_df = (
            df.group_by(["cod_mot_inv", "mot_inv_desc"] if "mot_inv_desc" in df.columns else ["cod_mot_inv"])
            .agg(pl.len().alias("qtd_itens"))
            .sort("qtd_itens", descending=True)
        )
        if "mot_inv_desc" not in motivos_df.columns:
            motivos_df = motivos_df.with_columns(pl.lit(None).alias("mot_inv_desc"))
        motivos = motivos_df.to_dicts()

    propriedade = []
    if "indicador_propriedade" in df.columns:
        propriedade = (
            df.group_by("indicador_propriedade")
            .agg(pl.len().alias("qtd_itens"))
            .sort("qtd_itens", descending=True)
            .to_dicts()
        )

    return {
        "inventarios_h005": int(inventarios_h005 or 0),
        "total_produtos_codigo_produto": int(total_produtos or 0),
        "total_linhas_h010": int(total_linhas or 0),
        "valor_total_itens": valor_total_itens,
        "motivos": motivos,
        "propriedade": propriedade,
    }


class FatorUpdate(BaseModel):
    id_agrupado: str
    id_produtos: str
    fator: float | None = None
    unid_ref: str | None = None


class UnidRefBatchUpdate(BaseModel):
    id_agrupado: str
    unid_ref: str


@router.patch("/{cnpj}/fatores_conversao/batch_unid_ref")
def patch_fatores_unid_ref_batch(cnpj: str, req: UnidRefBatchUpdate):
    cnpj = sanitize_cnpj(cnpj)
    path = _pasta_produtos(cnpj) / f"fatores_conversao_{cnpj}.parquet"

    with _fatores_conversao_lock(path):
        if not path.exists():
            raise HTTPException(404, "fatores_conversao não encontrado")

        df = pl.read_parquet(path)
        if "unid_ref_manual" not in df.columns:
            df = df.with_columns(pl.lit(False).alias("unid_ref_manual"))
        mask = pl.col("id_agrupado") == req.id_agrupado
        df = df.with_columns([
            pl.when(mask).then(pl.lit(req.unid_ref)).otherwise(pl.col("unid_ref")).alias("unid_ref"),
            pl.when(mask).then(pl.lit(True)).otherwise(pl.col("unid_ref_manual")).alias("unid_ref_manual"),
        ])
        df.write_parquet(path)
    return {"ok": True}


@router.patch("/{cnpj}/fatores_conversao")
def patch_fatores_conversao(cnpj: str, req: FatorUpdate):
    cnpj = sanitize_cnpj(cnpj)
    path = _pasta_produtos(cnpj) / f"fatores_conversao_{cnpj}.parquet"

    with _fatores_conversao_lock(path):
        if not path.exists():
            raise HTTPException(404, "fatores_conversao não encontrado")

        df = pl.read_parquet(path)
        if "fator_manual" not in df.columns:
            df = df.with_columns(pl.lit(False).alias("fator_manual"))
        if "unid_ref_manual" not in df.columns:
            df = df.with_columns(pl.lit(False).alias("unid_ref_manual"))
        mask = (pl.col("id_agrupado") == req.id_agrupado) & (pl.col("id_produtos") == req.id_produtos)
        if req.fator is not None:
            df = df.with_columns([
                pl.when(mask).then(pl.lit(req.fator)).otherwise(pl.col("fator")).alias("fator"),
                pl.when(mask).then(pl.lit(True)).otherwise(pl.col("fator_manual")).alias("fator_manual"),
            ])
        if req.unid_ref is not None:
            df = df.with_columns([
                pl.when(mask).then(pl.lit(req.unid_ref)).otherwise(pl.col("unid_ref")).alias("unid_ref"),
                pl.when(mask).then(pl.lit(True)).otherwise(pl.col("unid_ref_manual")).alias("unid_ref_manual"),
            ])
        df.write_parquet(path)
    return {"ok": True}
