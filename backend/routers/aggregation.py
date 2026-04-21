from __future__ import annotations

from pathlib import Path

import polars as pl
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from interface_grafica.config import CNPJ_ROOT
from routers._common import sanitize_cnpj, df_to_response

router = APIRouter()


def _pasta_produtos(cnpj: str) -> Path:
    return CNPJ_ROOT / cnpj / "analises" / "produtos"


def _enriquecer_lista_descr_compl(df: pl.DataFrame, cnpj: str) -> pl.DataFrame:
    """Junta lista_descr_compl (do C170) agrupada por id_agrupado."""
    arq_c170 = CNPJ_ROOT / cnpj / "arquivos_parquet" / f"c170_xml_{cnpj}.parquet"
    if not arq_c170.exists() or "id_agrupado" not in df.columns:
        return df
    try:
        df_c170 = (
            pl.scan_parquet(arq_c170).select(["id_agrupado", "Descr_compl"]).collect()
        )
        df_agg = (
            df_c170.filter(
                pl.col("Descr_compl").is_not_null()
                & (pl.col("Descr_compl").str.strip_chars() != "")
            )
            .group_by("id_agrupado")
            .agg(pl.col("Descr_compl").unique().sort().alias("lista_descr_compl"))
        )
        df = df.join(df_agg, on="id_agrupado", how="left")
        df = df.with_columns(
            pl.col("lista_descr_compl").fill_null([]).cast(pl.List(pl.String))
        )
    except Exception:
        pass
    return df


@router.get("/{cnpj}/tabela_agrupada")
def get_tabela_agrupada(cnpj: str, page: int = 1, page_size: int = 300):
    cnpj = sanitize_cnpj(cnpj)
    pasta = _pasta_produtos(cnpj)
    candidates = [
        pasta / f"produtos_agrupados_{cnpj}.parquet",
        pasta / f"produtos_final_{cnpj}.parquet",
    ]
    path = next((p for p in candidates if p.exists()), None)
    if path is None:
        raise HTTPException(404, "Tabela agrupada não encontrada")
    df = pl.read_parquet(path)
    df = _enriquecer_lista_descr_compl(df, cnpj)
    return df_to_response(df, page, page_size)


class AggregateRequest(BaseModel):
    cnpj: str
    id_agrupado_destino: str
    ids_origem: list[str]


@router.post("/merge")
def merge_agrupados(req: AggregateRequest):
    cnpj = sanitize_cnpj(req.cnpj)
    try:
        from interface_grafica.services.aggregation_service import ServicoAgregacao

        svc = ServicoAgregacao()
        # O primeiro elemento da lista é o id canônico (destino); os demais são as origens.
        ids_ordenados = [req.id_agrupado_destino] + [
            i for i in req.ids_origem if i != req.id_agrupado_destino
        ]
        resultado = svc.agregar_linhas(
            cnpj=cnpj, ids_agrupados_selecionados=ids_ordenados
        )
        return {"ok": True, "resultado": resultado}
    except ValueError as exc:
        raise HTTPException(400, "Parâmetros inválidos para agregação.") from exc
    except Exception as exc:
        raise HTTPException(500, "Erro interno ao processar agregação.") from exc


class UnmergeRequest(BaseModel):
    cnpj: str
    id_agrupado: str


@router.post("/unmerge")
def unmerge_agrupados(req: UnmergeRequest):
    """
    Reverte o ultimo merge manual de um grupo de produtos.

    Restaura os grupos originais a partir do historico de agregacoes
    (log_agregacoes_{cnpj}.json) e recalcula a cascata de tabelas derivadas.
    """
    cnpj = sanitize_cnpj(req.cnpj)
    try:
        from interface_grafica.services.aggregation_service import ServicoAgregacao

        svc = ServicoAgregacao()
        resultado = svc.reverter_agrupamento(cnpj=cnpj, id_agrupado=req.id_agrupado)
        return {"ok": True, "resultado": resultado}
    except ValueError as exc:
        raise HTTPException(400, "Não foi possível reverter o agrupamento devido a um erro de validação.") from exc
    except Exception as exc:
        raise HTTPException(500, "Erro interno ao processar desagregação.") from exc


@router.get("/{cnpj}/historico_agregacoes")
def get_historico_agregacoes(cnpj: str):
    """
    Retorna o historico completo de merges e reversoes de agregacoes.

    Le log_agregacoes_{cnpj}.json e retorna como lista de eventos.
    """
    cnpj = sanitize_cnpj(cnpj)
    log_path = _pasta_produtos(cnpj) / f"log_agregacoes_{cnpj}.json"
    if not log_path.exists():
        return {"eventos": []}

    try:
        import json

        with open(log_path, "r", encoding="utf-8") as f:
            eventos = json.load(f)
        return {"eventos": eventos}
    except Exception as exc:
        raise HTTPException(500, "Erro ao ler historico de agregações.") from exc
