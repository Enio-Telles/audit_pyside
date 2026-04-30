"""
enriquecimento_fontes.py

Objetivo: Materializar as regras de rastreabilidade na pratica sem destruir origens.

Este modulo unifica as rotas de enriquecimento:
- Usa `fontes_produtos.gerar_fontes_produtos()` como camada base (id_agrupado via descricao_normalizada)
- Aplica fatores de conversao para gerar `*_enriquecido` (camada Gold com qtd_padronizada)

Etapas:
1. Delega a `fontes_produtos` para gerar `*_agr` com id_agrupado + codigo_fonte + id_linha_origem
2. JOIN fatores_conversao (calcula equivalencias de unidade_ref)
3. Salva Parquets Enriched (Camada Gold)
"""

import re
import sys
from pathlib import Path
from utilitarios.project_paths import PROJECT_ROOT
import polars as pl
from rich import print as rprint

ROOT_DIR = PROJECT_ROOT
SRC_DIR = ROOT_DIR / "src"
DADOS_DIR = ROOT_DIR / "dados"
CNPJ_ROOT = DADOS_DIR / "CNPJ"


try:
    from utilitarios.salvar_para_parquet import salvar_para_parquet
except ImportError as e:
    rprint(f"[red]Erro ao importar modulos utilitarios:[/red] {e}")
    sys.exit(1)


def gerar_enriquecimento(cnpj: str, pasta_cnpj: Path | None = None) -> bool:
    """Enriches product data with conversion factors and traceability metadata.

    Delegates to ``fontes_produtos.gerar_fontes_produtos`` to build the
    ``*_agr`` layer (containing ``id_agrupado``, ``codigo_fonte``,
    ``id_linha_origem``), then joins ``fatores_conversao`` to compute
    ``q_conv`` and ``q_conv_fisica`` and saves Gold-layer Parquets.

    Args:
        cnpj: CNPJ string (digits only or formatted).
        pasta_cnpj: Root directory for this CNPJ's data.  Defaults to
            ``CNPJ_ROOT / cnpj``.

    Returns:
        ``True`` if the ``*_agr`` step succeeded, ``False`` otherwise.
    """
    cnpj = re.sub(r"\D", "", cnpj)
    if pasta_cnpj is None:
        pasta_cnpj = CNPJ_ROOT / cnpj

    pasta_analises = pasta_cnpj / "analises" / "produtos"
    pasta_brutos = pasta_cnpj / "arquivos_parquet"
    arq_fator = pasta_analises / f"fatores_conversao_{cnpj}.parquet"

    # Delegar a camada base para fontes_produtos (garante id_agrupado, codigo_fonte, id_linha_origem)
    try:
        from transformacao.rastreabilidade_produtos.fontes_produtos import (
            gerar_fontes_produtos,
        )

        ok_agr = gerar_fontes_produtos(cnpj, pasta_cnpj)
        if not ok_agr:
            rprint(
                "[yellow]fontes_produtos falhou; tentando prosseguir com *_agr existentes...[/yellow]"
            )
    except ImportError:
        rprint("[yellow]fontes_produtos indisponivel; usando *_agr existentes.[/yellow]")

    if not arq_fator.exists():
        rprint(
            "[yellow]fatores_conversao nao encontrado. Enriquecimento sem padronizacao de unidades.[/yellow]"
        )
        # Sem fatores: apenas retornar sucesso se os *_agr foram gerados
        return ok_agr if "ok_agr" in dir() else False

    df_fator = (
        pl.read_parquet(arq_fator)
        .select(
            [
                (
                    "id_agrupado"
                    if "id_agrupado" in pl.read_parquet_schema(arq_fator).names()
                    else "id_produtos"
                ),
                "unid",
                "unid_ref",
                "fator",
            ]
        )
        .rename(
            {
                c: "id_agrupado"
                for c in ["id_agrupado", "id_produtos"]
                if c in pl.read_parquet_schema(arq_fator).names()
            }
        )
    )

    sucesso = True

    def enriquecer_a_partir_agr(
        df_agr: pl.DataFrame,
        col_unid_original: str,
        col_qtd_original: str,
        col_valor_unitario: str = None,
    ) -> pl.DataFrame:
        """Enriquece a partir de um *_agr (ja com id_agrupado)."""
        df_join = df_agr.join(
            df_fator,
            left_on=["id_agrupado", col_unid_original],
            right_on=["id_agrupado", "unid"],
            how="left",
        )

        # Default de fator eh 1.0 se nao encontrado
        df_join = df_join.with_columns(pl.col("fator").fill_null(1.0))

        # Calcular qtd_padronizada e vuncom_padronizado
        df_join = df_join.with_columns(
            [
                (pl.col(col_qtd_original).cast(pl.Float64) * pl.col("fator")).alias(
                    "qtd_padronizada"
                ),
                (
                    (pl.col(col_valor_unitario).cast(pl.Float64) / pl.col("fator")).alias(
                        "vuncom_padronizado"
                    )
                    if col_valor_unitario and col_valor_unitario in df_join.columns
                    else pl.lit(None).alias("vuncom_padronizado")
                ),
            ]
        )
        return df_join

    def processar_fonte_agr(prefix: str, col_ucom: str, col_qcom: str, col_vuncom: str = None):
        """Processa uma fonte a partir do *_agr correspondente."""
        arq_agr = pasta_brutos / f"{prefix}_agr_{cnpj}.parquet"
        if not arq_agr.exists():
            # Fallback: procurar por padroes alternativos
            alternativas = list(pasta_brutos.glob(f"{prefix}_*.parquet")) or list(
                pasta_cnpj.glob(f"{prefix}*.parquet")
            )
            if not alternativas:
                return True
            arq_agr = alternativas[0]

        rprint(f"[cyan]Enriquecendo {prefix} a partir de {arq_agr.name}...[/cyan]")
        schema_agr = pl.read_parquet_schema(arq_agr)
        if "id_agrupado" not in schema_agr:
            rprint(
                f"[yellow]Ignorando {prefix}_agr - Sem coluna id_agrupado (execute fontes_produtos primeiro)[/yellow]"
            )
            return True

        # Carrega apenas as colunas necessarias para o enriquecimento
        cols_necessarias = ["id_agrupado", col_ucom, col_qcom]
        if col_vuncom:
            cols_necessarias.append(col_vuncom)
        cols_sel = [c for c in cols_necessarias if c in schema_agr]
        df_agr = pl.scan_parquet(arq_agr).select(cols_sel).collect()

        df_enr = enriquecer_a_partir_agr(df_agr, col_ucom, col_qcom, col_vuncom)
        return salvar_para_parquet(df_enr, pasta_analises, f"{prefix}_enriquecido_{cnpj}.parquet")

    # NFe
    if list(pasta_brutos.glob("nfe_agr_*.parquet")) or list(pasta_brutos.glob("NFe_*.parquet")):
        sucesso &= processar_fonte_agr("nfe", "prod_ucom", "prod_qcom", "prod_vuncom")

    # NFCe
    if list(pasta_brutos.glob("nfce_agr_*.parquet")) or list(pasta_brutos.glob("NFCe_*.parquet")):
        sucesso &= processar_fonte_agr("nfce", "prod_ucom", "prod_qcom", "prod_vuncom")

    # C170
    if list(pasta_brutos.glob("c170_agr_*.parquet")) or list(pasta_brutos.glob("c170*.parquet")):
        sucesso &= processar_fonte_agr("c170", "unid", "qtd", None)

    # Bloco H
    if list(pasta_brutos.glob("bloco_h_agr_*.parquet")) or list(
        pasta_brutos.glob("bloco_h*.parquet")
    ):
        sucesso &= processar_fonte_agr("bloco_h", "unidade_medida", "quantidade", "valor_unitario")

    return sucesso


if __name__ == "__main__":
    if len(sys.argv) > 1:
        gerar_enriquecimento(sys.argv[1])
    else:
        c = input("CNPJ: ")
        gerar_enriquecimento(c)
