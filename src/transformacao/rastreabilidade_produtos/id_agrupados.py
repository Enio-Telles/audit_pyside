from __future__ import annotations

import re
import sys
from pathlib import Path

import polars as pl
from rich import print as rprint

ROOT_DIR = Path(r"c:\funcoes - Copia")
SRC_DIR = ROOT_DIR / "src"
DADOS_DIR = ROOT_DIR / "dados"
CNPJ_ROOT = DADOS_DIR / "CNPJ"


try:
    from utilitarios.salvar_para_parquet import salvar_para_parquet
except ImportError as e:
    rprint(f"[red]Erro ao importar modulos:[/red] {e}")
    sys.exit(1)


def _limpar_lista(valores) -> list[str]:
    saida: list[str] = []
    for valor in valores or []:
        if valor is None:
            continue
        if isinstance(valor, list):
            saida.extend(_limpar_lista(valor))
            continue
        texto = str(valor).strip()
        if texto:
            saida.append(texto)
    return sorted(set(saida))


def _consolidar_grupo_id_agrupado(df_grupo: pl.DataFrame) -> pl.DataFrame:
    registros = df_grupo.to_dicts()
    descr_padrao = None
    descricoes: list[str] = []
    codigos: list[str] = []
    unidades: list[str] = []

    for row in registros:
        if descr_padrao is None:
            valor = (row.get("descr_padrao") or "").strip() if row.get("descr_padrao") is not None else ""
            if valor:
                descr_padrao = valor

        descricoes.extend(
            _limpar_lista(
                [
                    row.get("descr_padrao"),
                    row.get("descricao_final"),
                    row.get("descricao"),
                    row.get("lista_desc_compl"),
                ]
            )
        )
        codigos.extend(_limpar_lista(row.get("lista_codigos")))
        unidades.extend(
            _limpar_lista(
                [
                    row.get("lista_unid"),
                    row.get("lista_unidades_agr"),
                    row.get("unid_ref_sugerida"),
                ]
            )
        )

    return pl.DataFrame(
        {
            "id_agrupado": [str(registros[0]["id_agrupado"])],
            "descr_padrao": [descr_padrao],
            "lista_descricoes": [_limpar_lista(descricoes)],
            "lista_codigos": [_limpar_lista(codigos)],
            "lista_unidades": [_limpar_lista(unidades)],
        }
    )


def gerar_id_agrupados(cnpj: str, pasta_cnpj: Path | None = None) -> bool:
    cnpj = re.sub(r"\D", "", cnpj or "")
    if len(cnpj) not in {11, 14}:
        raise ValueError("CPF/CNPJ invalido.")

    if pasta_cnpj is None:
        pasta_cnpj = CNPJ_ROOT / cnpj

    pasta_analises = pasta_cnpj / "analises" / "produtos"
    arq_final = pasta_analises / f"produtos_final_{cnpj}.parquet"
    if not arq_final.exists():
        rprint(f"[red]Arquivo necessario nao encontrado:[/red] {arq_final}")
        return False

    df_final = pl.read_parquet(arq_final)
    if df_final.is_empty() or "id_agrupado" not in df_final.columns:
        rprint("[yellow]produtos_final vazio ou sem id_agrupado.[/yellow]")
        return False

    df_id_agrupados = (
        df_final
        .with_columns(
            [
                pl.col("id_agrupado").cast(pl.Utf8, strict=False),
                pl.col("descr_padrao").cast(pl.Utf8, strict=False),
                pl.col("descricao_final").cast(pl.Utf8, strict=False),
                pl.col("descricao").cast(pl.Utf8, strict=False),
                pl.col("lista_desc_compl").cast(pl.List(pl.Utf8), strict=False),
                pl.col("lista_codigos").cast(pl.List(pl.Utf8), strict=False),
                pl.col("lista_unid").cast(pl.List(pl.Utf8), strict=False),
                pl.col("lista_unidades_agr").cast(pl.List(pl.Utf8), strict=False),
                pl.col("unid_ref_sugerida").cast(pl.Utf8, strict=False),
            ]
        )
        .group_by("id_agrupado")
        .map_groups(_consolidar_grupo_id_agrupado)
        .sort("id_agrupado")
    )

    return salvar_para_parquet(df_id_agrupados, pasta_analises, f"id_agrupados_{cnpj}.parquet")


if __name__ == "__main__":
    if len(sys.argv) > 1:
        gerar_id_agrupados(sys.argv[1])
    else:
        gerar_id_agrupados(input("CNPJ: "))
