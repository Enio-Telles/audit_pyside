from pathlib import Path
import sys

import polars as pl


sys.path.insert(0, str(Path("src").resolve()))

from transformacao.fatores_conversao import calcular_fatores_conversao


def _salvar_item_unidades(
    pasta_prod: Path,
    cnpj: str,
    descricoes: list[str],
    unids: list[str],
    compras: list[float],
    vendas: list[float],
    qtd_compras: list[float],
    qtd_vendas: list[float],
) -> None:
    pl.DataFrame(
        {
            "descricao": descricoes,
            "unid": unids,
            "compras": compras,
            "vendas": vendas,
            "qtd_compras": qtd_compras,
            "qtd_vendas": qtd_vendas,
        }
    ).write_parquet(pasta_prod / f"item_unidades_{cnpj}.parquet")


def test_fator_origem_manual_preservado(tmp_path: Path):
    cnpj = "22222222000122"
    pasta_cnpj = tmp_path / cnpj
    pasta_prod = pasta_cnpj / "analises" / "produtos"
    pasta_prod.mkdir(parents=True, exist_ok=True)

    # duas unidades vinculadas ao mesmo id_agrupado
    _salvar_item_unidades(
        pasta_prod,
        cnpj,
        ["WHISKY JW BLACK 12/750ML", "WHISKY JW BLACK 12/750ML"],
        ["UN", "CX"],
        [10.0, 120.0],
        [0.0, 0.0],
        [1.0, 1.0],
        [0.0, 0.0],
    )

    pl.DataFrame(
        {
            "id_agrupado": ["id_agrupado_8"],
            "descricao_normalizada": ["WHISKY JW BLACK 12/750ML"],
            "descricao_final": ["WHISKY JW BLACK 12/750ML"],
            "descr_padrao": ["WHISKY JW BLACK 12/750ML"],
            "unid_ref_sugerida": ["UN"],
        }
    ).write_parquet(pasta_prod / f"produtos_final_{cnpj}.parquet")

    pl.DataFrame(
        {
            "id_agrupado": ["id_agrupado_8"],
            "id_produtos": ["id_agrupado_8"],
            "descr_padrao": ["WHISKY JW BLACK 12/750ML"],
            "unid": ["CX"],
            "unid_ref": ["UN"],
            "fator": [12.0],
            "fator_manual": [True],
            "unid_ref_manual": [True],
            "preco_medio": [120.0],
            "origem_preco": ["COMPRA"],
        }
    ).write_parquet(pasta_prod / f"fatores_conversao_{cnpj}.parquet")

    assert calcular_fatores_conversao(cnpj, pasta_cnpj=pasta_cnpj) is True

    df_resultado = pl.read_parquet(
        pasta_prod / f"fatores_conversao_{cnpj}.parquet"
    ).sort(["id_agrupado", "unid"])
    df_whisky = df_resultado.filter(pl.col("id_agrupado") == "id_agrupado_8")

    assert df_whisky.height == 2
    assert (
        df_whisky.filter(pl.col("unid") == "CX").row(0, named=True)["fator_origem"]
        == "manual"
    )


def test_fator_origem_fallback_sem_preco(tmp_path: Path):
    cnpj = "33333333000133"
    pasta_cnpj = tmp_path / cnpj
    pasta_prod = pasta_cnpj / "analises" / "produtos"
    pasta_prod.mkdir(parents=True, exist_ok=True)

    # Sem compras nem vendas => origem_preco = SEM_PRECO
    _salvar_item_unidades(
        pasta_prod,
        cnpj,
        ["PROD SEM PRECO"],
        ["UN"],
        [0.0],
        [0.0],
        [0.0],
        [0.0],
    )

    pl.DataFrame(
        {
            "id_agrupado": ["id_agrupado_x"],
            "descricao_normalizada": ["PROD SEM PRECO"],
            "descricao_final": ["PROD SEM PRECO"],
            "descr_padrao": ["PROD SEM PRECO"],
            "unid_ref_sugerida": ["UN"],
        }
    ).write_parquet(pasta_prod / f"produtos_final_{cnpj}.parquet")

    assert calcular_fatores_conversao(cnpj, pasta_cnpj=pasta_cnpj) is True

    df_resultado = pl.read_parquet(pasta_prod / f"fatores_conversao_{cnpj}.parquet")
    assert not df_resultado.is_empty()
    assert df_resultado.row(0, named=True)["fator_origem"] == "fallback_sem_preco"


def test_fator_origem_preco_calculado(tmp_path: Path):
    cnpj = "44444444000144"
    pasta_cnpj = tmp_path / cnpj
    pasta_prod = pasta_cnpj / "analises" / "produtos"
    pasta_prod.mkdir(parents=True, exist_ok=True)

    # Dois registros com preco medio de compra definido
    _salvar_item_unidades(
        pasta_prod,
        cnpj,
        ["PROD A", "PROD A"],
        ["UN", "CX"],
        [10.0, 100.0],
        [0.0, 0.0],
        [1.0, 10.0],
        [0.0, 0.0],
    )

    pl.DataFrame(
        {
            "id_agrupado": ["id_agrupado_a"],
            "descricao_normalizada": ["PROD A"],
            "descricao_final": ["PROD A"],
            "descr_padrao": ["PROD A"],
            "unid_ref_sugerida": ["UN"],
        }
    ).write_parquet(pasta_prod / f"produtos_final_{cnpj}.parquet")

    assert calcular_fatores_conversao(cnpj, pasta_cnpj=pasta_cnpj) is True

    df_resultado = pl.read_parquet(
        pasta_prod / f"fatores_conversao_{cnpj}.parquet"
    ).sort(["id_agrupado", "unid"])
    # todos os fatores calculados a partir de preco devem ter origem 'preco'
    assert df_resultado.filter(pl.col("fator_origem") == "preco").height >= 1
