from pathlib import Path
import sys

import polars as pl

sys.path.insert(0, str(Path("src").resolve()))

from transformacao.rastreabilidade_produtos.fontes_produtos import gerar_fontes_produtos  # noqa: E402


def _preparar_contexto(tmp_path: Path, cnpj: str) -> Path:
    pasta_cnpj = tmp_path / cnpj
    pasta_analises = pasta_cnpj / "analises" / "produtos"
    pasta_brutos = pasta_cnpj / "arquivos_parquet"
    pasta_analises.mkdir(parents=True, exist_ok=True)
    pasta_brutos.mkdir(parents=True, exist_ok=True)

    pl.DataFrame(
        {
            "id_agrupado": ["AGR_1", "AGR_2"],
            "descricao_normalizada": ["PRODUTO A", "PRODUTO B"],
            "descr_padrao": ["Produto A", "Produto B"],
            "ncm_padrao": ["22083000", "22083000"],
            "cest_padrao": ["0300700", "0300700"],
            "co_sefin_final": ["123", "123"],
            "unid_ref_sugerida": ["UN", "UN"],
        }
    ).write_parquet(pasta_analises / f"produtos_final_{cnpj}.parquet")

    return pasta_cnpj


def test_fontes_prioriza_codigo_fonte(tmp_path: Path):
    cnpj = "12345678000110"
    pasta_cnpj = _preparar_contexto(tmp_path, cnpj)
    pasta_analises = pasta_cnpj / "analises" / "produtos"
    pasta_brutos = pasta_cnpj / "arquivos_parquet"

    pl.DataFrame(
        {
            "id_agrupado": ["AGR_1", "AGR_2"],
            "codigo_fonte": ["111|001", "111|002"],
            "descricao_normalizada": ["PRODUTO A", "PRODUTO A"],
        }
    ).write_parquet(pasta_analises / f"map_produto_agrupado_{cnpj}.parquet")

    pl.DataFrame(
        {
            "codigo_fonte": ["111|001"],
            "prod_xprod": ["Produto A"],
            "chave_acesso": ["NFE1"],
        }
    ).write_parquet(pasta_brutos / f"nfe_{cnpj}.parquet")

    assert gerar_fontes_produtos(cnpj, pasta_cnpj=pasta_cnpj)

    df_saida = pl.read_parquet(pasta_brutos / f"nfe_agr_{cnpj}.parquet")
    assert df_saida["id_agrupado"].to_list() == ["AGR_1"]
    assert df_saida["origem_vinculo_agrupamento"].to_list() == ["codigo_fonte"]


def test_fontes_separa_fora_escopo_canonico(tmp_path: Path):
    cnpj = "12345678000112"
    pasta_cnpj = _preparar_contexto(tmp_path, cnpj)
    pasta_analises = pasta_cnpj / "analises" / "produtos"
    pasta_brutos = pasta_cnpj / "arquivos_parquet"

    pl.DataFrame(
        {
            "id_agrupado": ["AGR_1", "AGR_2"],
            "codigo_fonte": ["111|001", "111|002"],
            "descricao_normalizada": ["PRODUTO A", "PRODUTO B"],
        }
    ).write_parquet(pasta_analises / f"map_produto_agrupado_{cnpj}.parquet")

    pl.DataFrame(
        {
            "codigo_fonte": ["111|001", "111|001"],
            "prod_xprod": ["Produto A", "Produto A"],
            "tipo_operacao": ["0 - ENTRADA", "1 - SAIDA"],
            "co_emitente": [cnpj, cnpj],
            "chave_acesso": ["NFE1", "NFE2"],
        }
    ).write_parquet(pasta_brutos / f"nfe_{cnpj}.parquet")

    assert gerar_fontes_produtos(cnpj, pasta_cnpj=pasta_cnpj)

    df_saida = pl.read_parquet(pasta_brutos / f"nfe_agr_{cnpj}.parquet")
    assert df_saida["id_agrupado"].to_list() == ["AGR_1"]

    df_auditoria = pl.read_parquet(
        pasta_analises / f"nfe_agr_fora_escopo_canonico_{cnpj}.parquet"
    )
    assert df_auditoria.height == 1
    assert df_auditoria["motivo_fora_escopo_canonico"].to_list() == ["fora_escopo_canonico"]


def test_fontes_gera_auditoria_para_descricao_ambigua(tmp_path: Path):
    cnpj = "12345678000111"
    pasta_cnpj = _preparar_contexto(tmp_path, cnpj)
    pasta_analises = pasta_cnpj / "analises" / "produtos"
    pasta_brutos = pasta_cnpj / "arquivos_parquet"

    pl.DataFrame(
        {
            "id_agrupado": ["AGR_1", "AGR_2"],
            "codigo_fonte": [None, None],
            "descricao_normalizada": ["PRODUTO A", "PRODUTO A"],
        }
    ).write_parquet(pasta_analises / f"map_produto_agrupado_{cnpj}.parquet")

    pl.DataFrame(
        {
            "prod_xprod": ["Produto A"],
            "chave_acesso": ["NFE1"],
        }
    ).write_parquet(pasta_brutos / f"nfe_{cnpj}.parquet")

    assert not gerar_fontes_produtos(cnpj, pasta_cnpj=pasta_cnpj)

    df_auditoria = pl.read_parquet(pasta_analises / f"nfe_agr_sem_id_agrupado_{cnpj}.parquet")
    assert df_auditoria["motivo_sem_id_agrupado"].to_list() == ["descricao_normalizada_ambigua"]
