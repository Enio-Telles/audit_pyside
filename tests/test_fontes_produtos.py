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


def test_fontes_preserva_nfce_aquisicao_de_terceiro(tmp_path: Path):
    """NFC-e com co_emitente != CNPJ e tipo_operacao=ENTRADA permanece em nfce_agr."""

    cnpj = "84654326000394"
    pasta_cnpj = _preparar_contexto(tmp_path, cnpj)
    pasta_analises = pasta_cnpj / "analises" / "produtos"
    pasta_brutos = pasta_cnpj / "arquivos_parquet"

    pl.DataFrame(
        {
            "id_agrupado": ["AGR_1"],
            "codigo_fonte": ["111|001"],
            "descricao_normalizada": ["PRODUTO A"],
        }
    ).write_parquet(pasta_analises / f"map_produto_agrupado_{cnpj}.parquet")

    pl.DataFrame(
        {
            "codigo_fonte": ["111|001", "111|001"],
            "prod_xprod": ["Produto A", "Produto A"],
            "tipo_operacao": ["0 - ENTRADA", "0 - ENTRADA"],
            "co_emitente": ["12345678000111", "98765432000122"],
            "co_cfop": ["5101", "5102"],
            "chave_acesso": ["NFCE_AQUI_1", "NFCE_AQUI_2"],
            "prod_nitem": [1, 1],
        }
    ).write_parquet(pasta_brutos / f"nfce_{cnpj}.parquet")

    assert gerar_fontes_produtos(cnpj, pasta_cnpj=pasta_cnpj)

    df_saida = pl.read_parquet(pasta_brutos / f"nfce_agr_{cnpj}.parquet")
    assert df_saida.height == 2
    assert df_saida["id_agrupado"].to_list() == ["AGR_1", "AGR_1"]

    arq_fora = pasta_analises / f"nfce_agr_fora_escopo_canonico_{cnpj}.parquet"
    assert not arq_fora.exists()


def test_fontes_separa_terceiro_para_terceiro_em_nfe(tmp_path: Path):
    cnpj = "12345678000114"
    pasta_cnpj = _preparar_contexto(tmp_path, cnpj)
    pasta_analises = pasta_cnpj / "analises" / "produtos"
    pasta_brutos = pasta_cnpj / "arquivos_parquet"

    pl.DataFrame(
        {
            "id_agrupado": ["AGR_1"],
            "codigo_fonte": ["111|001"],
            "descricao_normalizada": ["PRODUTO A"],
        }
    ).write_parquet(pasta_analises / f"map_produto_agrupado_{cnpj}.parquet")

    pl.DataFrame(
        {
            "codigo_fonte": ["111|001", "111|001"],
            "prod_xprod": ["Produto A", "Produto A"],
            "tipo_operacao": ["SAIDA", "SAIDA"],
            "co_emitente": ["12345678000111", "98765432000122"],
            "co_cfop": ["5101", "5102"],
            "chave_acesso": ["NFE3", "NFE4"],
        }
    ).write_parquet(pasta_brutos / f"nfe_{cnpj}.parquet")

    assert gerar_fontes_produtos(cnpj, pasta_cnpj=pasta_cnpj)

    df_saida = pl.read_parquet(pasta_brutos / f"nfe_agr_{cnpj}.parquet")
    assert df_saida.height == 0

    df_auditoria = pl.read_parquet(pasta_analises / f"nfe_agr_fora_escopo_canonico_{cnpj}.parquet")
    assert df_auditoria.height == 2
    assert df_auditoria["motivo_fora_escopo_canonico"].to_list() == [
        "fora_escopo_canonico",
        "fora_escopo_canonico",
    ]


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
    assert df_saida["id_agrupado"].to_list() == ["AGR_1", "AGR_1"]

    arq_auditoria = pasta_analises / f"nfe_agr_fora_escopo_canonico_{cnpj}.parquet"
    assert not arq_auditoria.exists()


def test_fontes_grava_vazio_quando_tudo_vai_para_fora_escopo(tmp_path: Path):
    cnpj = "12345678000113"
    pasta_cnpj = _preparar_contexto(tmp_path, cnpj)
    pasta_analises = pasta_cnpj / "analises" / "produtos"
    pasta_brutos = pasta_cnpj / "arquivos_parquet"

    pl.DataFrame(
        {
            "id_agrupado": ["AGR_1"],
            "codigo_fonte": ["111|001"],
            "descricao_normalizada": ["PRODUTO A"],
        }
    ).write_parquet(pasta_analises / f"map_produto_agrupado_{cnpj}.parquet")

    pl.DataFrame(
        {
            "id_agrupado": ["STALE"],
            "codigo_fonte": ["stale|001"],
            "descricao_normalizada": ["STALE"],
        }
    ).write_parquet(pasta_brutos / f"nfe_agr_{cnpj}.parquet")

    # Todas as linhas sao entrada de terceiros — 100% vai para fora_escopo
    pl.DataFrame(
        {
            "codigo_fonte": ["111|001", "111|001"],
            "prod_xprod": ["Produto A", "Produto A"],
            "tipo_operacao": ["1 - SAIDA", "1 - SAIDA"],
            "co_emitente": ["99999999000199", "88888888000188"],
            "co_cfop": ["5101", "5102"],
            "chave_acesso": ["NFE1", "NFE2"],
        }
    ).write_parquet(pasta_brutos / f"nfe_{cnpj}.parquet")

    assert gerar_fontes_produtos(cnpj, pasta_cnpj=pasta_cnpj)

    # Arquivo principal deve existir e ser vazio (nao stale de run anterior)
    arq_principal = pasta_brutos / f"nfe_agr_{cnpj}.parquet"
    assert arq_principal.exists(), "nfe_agr deve existir mesmo quando vazio"
    df_saida = pl.read_parquet(arq_principal)
    assert df_saida.height == 0, f"esperado 0 linhas, obtido {df_saida.height}"

    # Auditoria deve conter as 2 linhas descartadas
    df_auditoria = pl.read_parquet(pasta_analises / f"nfe_agr_fora_escopo_canonico_{cnpj}.parquet")
    assert df_auditoria.height == 2


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
