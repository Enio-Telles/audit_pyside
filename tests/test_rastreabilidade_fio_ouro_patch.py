from pathlib import Path
import sys

import polars as pl

sys.path.insert(0, str(Path("src").resolve()))

from transformacao.movimentacao_estoque_pkg.movimentacao_estoque import marcar_mov_rep_por_chave_item
from transformacao.rastreabilidade_produtos.id_agrupados import gerar_id_agrupados
from utilitarios.codigo_fonte import gerar_codigo_fonte, normalizar_codigo_fonte


def test_mov_rep_diferentes_fornecedores_com_mesmo_num_doc_nao_sao_duplicados():
    df = pl.DataFrame(
        {
            "Chv_nfe": [None, None],
            "num_doc": ["123", "123"],
            "Num_item": ["1", "1"],
            "cnpj_emitente": ["00000000000191", "99999999999999"],
            "Qtd": [10.0, 5.0],
        }
    )

    result = marcar_mov_rep_por_chave_item(df)
    assert result["mov_rep"].to_list() == [False, False]


def test_codigo_fonte_utilitario_padroniza_formato():
    assert gerar_codigo_fonte("12.345.678/0001-90", "  ABC-1 ") == "12345678000190|ABC-1"
    assert normalizar_codigo_fonte("12.345.678/0001-90 | ABC-1 ") == "12345678000190|ABC-1"


def test_id_agrupados_recupera_codigos_pela_tabela_ponte_quando_produto_final_nao_tem_lista_codigos(tmp_path: Path):
    cnpj = "12345678000120"
    pasta_cnpj = tmp_path / cnpj
    pasta_analises = pasta_cnpj / "analises" / "produtos"
    pasta_analises.mkdir(parents=True, exist_ok=True)

    pl.DataFrame(
        {
            "id_agrupado": ["AGR_1"],
            "descr_padrao": ["Produto A"],
            "descricao_final": ["Produto A"],
            "descricao": ["Produto A"],
            "lista_desc_compl": [["Lote 1"]],
            "lista_unid": [["UN"]],
            "lista_unidades_agr": [["UN"]],
            "unid_ref_sugerida": ["UN"],
        }
    ).write_parquet(pasta_analises / f"produtos_final_{cnpj}.parquet")

    pl.DataFrame(
        {
            "chave_produto": ["id_desc_1"],
            "id_agrupado": ["AGR_1"],
            "codigo_fonte": ["12345678000120|001"],
            "descricao_normalizada": ["PRODUTO A"],
        }
    ).write_parquet(pasta_analises / f"map_produto_agrupado_{cnpj}.parquet")

    assert gerar_id_agrupados(cnpj, pasta_cnpj=pasta_cnpj)
    df = pl.read_parquet(pasta_analises / f"id_agrupados_{cnpj}.parquet")
    assert df["lista_codigos"].to_list() == [["12345678000120|001"]]
