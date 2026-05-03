"""
test_schema_boundaries.py — P10-04: contratos de schema nas fronteiras de modulos.

Verifica que as guards de schema adicionadas nos modulos de transformacao
levantam SchemaValidacaoError com mensagens claras quando o contrato e violado.
"""
import polars as pl
import pytest

from utilitarios.validacao_schema import SchemaValidacaoError
from transformacao.produtos_itens import gerar_produtos_itens


def _write_parquets(tmp_path, cnpj: str, unid_df: pl.DataFrame, prod_df: pl.DataFrame):
    pasta = tmp_path / "CNPJ" / cnpj / "analises" / "produtos"
    pasta.mkdir(parents=True, exist_ok=True)
    unid_df.write_parquet(pasta / f"produtos_unidades_{cnpj}.parquet")
    prod_df.write_parquet(pasta / f"produtos_{cnpj}.parquet")
    return pasta


def _unid_minimo() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "descricao": ["Produto A"],
            "codigo": ["COD1"],
            "unid": ["UN"],
        }
    )


def _prod_valido() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "chave_item": ["item_1"],
            "chave_produto": ["prod_1"],
            "descricao_normalizada": ["PRODUTO A"],
        }
    )


class TestProdutosItensSchemaGuard:
    def test_guard_falta_chave_levanta_erro(self, tmp_path):
        cnpj = "00000000000191"
        prod_sem_chave = pl.DataFrame(
            {
                "descricao_normalizada": ["PRODUTO A"],
                "outro_campo": ["x"],
            }
        )
        _write_parquets(tmp_path, cnpj, _unid_minimo(), prod_sem_chave)

        with pytest.raises(SchemaValidacaoError, match="chave_item.*chave_produto"):
            gerar_produtos_itens(cnpj, tmp_path / "CNPJ" / cnpj)

    def test_guard_falta_descricao_normalizada_levanta_erro(self, tmp_path):
        cnpj = "00000000000192"
        prod_sem_norm = pl.DataFrame(
            {
                "chave_item": ["item_1"],
                "chave_produto": ["prod_1"],
            }
        )
        _write_parquets(tmp_path, cnpj, _unid_minimo(), prod_sem_norm)

        with pytest.raises(SchemaValidacaoError, match="descricao_normalizada"):
            gerar_produtos_itens(cnpj, tmp_path / "CNPJ" / cnpj)

    def test_chave_produto_sem_chave_item_e_aceito(self, tmp_path):
        cnpj = "00000000000193"
        prod_so_chave_produto = pl.DataFrame(
            {
                "chave_produto": ["prod_1"],
                "descricao_normalizada": ["PRODUTO A"],
            }
        )
        _write_parquets(tmp_path, cnpj, _unid_minimo(), prod_so_chave_produto)
        resultado = gerar_produtos_itens(cnpj, tmp_path / "CNPJ" / cnpj)
        assert resultado is True

    def test_chave_item_sem_chave_produto_e_aceito(self, tmp_path):
        cnpj = "00000000000194"
        prod_so_chave_item = pl.DataFrame(
            {
                "chave_item": ["item_1"],
                "descricao_normalizada": ["PRODUTO A"],
            }
        )
        _write_parquets(tmp_path, cnpj, _unid_minimo(), prod_so_chave_item)
        resultado = gerar_produtos_itens(cnpj, tmp_path / "CNPJ" / cnpj)
        assert resultado is True

    def test_schema_valido_executa_sem_erro(self, tmp_path):
        cnpj = "00000000000195"
        _write_parquets(tmp_path, cnpj, _unid_minimo(), _prod_valido())
        resultado = gerar_produtos_itens(cnpj, tmp_path / "CNPJ" / cnpj)
        assert resultado is True
