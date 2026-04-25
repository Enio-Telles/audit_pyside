from __future__ import annotations

import polars as pl
import pytest

from metodologia_mds.service import MovimentacaoService


def _mov_df(**cols):
    return pl.DataFrame({"tipo_operacao": ["1 - ENTRADA"], **cols})


# ---------------------------------------------------------------------------
# derive_quantities – missing quantity columns
# ---------------------------------------------------------------------------


def test_derive_quantities_uses_quantidade_original() -> None:
    df = _mov_df(quantidade_original=[5.0])
    result = MovimentacaoService.derive_quantities(df)
    assert "quantidade_convertida" in result.columns
    assert result["quantidade_convertida"][0] == pytest.approx(5.0)


def test_derive_quantities_fallback_to_zero_when_no_quantidade_cols() -> None:
    df = _mov_df()
    result = MovimentacaoService.derive_quantities(df)
    assert "quantidade_convertida" in result.columns
    assert result["quantidade_convertida"][0] == pytest.approx(0.0)


# ---------------------------------------------------------------------------
# apply_conversion_factors – fator branches
# ---------------------------------------------------------------------------


def test_apply_conversion_factors_no_fator_columns() -> None:
    df = pl.DataFrame({"id_agrupado": ["A", "B"], "valor": [10.0, 20.0]})
    result = MovimentacaoService.apply_conversion_factors(df)
    assert all(v == pytest.approx(1.0) for v in result["fator_conversao"].to_list())
    assert (result["fator_conversao_origem"] == "fallback_sem_dados").all()


def test_apply_conversion_factors_override_without_fator() -> None:
    df = pl.DataFrame(
        {"id_agrupado": ["A", "B"], "fator_conversao_override": [2.0, None]}
    )
    result = MovimentacaoService.apply_conversion_factors(df)
    assert result["fator_conversao"][0] == pytest.approx(2.0)
    assert result["fator_conversao"][1] == pytest.approx(1.0)
    assert result["fator_conversao_origem"][0] == "manual"
    assert result["fator_conversao_origem"][1] == "fallback_sem_dados"


def test_apply_conversion_factors_with_df_prod_final() -> None:
    df = pl.DataFrame({"id_agrupado": ["A", "B"], "valor": [1.0, 2.0]})
    df_prod_final = pl.DataFrame(
        {"id_agrupado": ["A", "B"], "unid_ref_sugerida": ["KG", "UN"]}
    )
    result = MovimentacaoService.apply_conversion_factors(df, df_prod_final=df_prod_final)
    assert "unidade_referencia" in result.columns


# ---------------------------------------------------------------------------
# compute_preco_unit – Vl_item branch
# ---------------------------------------------------------------------------


def test_compute_preco_unit_vl_item() -> None:
    df = pl.DataFrame(
        {
            "q_conv": [2.0, 0.0],
            "Vl_item": [10.0, 5.0],
        }
    )
    result = MovimentacaoService.compute_preco_unit(df)
    assert "preco_unit" in result.columns
    assert result["preco_unit"][0] == pytest.approx(5.0)
    assert result["preco_unit"][1] == pytest.approx(0.0)


def test_compute_preco_unit_no_preco_col_returns_unchanged() -> None:
    df = pl.DataFrame({"q_conv": [2.0], "valor": [10.0]})
    result = MovimentacaoService.compute_preco_unit(df)
    assert "preco_unit" not in result.columns


# ---------------------------------------------------------------------------
# compute_is_devolucao – finnfe branch
# ---------------------------------------------------------------------------


def test_compute_is_devolucao_with_finnfe_4() -> None:
    df = pl.DataFrame({"finnfe": ["4", "1", None]})
    result = MovimentacaoService.compute_is_devolucao(df)
    assert result["__is_devolucao__"][0] is True
    assert result["__is_devolucao__"][1] is False


# ---------------------------------------------------------------------------
# mark_valid_for_average – missing excluir_estoque / q_conv_fisica
# ---------------------------------------------------------------------------


def test_mark_valid_for_average_without_excluir_estoque() -> None:
    df = pl.DataFrame(
        {"__is_devolucao__": [False, True], "q_conv_fisica": [1.0, 2.0]}
    )
    result = MovimentacaoService.mark_valid_for_average(df)
    assert "__is_valida_media__" in result.columns
    assert result["__is_valida_media__"][0] is True
    assert result["__is_valida_media__"][1] is False


def test_mark_valid_for_average_without_q_conv_fisica() -> None:
    df = pl.DataFrame({"__is_devolucao__": [False]})
    result = MovimentacaoService.mark_valid_for_average(df)
    assert "__is_valida_media__" in result.columns
    assert result["__is_valida_media__"][0] is False


# ---------------------------------------------------------------------------
# apply_neutralizations
# ---------------------------------------------------------------------------


def test_apply_neutralizations_empty_df_returns_unchanged() -> None:
    result = MovimentacaoService.apply_neutralizations(pl.DataFrame())
    assert result.is_empty()


def test_apply_neutralizations_no_num_item_returns_unchanged() -> None:
    df = pl.DataFrame({"valor": [1.0, 2.0]})
    result = MovimentacaoService.apply_neutralizations(df)
    assert "Num_item" not in result.columns
    assert "__is_neutralizada__" not in result.columns


def test_apply_neutralizations_no_key_cols_returns_unchanged() -> None:
    df = pl.DataFrame({"Num_item": ["1", "2"], "valor": [10.0, 20.0]})
    result = MovimentacaoService.apply_neutralizations(df)
    assert "__is_neutralizada__" not in result.columns


def test_apply_neutralizations_with_num_doc() -> None:
    df = pl.DataFrame(
        {
            "Num_item": ["1", "2"],
            "num_doc": ["001", "002"],
            "valor": [10.0, 20.0],
        }
    )
    result = MovimentacaoService.apply_neutralizations(df)
    assert "__is_neutralizada__" in result.columns


def test_apply_neutralizations_with_id_linha_origem() -> None:
    df = pl.DataFrame(
        {
            "Num_item": ["1", "2"],
            "id_linha_origem": ["abc|1", "def|2"],
            "valor": [10.0, 20.0],
        }
    )
    result = MovimentacaoService.apply_neutralizations(df)
    assert "__is_neutralizada__" in result.columns


def test_apply_neutralizations_existing_mov_rep_merged() -> None:
    df = pl.DataFrame(
        {
            "Num_item": ["1", "1"],
            "Chv_nfe": ["abc", "abc"],
            "mov_rep": [False, False],
            "valor": [10.0, 10.0],
        }
    )
    result = MovimentacaoService.apply_neutralizations(df)
    assert "mov_rep" in result.columns
    assert all(result["__is_neutralizada__"].to_list())


def test_apply_neutralizations_persist_neutralized(tmp_path) -> None:
    df = pl.DataFrame(
        {
            "Num_item": ["1", "1"],
            "Chv_nfe": ["abc123", "abc123"],
            "valor": [10.0, 10.0],
        }
    )
    result = MovimentacaoService.apply_neutralizations(
        df,
        persist_neutralized=True,
        output_dir=tmp_path,
        cnpj="12345678000190",
    )
    assert "__is_neutralizada__" in result.columns
    out_file = tmp_path / "linhas_neutralizadas_duplicidade_12345678000190.parquet"
    assert out_file.exists()


# ---------------------------------------------------------------------------
# load_parquet — line 30
# ---------------------------------------------------------------------------


def test_load_parquet_reads_file(tmp_path) -> None:
    df_orig = pl.DataFrame({"a": [1, 2, 3]})
    path = tmp_path / "test.parquet"
    df_orig.write_parquet(str(path))
    result = MovimentacaoService.load_parquet(path)
    assert result.shape == (3, 1)


# ---------------------------------------------------------------------------
# compute_preco_unit — early return when preco_unit already present (line 318)
# ---------------------------------------------------------------------------


def test_compute_preco_unit_ja_tem_coluna() -> None:
    df = pl.DataFrame({"preco_unit": [5.0, 10.0]})
    result = MovimentacaoService.compute_preco_unit(df)
    assert result["preco_unit"].to_list() == [5.0, 10.0]


# ---------------------------------------------------------------------------
# apply_neutralizations — emitente + serie cols (lines 365, 367)
# ---------------------------------------------------------------------------


def test_apply_neutralizations_com_emitente_serie() -> None:
    df = pl.DataFrame(
        {
            "Num_item": ["1", "2"],
            "num_doc": ["NF001", "NF001"],
            "emit_cnpj_cpf": ["12345678000190", "12345678000190"],
            "Serie": ["A", "A"],
        }
    )
    result = MovimentacaoService.apply_neutralizations(df)
    assert "__is_neutralizada__" in result.columns


# ---------------------------------------------------------------------------
# apply_neutralizations — persist fails silently (lines 410, 412)
# ---------------------------------------------------------------------------


def test_apply_neutralizations_persist_falha_silenciosa(tmp_path) -> None:
    fake_dir = tmp_path / "fake_dir.txt"
    fake_dir.write_text("not a directory")
    df = pl.DataFrame(
        {
            "Num_item": ["1", "1"],
            "Chv_nfe": ["abc123", "abc123"],
            "valor": [10.0, 10.0],
        }
    )
    result = MovimentacaoService.apply_neutralizations(
        df,
        persist_neutralized=True,
        output_dir=fake_dir,
        cnpj="12345678000190",
    )
    assert "__is_neutralizada__" in result.columns
