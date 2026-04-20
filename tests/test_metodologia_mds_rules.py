import polars as pl
from src.metodologia_mds.service import MovimentacaoService


def test_compute_is_devolucao_from_flags():
    df = pl.DataFrame({
        "dev_venda": ["S"],
        "q_conv": [1.0],
        "q_conv_fisica": [1.0],
        "tipo_operacao": ["1 - ENTRADA"],
    })

    out = MovimentacaoService.compute_is_devolucao(df)
    assert "__is_devolucao__" in out.columns
    assert out["__is_devolucao__"][0] is True


def test_mark_valid_for_average_excludes_devolucao_and_excluir():
    df = pl.DataFrame({
        "dev_simples": ["S"],
        "excluir_estoque": [None],
        "q_conv": [2.0],
        "q_conv_fisica": [2.0],
    })

    out = MovimentacaoService.compute_is_devolucao(df)
    out = MovimentacaoService.mark_valid_for_average(out)
    assert out["__is_devolucao__"][0] is True
    assert out["__is_valida_media__"][0] is False

    # excluir_estoque should also block
    df2 = pl.DataFrame({
        "dev_simples": [None],
        "excluir_estoque": ["S"],
        "q_conv": [2.0],
        "q_conv_fisica": [2.0],
    })
    out2 = MovimentacaoService.compute_is_devolucao(df2)
    out2 = MovimentacaoService.mark_valid_for_average(out2)
    assert out2["__is_devolucao__"][0] is False
    assert out2["__is_valida_media__"][0] is False


def test_compute_preco_unit_derived():
    df = pl.DataFrame({"q_conv": [2.0], "preco_item": [10.0]})
    out = MovimentacaoService.compute_preco_unit(df)
    assert "preco_unit" in out.columns
    assert out["preco_unit"][0] == 5.0


def test_apply_neutralizations_marks_duplicates():
    df = pl.DataFrame({
        "Chv_nfe": ["A", "A", "B"],
        "Num_item": ["1", "1", "1"],
    })

    out = MovimentacaoService.apply_neutralizations(df)
    assert "mov_rep" in out.columns
    # first two rows are duplicates
    assert out["mov_rep"][0] is True
    assert out["mov_rep"][1] is True
    assert out["mov_rep"][2] is False
    assert out["__is_neutralizada__"][0] is True
    assert out["__is_neutralizada__"][2] is False
