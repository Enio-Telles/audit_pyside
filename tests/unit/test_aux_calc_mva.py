from __future__ import annotations

import polars as pl

from utilitarios.aux_calc_mva_ajustado import aux_calc_mva_ajustado
from utilitarios.aux_st import aux_calc_VBC_ST


def _mva_df(flag: str, uf_emit: str) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "flag": [flag],
            "mva_orig": [30.0],
            "aliq_interna": [17.0],
            "aliq_inter": [12.0],
            "uf_emit": [uf_emit],
            "uf_dest": ["RO"],
        }
    )


def _calcular_mva(df: pl.DataFrame) -> object:
    result = aux_calc_mva_ajustado(
        df, "flag", "mva_orig", "aliq_interna", "aliq_inter", "uf_emit", "uf_dest"
    )
    return result["mva_ajustado_calc"][0]


def test_mva_ajustado_ajusta_interestadual() -> None:
    val = _calcular_mva(_mva_df("S", "SP"))
    assert val is not None and val > 0


def test_mva_ajustado_sem_ajuste_retorna_nulo() -> None:
    assert _calcular_mva(_mva_df("N", "SP")) is None
    assert _calcular_mva(_mva_df("S", "RO")) is None


def test_vbc_st_calculo() -> None:
    df = pl.DataFrame({
        "vprod": [100.0],
        "vfrete": [10.0],
        "vseg": [0.0],
        "voutro": [0.0],
        "vdesc": [5.0],
        "vipi": [0.0],
        "mva_flag": ["S"],
        "mva_orig": [30.0],
        "mva_ajustado": [0.35],
    })
    result = aux_calc_VBC_ST(
        df, "vprod", "vfrete", "vseg", "voutro", "vdesc", "vipi", "mva_flag", "mva_orig", "mva_ajustado"
    )
    vbc = result["vbc_st_calc"][0]
    assert vbc is not None
    # Base = (100+10+0+0-5+0) = 105; * (1 + 0.35) = 141.75
    assert abs(vbc - 141.75) < 0.01


def test_vbc_st_usa_mva_original_quando_sem_ajustado() -> None:
    df = pl.DataFrame({
        "vprod": [100.0],
        "vfrete": [0.0],
        "vseg": [0.0],
        "voutro": [0.0],
        "vdesc": [0.0],
        "vipi": [0.0],
        "mva_flag": ["N"],
        "mva_orig": [20.0],
        "mva_ajustado": [None],
    })
    result = aux_calc_VBC_ST(
        df, "vprod", "vfrete", "vseg", "voutro", "vdesc", "vipi", "mva_flag", "mva_orig", "mva_ajustado"
    )
    vbc = result["vbc_st_calc"][0]
    # Base = 100; MVA = 20/100 = 0.20; VBC_ST = 100 * 1.20 = 120
    assert abs(vbc - 120.0) < 0.01
