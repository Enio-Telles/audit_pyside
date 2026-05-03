"""
test_golden_invariants.py - P10-01: contratos e hashes das 5 chaves invariantes.

Protege:
- id_agrupado: formato canonico id_agrupado_auto_<sha1[:12]>, SHA-1 deterministico
- id_agregado: alias de id_agrupado, mesmo tipo
- __qtd_decl_final_audit__: Float64, sem nulos, sem negativos
- q_conv: Float64, sem nulos, positivo
- q_conv_fisica: Float64, sem nulos, positivo, proporcional a q_conv
"""
import hashlib

import polars as pl
import pytest

from tests.diff_harness.golden_dataset import INVARIANTS, load_golden

pytestmark = pytest.mark.diff_harness

_SMALL = 1_000


@pytest.fixture(scope="module")
def golden_small() -> pl.DataFrame:
    return load_golden(seed=42, n_rows=_SMALL)


def _col_hash(series: pl.Series) -> int:
    """Hash deterministico de uma Series via xxHash com seeds fixos."""
    return series.hash(seed=0, seed_1=0, seed_2=0, seed_3=0).sum()


def _id_agrupado_esperado(series: pl.Series) -> pl.Series:
    return series.cast(pl.Utf8, strict=False).fill_null("").map_elements(
        lambda descricao: f"id_agrupado_auto_{hashlib.sha1(descricao.encode('utf-8')).hexdigest()[:12]}",
        return_dtype=pl.Utf8,
    )


class TestSchemaInvariants:
    def test_colunas_invariantes_existem(self, golden_small: pl.DataFrame) -> None:
        for col in INVARIANTS:
            assert col in golden_small.columns, f"Coluna invariante ausente: {col}"

    def test_id_agrupado_tipo_string(self, golden_small: pl.DataFrame) -> None:
        assert golden_small["id_agrupado"].dtype == pl.Utf8

    def test_id_agregado_tipo_string(self, golden_small: pl.DataFrame) -> None:
        assert golden_small["id_agregado"].dtype == pl.Utf8

    def test_qtd_decl_tipo_float(self, golden_small: pl.DataFrame) -> None:
        assert golden_small["__qtd_decl_final_audit__"].dtype in (pl.Float32, pl.Float64)

    def test_q_conv_tipo_float(self, golden_small: pl.DataFrame) -> None:
        assert golden_small["q_conv"].dtype in (pl.Float32, pl.Float64)

    def test_q_conv_fisica_tipo_float(self, golden_small: pl.DataFrame) -> None:
        assert golden_small["q_conv_fisica"].dtype in (pl.Float32, pl.Float64)


class TestIdAgrupado:
    def test_formato_canonico(self, golden_small: pl.DataFrame) -> None:
        invalidos = (
            golden_small["id_agrupado"]
            .str.contains(r"^id_agrupado_auto_[0-9a-f]{12}$")
            .not_()
            .sum()
        )
        assert invalidos == 0, f"{invalidos} linhas com id_agrupado fora do formato canonico"

    def test_sem_nulos(self, golden_small: pl.DataFrame) -> None:
        assert golden_small["id_agrupado"].null_count() == 0

    def test_sha1_texto_conhecido(self, golden_small: pl.DataFrame) -> None:
        esperado = _id_agrupado_esperado(golden_small["descricao"])
        divergentes = (golden_small["id_agrupado"] != esperado).sum()
        assert divergentes == 0, (
            f"{divergentes} linhas com id_agrupado diferente do SHA-1 da descricao"
        )

    def test_sha1_texto_vazio(self) -> None:
        digest = hashlib.sha1(b"").hexdigest()[:12]
        expected = f"id_agrupado_auto_{digest}"
        assert expected.startswith("id_agrupado_auto_")
        assert len(expected) == len("id_agrupado_auto_") + 12

    def test_id_agrupado_igual_id_agregado(self, golden_small: pl.DataFrame) -> None:
        divergentes = (golden_small["id_agrupado"] != golden_small["id_agregado"]).sum()
        assert divergentes == 0, f"{divergentes} linhas com id_agrupado != id_agregado"

    def test_determinismo_entre_cargas(self) -> None:
        df1 = load_golden(seed=42, n_rows=500)
        df2 = load_golden(seed=42, n_rows=500)
        assert df1["id_agrupado"].equals(df2["id_agrupado"])


class TestQConv:
    def test_sem_nulos(self, golden_small: pl.DataFrame) -> None:
        assert golden_small["q_conv"].null_count() == 0

    def test_todos_positivos(self, golden_small: pl.DataFrame) -> None:
        n_neg = (golden_small["q_conv"] <= 0).sum()
        assert n_neg == 0, f"{n_neg} linhas com q_conv <= 0"

    def test_q_conv_fisica_sem_nulos(self, golden_small: pl.DataFrame) -> None:
        assert golden_small["q_conv_fisica"].null_count() == 0

    def test_q_conv_fisica_positiva(self, golden_small: pl.DataFrame) -> None:
        n_neg = (golden_small["q_conv_fisica"] <= 0).sum()
        assert n_neg == 0, f"{n_neg} linhas com q_conv_fisica <= 0"

    def test_q_conv_fisica_proporcional_q_conv(self, golden_small: pl.DataFrame) -> None:
        # no golden dataset, q_conv_fisica = q_conv * U[0.98, 1.02]
        ratio = golden_small["q_conv_fisica"] / golden_small["q_conv"]
        assert (ratio >= 0.97).all(), "q_conv_fisica/q_conv abaixo de 0.97"
        assert (ratio <= 1.03).all(), "q_conv_fisica/q_conv acima de 1.03"

    def test_determinismo_entre_cargas(self) -> None:
        df1 = load_golden(seed=42, n_rows=500)
        df2 = load_golden(seed=42, n_rows=500)
        assert df1["q_conv"].equals(df2["q_conv"])
        assert df1["q_conv_fisica"].equals(df2["q_conv_fisica"])


class TestQtdDeclAudit:
    def test_sem_nulos(self, golden_small: pl.DataFrame) -> None:
        assert golden_small["__qtd_decl_final_audit__"].null_count() == 0

    def test_todos_positivos(self, golden_small: pl.DataFrame) -> None:
        n_neg = (golden_small["__qtd_decl_final_audit__"] <= 0).sum()
        assert n_neg == 0, f"{n_neg} linhas com __qtd_decl_final_audit__ <= 0"

    def test_determinismo_entre_cargas(self) -> None:
        df1 = load_golden(seed=42, n_rows=500)
        df2 = load_golden(seed=42, n_rows=500)
        assert df1["__qtd_decl_final_audit__"].equals(df2["__qtd_decl_final_audit__"])


class TestHashDeterministico:
    """Garante que hashes das 5 colunas sao estaveis entre execucoes."""

    def test_hash_todas_invariantes_estavel(self) -> None:
        df1 = load_golden(seed=42, n_rows=5_000)
        df2 = load_golden(seed=42, n_rows=5_000)
        for col in INVARIANTS:
            assert _col_hash(df1[col]) == _col_hash(df2[col]), (
                f"Hash da coluna {col} divergiu entre cargas com seed=42"
            )

    def test_hash_muda_com_seed_diferente(self) -> None:
        df_42 = load_golden(seed=42, n_rows=500)
        df_99 = load_golden(seed=99, n_rows=500)
        hash_42 = _col_hash(df_42["q_conv"])
        hash_99 = _col_hash(df_99["q_conv"])
        assert hash_42 != hash_99, (
            "Seeds diferentes produziram o mesmo hash de q_conv - o gerador pode nao ser aleatorio"
        )
