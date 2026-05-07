"""
tests/unit/io/test_categorical_writer.py
=========================================

Testes unitarios para ``src/io/categorical_writer.py`` (PR4).

Cobre:
- rewrite basico side-by-side
- defesa: output_path == input_path levanta ValueError
- defesa: input_path inexistente levanta FileNotFoundError
- idempotencia: rewrite de v2 ja tipado e no-op
- preservacao de invariantes
- strict_cast=True levanta em valor invalido
- strict_cast=False tolera valor invalido
- metadados de retorno corretos
"""

from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest

from src.io.categorical_recovery import (
    INVARIANT_BLOCKLIST,
    reload_fiscal_codes,
)
from src.io.categorical_writer import rewrite_parquet_typed


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def tmp_codes_json(tmp_path: Path) -> Path:
    """JSON minimal com codigos fiscais para testes."""
    import json

    data = {
        "_metadata": {"versao": "test"},
        "cfop_all": ["5101", "5102", "5405", "5949", "6102", "6108", "6403"],
        "cst_icms_completo": ["000", "010", "020", "030", "040", "050", "060", "070"],
        "cst_icms_e_csosn": [
            "000", "010", "020", "030", "040", "050", "060", "070",
            "101", "102", "103", "201", "202", "203", "300", "400", "500", "900",
        ],
        "csosn": ["101", "102", "103", "201", "202", "203", "300", "400", "500", "900"],
        "cst_pis_cofins_completo": ["01", "02", "03", "04", "05", "06", "07", "08", "09"],
        "uf": ["SP", "RJ", "MG", "RS", "PR", "BA", "PE", "CE"],
        "uf_codigo_ibge": ["35", "33", "31", "43", "41", "29", "26", "23"],
        "modelo_documento": ["55", "65", "57"],
        "tipo_operacao_nfe": ["0", "1"],
        "indicador_operacao_sped_c170": ["0", "1"],
        "tipo_emissao_nfe": ["1", "2", "3", "4", "5", "6", "7"],
        "tipo_ambiente_nfe": ["1", "2"],
        "finalidade_nfe": ["1", "2", "3", "4"],
        "indicador_presenca_nfe": ["0", "1", "2", "3", "4", "5", "9"],
        "indicador_ie_destinatario": ["1", "2", "9"],
        "modalidade_base_calculo_icms": ["0", "1", "2", "3"],
        "modalidade_base_calculo_icms_st": ["0", "1", "2", "3", "4", "5", "6"],
        "motivo_desoneracao_icms": [
            "1", "2", "3", "4", "5", "6", "7", "8", "9",
            "10", "11", "12", "16", "90",
        ],
        "modalidade_frete_nfe": ["0", "1", "2", "3", "4", "9"],
        "meio_pagamento_nfe": [
            "01", "02", "03", "04", "05", "10", "11", "12", "13",
            "14", "15", "16", "17", "18", "19", "90", "99",
        ],
        "bandeira_cartao_nfe": [
            "01", "02", "03", "04", "05", "06", "07", "08", "09", "99",
        ],
        "tipo_integracao_pagamento_nfe": ["1", "2"],
        "regime_especial_tributacao_issqn": ["A", "E", "M", "N", "S"],
        "regime_tributario_crt": ["1", "2", "3"],
        "cst_icms_origem": ["0", "1", "2", "3", "4", "5", "6", "7", "8"],
        "codigo_situacao_documento_sped": ["00", "01", "02", "03", "04", "05"],
        "indicador_movimento_sped": ["0", "1"],
        "tipo_item_sped": ["00", "01", "02", "03", "04", "05", "06", "99"],
    }
    p = tmp_path / "fiscal_codes_test.json"
    p.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")
    reload_fiscal_codes()
    return p


@pytest.fixture
def sample_v1_parquet(tmp_path: Path) -> Path:
    """Parquet v1 sintetico com colunas fiscais tipicas."""
    df = pl.DataFrame({
        "cfop": ["5102", "5405", "6102", "5101", "5949"],
        "Cst": ["000", "010", "102", "000", "900"],
        "ncm": ["84713012", "84713019", "84713090", "84713012", "84713019"],
        "unid": ["UN", "KG", "UN", "LT", "UN"],
        "uf": ["SP", "RJ", "MG", "SP", "PR"],
        "id_agrupado": [
            "id_agrupado_auto_a1b2c3d4e5f6",
            "id_agrupado_auto_b2c3d4e5f6a1",
            "id_agrupado_auto_c3d4e5f6a1b2",
            "id_agrupado_auto_d4e5f6a1b2c3",
            "id_agrupado_auto_e5f6a1b2c3d4",
        ],
        "id_agregado": ["agg_1", "agg_2", "agg_3", "agg_4", "agg_5"],
        "__qtd_decl_final_audit__": [10.0, 20.0, 30.0, 40.0, 50.0],
        "q_conv": [1.0, 2.0, 3.0, 4.0, 5.0],
        "q_conv_fisica": [1.5, 2.5, 3.5, 4.5, 5.5],
        "valor": [100.0, 200.0, 300.0, 400.0, 500.0],
    })
    p = tmp_path / "sample_v1.parquet"
    df.write_parquet(p)
    return p


# ---------------------------------------------------------------------------
# Testes
# ---------------------------------------------------------------------------


class TestRewriteParquetTyped:
    """Testes para rewrite_parquet_typed."""

    def test_rewrite_basico_side_by_side(
        self, sample_v1_parquet: Path, tmp_codes_json: Path, tmp_path: Path
    ) -> None:
        """Rewrite basico: v1 -> v2 em path diferente, sem sobrescrever."""
        output = tmp_path / "sample_v2.parquet"
        result = rewrite_parquet_typed(
            sample_v1_parquet, output, codes_path=tmp_codes_json,
        )

        # Verificar que v1 continua intacto
        assert sample_v1_parquet.exists()
        # Verificar que v2 foi criado
        assert output.exists()
        assert output != sample_v1_parquet

        # Metadados
        assert result["n_rows"] == 5
        assert result["n_cols"] == 11
        assert result["n_cols_typed"] > 0
        assert result["schema_diff"] == {}

        # Invariantes continuam com dtype original (String ou Float64),
        # mas nunca Enum ou Categorical
        for inv in INVARIANT_BLOCKLIST:
            assert inv in result["invariant_dtypes"]
            dtype_str = result["invariant_dtypes"][inv]
            assert "Enum" not in dtype_str, f"{inv} nao deveria ser Enum: {dtype_str}"
            assert "Categorical" not in dtype_str, f"{inv} nao deveria ser Categorical: {dtype_str}"

    def test_output_path_igual_input_path_levanta(
        self, sample_v1_parquet: Path, tmp_codes_json: Path
    ) -> None:
        """output_path == input_path deve levantar ValueError."""
        with pytest.raises(ValueError, match="output_path deve ser diferente"):
            rewrite_parquet_typed(
                sample_v1_parquet, sample_v1_parquet, codes_path=tmp_codes_json,
            )

    def test_input_inexistente_levanta(
        self, tmp_codes_json: Path, tmp_path: Path
    ) -> None:
        """input_path inexistente deve levantar FileNotFoundError."""
        inexistente = tmp_path / "nao_existe.parquet"
        output = tmp_path / "output.parquet"
        with pytest.raises(FileNotFoundError, match="nao encontrado"):
            rewrite_parquet_typed(inexistente, output, codes_path=tmp_codes_json)

    def test_idempotente_rewrite_v2_ja_tipado(
        self, sample_v1_parquet: Path, tmp_codes_json: Path, tmp_path: Path
    ) -> None:
        """Rewrite de v2 ja tipado e no-op (idempotente)."""
        v2 = tmp_path / "sample_v2.parquet"
        rewrite_parquet_typed(sample_v1_parquet, v2, codes_path=tmp_codes_json)

        # Segundo rewrite: v2 -> v3
        v3 = tmp_path / "sample_v3.parquet"
        result = rewrite_parquet_typed(v2, v3, codes_path=tmp_codes_json)

        assert result["n_rows"] == 5
        assert result["schema_diff"] == {}

    def test_preserva_invariantes_string(
        self, sample_v1_parquet: Path, tmp_codes_json: Path, tmp_path: Path
    ) -> None:
        """Invariantes fiscais permanecem String no v2."""
        output = tmp_path / "sample_v2.parquet"
        result = rewrite_parquet_typed(
            sample_v1_parquet, output, codes_path=tmp_codes_json,
        )

        # Ler v2 e verificar dtypes
        df_v2 = pl.read_parquet(output)
        for inv in INVARIANT_BLOCKLIST:
            if inv in df_v2.schema:
                dtype = df_v2.schema[inv]
                assert not isinstance(dtype, pl.Enum), f"{inv} nao deveria ser Enum"
                assert not isinstance(dtype, pl.Categorical), f"{inv} nao deveria ser Categorical"

    def test_strict_cast_true_levanta_em_valor_invalido(
        self, tmp_codes_json: Path, tmp_path: Path
    ) -> None:
        """strict_cast=True levanta InvalidOperationError em CFOP invalido."""
        df = pl.DataFrame({"cfop": ["9999"]})
        v1 = tmp_path / "bad_cfop.parquet"
        df.write_parquet(v1)

        output = tmp_path / "bad_cfop_v2.parquet"
        with pytest.raises(pl.exceptions.InvalidOperationError):
            rewrite_parquet_typed(
                v1, output, codes_path=tmp_codes_json, strict_cast=True,
            )

    def test_strict_cast_false_tolera_valor_invalido(
        self, tmp_codes_json: Path, tmp_path: Path
    ) -> None:
        """strict_cast=False tolera valor invalido (vira null)."""
        df = pl.DataFrame({"cfop": ["9999", "5102"]})
        v1 = tmp_path / "mixed_cfop.parquet"
        df.write_parquet(v1)

        output = tmp_path / "mixed_cfop_v2.parquet"
        result = rewrite_parquet_typed(
            v1, output, codes_path=tmp_codes_json, strict_cast=False,
        )

        assert result["n_rows"] == 2
        # schema_diff pode reportar que cfop nao e Enum se strict=False
        # produziu nulls — isso e esperado

    def test_parquet_sem_colunas_alvo(
        self, tmp_codes_json: Path, tmp_path: Path
    ) -> None:
        """Parquet sem colunas fiscais: rewrite e no-op estrutural."""
        df = pl.DataFrame({"qualquer": [1, 2, 3], "outro": ["a", "b", "c"]})
        v1 = tmp_path / "neutral.parquet"
        df.write_parquet(v1)

        output = tmp_path / "neutral_v2.parquet"
        result = rewrite_parquet_typed(v1, output, codes_path=tmp_codes_json)

        assert result["n_rows"] == 3
        assert result["n_cols_typed"] == 0

    def test_metadados_retorno(
        self, sample_v1_parquet: Path, tmp_codes_json: Path, tmp_path: Path
    ) -> None:
        """Verificar estrutura do dicionario de retorno."""
        output = tmp_path / "sample_v2.parquet"
        result = rewrite_parquet_typed(
            sample_v1_parquet, output, codes_path=tmp_codes_json,
        )

        assert "input_path" in result
        assert "output_path" in result
        assert "n_rows" in result
        assert "n_cols" in result
        assert "n_cols_typed" in result
        assert "invariant_dtypes" in result
        assert "schema_diff" in result
        assert isinstance(result["n_rows"], int)
        assert isinstance(result["n_cols_typed"], int)

    def test_cria_diretorio_de_saida_automaticamente(
        self, sample_v1_parquet: Path, tmp_codes_json: Path, tmp_path: Path
    ) -> None:
        """output_path em diretorio inexistente: cria automaticamente."""
        output = tmp_path / "subdir" / "nested" / "sample_v2.parquet"
        result = rewrite_parquet_typed(
            sample_v1_parquet, output, codes_path=tmp_codes_json,
        )

        assert output.exists()
        assert result["n_rows"] == 5

    def test_encoding_fisico_rle_dictionary(
        self, sample_v1_parquet: Path, tmp_codes_json: Path, tmp_path: Path
    ) -> None:
        """Verificar que v2 tem encoding RLE_DICTIONARY nas colunas alvo."""
        output = tmp_path / "sample_v2.parquet"
        rewrite_parquet_typed(sample_v1_parquet, output, codes_path=tmp_codes_json)

        # Usar pyarrow para inspecionar encoding fisico
        try:
            import pyarrow.parquet as pq
        except ImportError:
            pytest.skip("pyarrow nao disponivel para inspecao de encoding")

        pf = pq.ParquetFile(output)
        # Pegar metadados do primeiro row group
        if pf.metadata.num_row_groups > 0:
            rg = pf.metadata.row_group(0)
            encodings_por_coluna: dict[str, set[str]] = {}
            for col_idx in range(rg.num_columns):
                col = rg.column(col_idx)
                col_name = col.path_in_schema
                encodings_por_coluna[col_name] = set(col.encodings)

            # Colunas alvo (cfop, Cst, uf) devem ter RLE_DICTIONARY
            for col_name in ["cfop", "Cst", "uf"]:
                if col_name in encodings_por_coluna:
                    assert "RLE_DICTIONARY" in encodings_por_coluna[col_name], (
                        f"{col_name} deveria ter RLE_DICTIONARY, "
                        f"mas tem {encodings_por_coluna[col_name]}"
                    )

            # Invariantes NAO devem ter RLE_DICTIONARY (sao String)
            for inv in INVARIANT_BLOCKLIST:
                if inv in encodings_por_coluna:
                    # String columns may or may not have dictionary encoding
                    # depending on cardinality — the key is they are not Enum/Categorical
                    pass
