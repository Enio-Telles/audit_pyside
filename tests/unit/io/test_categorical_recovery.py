"""
Testes de ``src/io/categorical_recovery.py``.

Cobertura:
- Carregamento do JSON e construÃ§Ã£o de cfop_all
- Build de ENUM_MAP e CATEG_MAP
- ``scan_parquet_typed``: cast correto, idempotÃªncia, defesa em
  profundidade vs invariantes
- ``cast_dataframe_typed``: versÃ£o eager
- ValidaÃ§Ãµes: ``validate_schema_post_cast``, ``get_invariant_dtypes``,
  ``assert_no_invariant_categorized``
- Edge cases: Parquet sem colunas-alvo, JSON ausente, valor fora do
  domÃ­nio Enum

ConvenÃ§Ã£o: usa fixture ``tmp_codes_json`` que cria JSON minimalista em
``tmp_path`` para isolar testes do cadastro real.
"""

from __future__ import annotations

import json
from pathlib import Path

import polars as pl
import pytest

from src.io import categorical_recovery as cr


# =====================================================================
# Fixtures
# =====================================================================


@pytest.fixture
def minimal_codes() -> dict[str, list[str] | dict[str, str]]:
    """Cadastro JSON minimalista cobrindo todos os mapeamentos do mÃ³dulo."""
    return {
        "_metadata": {"version": "test"},
        "uf": ["RO", "SP", "MG", "RJ", "EX"],
        "uf_codigo_ibge": ["11", "35", "31", "33"],
        "cst_icms_origem": ["0", "1", "2"],
        "cst_icms_completo": ["000", "010", "060", "090"],
        "csosn": ["101", "102", "500", "900"],
        "cst_pis_cofins_completo": ["01", "49", "50", "99"],
        "modelo_documento": ["55", "65"],
        "tipo_operacao_nfe": ["0", "1"],
        "tipo_emissao_nfe": ["1", "2"],
        "tipo_ambiente_nfe": ["1", "2"],
        "finalidade_nfe": ["1", "2", "3", "4"],
        "regime_tributario_crt": ["1", "2", "3", "4"],
        "indicador_operacao_sped_c170": ["0", "1"],
        "codigo_situacao_documento_sped": ["00", "01", "02"],
        "indicador_movimento_sped": ["0", "1"],
        "tipo_item_sped": ["00", "01", "99"],
        "indicador_presenca_nfe": ["0", "1", "2", "3", "4", "5", "9"],
        "indicador_ie_destinatario": ["1", "2", "9"],
        "modalidade_base_calculo_icms": {"0": "MVA", "1": "Pauta", "2": "Preco", "3": "Valor"},
        "modalidade_base_calculo_icms_st": {"0": "Preco", "1": "Negativa", "2": "Positiva"},
        "motivo_desoneracao_icms": {"1": "Taxi", "3": "Produtor", "9": "Outros"},
        "modalidade_frete_nfe": {"0": "CIF", "1": "FOB", "9": "Sem frete"},
        "meio_pagamento_nfe": {"01": "Dinheiro", "03": "Cartao", "17": "PIX"},
        "bandeira_cartao_nfe": {"01": "Visa", "02": "Mastercard", "99": "Outros"},
        "tipo_integracao_pagamento_nfe": {"1": "Integrado", "2": "Nao integrado"},
        "regime_especial_tributacao_issqn": {"1": "ME municipal", "6": "ME/EPP"},
        "cfop_entrada_estadual": ["1102", "1551"],
        "cfop_saida_estadual": ["5102", "5405", "5949"],
        "cfop_saida_interestadual": ["6102", "6405"],
    }


@pytest.fixture
def tmp_codes_json(tmp_path: Path, minimal_codes: dict) -> Path:
    """JSON minimalista em tmp_path; limpa cache do mÃ³dulo entre testes."""
    p = tmp_path / "fiscal_codes_test.json"
    p.write_text(json.dumps(minimal_codes, ensure_ascii=False), encoding="utf-8")
    cr.reload_fiscal_codes()
    yield p
    cr.reload_fiscal_codes()


@pytest.fixture
def sample_parquet(tmp_path: Path) -> Path:
    """Parquet de teste com mix de colunas: tipÃ¡veis, invariantes, neutras."""
    df = pl.DataFrame({
        # TipÃ¡veis para Enum
        "cfop": ["5102", "5405", "5102", "1102", "5949"],
        "cst_icms": ["000", "060", "000", "010", "090"],
        "uf": ["RO", "SP", "RO", "MG", "EX"],
        "mod": ["55", "65", "55", "55", "65"],
        "modBC": ["0", "1", "2", "3", "0"],
        "modFrete": ["0", "1", "9", "0", "1"],
        "tPag": ["01", "03", "17", "01", "17"],
        "indFinal": ["1", "0", "1", None, "0"],
        "indEscala": ["S", "N", None, "S", "N"],
        "IND_MOV": ["0", "1", "0", "1", "0"],
        # TipÃ¡veis para Categorical
        "ncm": ["12345678", "98765432", "12345678", "00000000", "11111111"],
        "cest": ["28.038.00", "01.001.00", "28.038.00", None, None],
        "unid": ["UN", "PC", "UN", "CX", "UN"],
        # Invariantes â€” NUNCA categorizar
        "id_agrupado": ["a", "b", "c", "d", "e"],
        "id_agregado": ["x", "y", "z", "w", "v"],
        "__qtd_decl_final_audit__": [10.0, 20.0, 30.0, 40.0, 50.0],
        "q_conv": [1.0, 2.0, 3.0, 4.0, 5.0],
        "q_conv_fisica": [1.5, 2.5, 3.5, 4.5, 5.5],
        # Outros (nÃ£o tocar)
        "valor_total": [100.0, 200.0, 300.0, 400.0, 500.0],
        "Descr_item": ["produto a", "produto b", "produto c", "produto d", "produto e"],
    })
    p = tmp_path / "test.parquet"
    df.write_parquet(p)
    return p


# =====================================================================
# load_fiscal_codes
# =====================================================================


class TestLoadFiscalCodes:
    def test_carrega_listas_basicas(self, tmp_codes_json: Path) -> None:
        codes = cr.load_fiscal_codes(tmp_codes_json)
        assert "uf" in codes
        assert codes["uf"] == ["RO", "SP", "MG", "RJ", "EX"]
        assert "cst_icms_completo" in codes

    def test_metadata_e_excluido(self, tmp_codes_json: Path) -> None:
        codes = cr.load_fiscal_codes(tmp_codes_json)
        assert "_metadata" not in codes

    def test_cfop_all_uniao_deterministica(self, tmp_codes_json: Path) -> None:
        codes = cr.load_fiscal_codes(tmp_codes_json)
        assert "cfop_all" in codes
        # Inclui ambos entrada e saÃ­da sem duplicatas
        cfops = codes["cfop_all"]
        assert "1102" in cfops
        assert "5102" in cfops
        assert "6102" in cfops
        assert len(cfops) == len(set(cfops))

    def test_arquivo_ausente_levanta(self, tmp_path: Path) -> None:
        p = tmp_path / "nonexistent.json"
        with pytest.raises(FileNotFoundError, match="358edc8b7d5d81cfb33ce023d4cee84f"):
            cr.load_fiscal_codes(p)

    def test_valor_nao_string_levanta(self, tmp_path: Path) -> None:
        p = tmp_path / "bad.json"
        p.write_text(json.dumps({"uf": ["RO", 123]}), encoding="utf-8")
        cr.reload_fiscal_codes()
        with pytest.raises(ValueError, match="string"):
            cr.load_fiscal_codes(p)


# =====================================================================
# build_enum_map / build_categorical_map
# =====================================================================


class TestBuildEnumMap:
    def test_enum_map_contem_colunas_conhecidas(self, tmp_codes_json: Path) -> None:
        em = cr.build_enum_map(tmp_codes_json)
        assert "cfop" in em
        assert "uf" in em
        assert "cst_icms" in em
        assert isinstance(em["cfop"], pl.Enum)

    def test_enum_uf_tem_valores_corretos(self, tmp_codes_json: Path) -> None:
        em = cr.build_enum_map(tmp_codes_json)
        categorias = list(em["uf"].categories)
        assert categorias == ["RO", "SP", "MG", "RJ", "EX"]

    def test_chave_ausente_no_json_levanta(self, tmp_path: Path) -> None:
        # JSON sem cst_icms_completo â†’ erro ao construir
        bad = tmp_path / "incomplete.json"
        bad.write_text(json.dumps({"uf": ["RO"]}), encoding="utf-8")
        cr.reload_fiscal_codes()
        with pytest.raises(ValueError, match="ENUM_MAP ausentes"):
            cr.build_enum_map(bad)


class TestBuildCategoricalMap:
    def test_contem_ncm_cest_unid(self) -> None:
        cm = cr.build_categorical_map()
        assert "ncm" in cm
        assert "cest" in cm
        assert "unid" in cm

    def test_invariantes_nao_estao(self) -> None:
        cm = cr.build_categorical_map()
        assert "id_agrupado" not in cm
        assert "q_conv" not in cm


# =====================================================================
# scan_parquet_typed â€” comportamento principal
# =====================================================================


class TestScanParquetTyped:
    def test_aplica_enum_em_cfop(
        self, sample_parquet: Path, tmp_codes_json: Path
    ) -> None:
        lf = cr.scan_parquet_typed(sample_parquet, codes_path=tmp_codes_json)
        schema = lf.collect_schema()
        assert isinstance(schema["cfop"], pl.Enum)
        assert isinstance(schema["uf"], pl.Enum)
        assert isinstance(schema["cst_icms"], pl.Enum)
        assert isinstance(schema["modBC"], pl.Enum)
        assert isinstance(schema["modFrete"], pl.Enum)
        assert isinstance(schema["tPag"], pl.Enum)

    def test_aplica_boolean_em_campos_classicos(
        self, sample_parquet: Path, tmp_codes_json: Path
    ) -> None:
        lf = cr.scan_parquet_typed(sample_parquet, codes_path=tmp_codes_json)
        schema = lf.collect_schema()
        assert schema["indFinal"] == pl.Boolean
        assert schema["indEscala"] == pl.Boolean

        df = lf.select("indFinal", "indEscala").collect()
        assert df["indFinal"].to_list() == [True, False, True, None, False]
        assert df["indEscala"].to_list() == [True, False, None, True, False]

    def test_ind_mov_invertido_permanece_enum(
        self, sample_parquet: Path, tmp_codes_json: Path
    ) -> None:
        lf = cr.scan_parquet_typed(sample_parquet, codes_path=tmp_codes_json)
        schema = lf.collect_schema()
        assert isinstance(schema["IND_MOV"], pl.Enum)

    def test_aplica_categorical_em_ncm(
        self, sample_parquet: Path, tmp_codes_json: Path
    ) -> None:
        lf = cr.scan_parquet_typed(sample_parquet, codes_path=tmp_codes_json)
        schema = lf.collect_schema()
        assert isinstance(schema["ncm"], pl.Categorical)
        assert isinstance(schema["cest"], pl.Categorical)
        assert isinstance(schema["unid"], pl.Categorical)

    def test_invariantes_permanecem_intocadas(
        self, sample_parquet: Path, tmp_codes_json: Path
    ) -> None:
        lf = cr.scan_parquet_typed(sample_parquet, codes_path=tmp_codes_json)
        schema = lf.collect_schema()
        for inv in cr.INVARIANT_BLOCKLIST:
            if inv in schema.names():
                dtype = schema[inv]
                assert not isinstance(dtype, pl.Categorical), (
                    f"Invariante {inv} foi categorizada â€” defesa em profundidade falhou"
                )
                assert not isinstance(dtype, pl.Enum), (
                    f"Invariante {inv} virou Enum â€” defesa em profundidade falhou"
                )

    def test_valores_corretos_apos_cast(
        self, sample_parquet: Path, tmp_codes_json: Path
    ) -> None:
        lf = cr.scan_parquet_typed(sample_parquet, codes_path=tmp_codes_json)
        df = lf.collect()
        cfops = sorted(set(df["cfop"].cast(pl.String).to_list()))
        assert cfops == ["1102", "5102", "5405", "5949"]

    def test_filtro_funciona_em_enum(
        self, sample_parquet: Path, tmp_codes_json: Path
    ) -> None:
        lf = cr.scan_parquet_typed(sample_parquet, codes_path=tmp_codes_json)
        # Filtrar por valor textual; Polars converte automaticamente
        result = lf.filter(pl.col("cfop") == "5102").collect()
        assert result.height == 2

    def test_idempotente(
        self, sample_parquet: Path, tmp_codes_json: Path
    ) -> None:
        # Tipa, escreve, re-tipa: schemas devem ser idÃªnticos
        lf1 = cr.scan_parquet_typed(sample_parquet, codes_path=tmp_codes_json)
        df1 = lf1.collect()
        # Re-cast em DataFrame jÃ¡ tipado
        df2 = cr.cast_dataframe_typed(df1, codes_path=tmp_codes_json)
        assert df1.schema == df2.schema

    def test_parquet_sem_colunas_alvo(
        self, tmp_path: Path, tmp_codes_json: Path
    ) -> None:
        # Parquet apenas com colunas neutras
        df = pl.DataFrame({"qualquer": [1, 2, 3], "outro": ["a", "b", "c"]})
        p = tmp_path / "neutral.parquet"
        df.write_parquet(p)
        lf = cr.scan_parquet_typed(p, codes_path=tmp_codes_json)
        schema = lf.collect_schema()
        assert schema["qualquer"] == pl.Int64
        assert schema["outro"] == pl.String

    def test_valor_fora_dominio_levanta_ao_collect(
        self, tmp_path: Path, tmp_codes_json: Path
    ) -> None:
        # CFOP "9999" nÃ£o existe no JSON minimal
        df = pl.DataFrame({"cfop": ["9999"]})
        p = tmp_path / "bad_cfop.parquet"
        df.write_parquet(p)
        lf = cr.scan_parquet_typed(p, codes_path=tmp_codes_json)
        # Erro acontece no collect (Polars 1.x usa lazy)
        with pytest.raises(pl.exceptions.InvalidOperationError):
            lf.collect()

    def test_extra_enum_map_aplicado(
        self, tmp_path: Path, tmp_codes_json: Path
    ) -> None:
        # Coluna ad-hoc com Enum customizado
        df = pl.DataFrame({"meu_codigo": ["A", "B", "A"]})
        p = tmp_path / "adhoc.parquet"
        df.write_parquet(p)
        lf = cr.scan_parquet_typed(
            p,
            codes_path=tmp_codes_json,
            extra_enum_map={"meu_codigo": pl.Enum(["A", "B", "C"])},
        )
        schema = lf.collect_schema()
        assert isinstance(schema["meu_codigo"], pl.Enum)

    def test_blocklist_extra_respeitado(
        self, sample_parquet: Path, tmp_codes_json: Path
    ) -> None:
        # Adicionar cfop ao blocklist deve impedir cast
        lf = cr.scan_parquet_typed(
            sample_parquet,
            codes_path=tmp_codes_json,
            blocklist=["cfop"],
        )
        schema = lf.collect_schema()
        assert schema["cfop"] == pl.String  # sem cast


# =====================================================================
# cast_dataframe_typed
# =====================================================================


class TestCastDataframeTyped:
    def test_cast_eager_funciona(
        self, sample_parquet: Path, tmp_codes_json: Path
    ) -> None:
        df = pl.read_parquet(sample_parquet)
        df_typed = cr.cast_dataframe_typed(df, codes_path=tmp_codes_json)
        assert isinstance(df_typed.schema["cfop"], pl.Enum)
        assert isinstance(df_typed.schema["ncm"], pl.Categorical)

    def test_invariantes_preservadas_eager(
        self, sample_parquet: Path, tmp_codes_json: Path
    ) -> None:
        df = pl.read_parquet(sample_parquet)
        df_typed = cr.cast_dataframe_typed(df, codes_path=tmp_codes_json)
        for inv in cr.INVARIANT_BLOCKLIST:
            if inv in df_typed.columns:
                dtype = df_typed.schema[inv]
                assert not isinstance(dtype, pl.Categorical | pl.Enum)

    def test_dataframe_sem_alvos_retorna_inalterado(
        self, tmp_codes_json: Path
    ) -> None:
        df = pl.DataFrame({"qualquer": [1, 2, 3]})
        df_typed = cr.cast_dataframe_typed(df, codes_path=tmp_codes_json)
        assert df.schema == df_typed.schema


# =====================================================================
# ValidaÃ§Ãµes para differential_harness
# =====================================================================


class TestValidations:
    def test_validate_schema_post_cast_ok(
        self, sample_parquet: Path, tmp_codes_json: Path
    ) -> None:
        lf = cr.scan_parquet_typed(sample_parquet, codes_path=tmp_codes_json)
        diffs = cr.validate_schema_post_cast(lf, codes_path=tmp_codes_json)
        assert diffs == {}

    def test_validate_detecta_string_quando_deveria_enum(
        self, sample_parquet: Path, tmp_codes_json: Path
    ) -> None:
        # LazyFrame sem aplicar typing â€” schema tem strings em colunas
        # que deveriam ser Enum
        lf = pl.scan_parquet(sample_parquet)
        diffs = cr.validate_schema_post_cast(lf, codes_path=tmp_codes_json)
        assert "cfop" in diffs
        assert "uf" in diffs
        assert "esperado pl.Enum" in diffs["cfop"]

    def test_get_invariant_dtypes_retorna_strings(
        self, sample_parquet: Path, tmp_codes_json: Path
    ) -> None:
        lf = cr.scan_parquet_typed(sample_parquet, codes_path=tmp_codes_json)
        invs = cr.get_invariant_dtypes(lf)
        assert "id_agrupado" in invs
        assert "q_conv" in invs
        # Devem ser tipos nÃ£o-categÃ³ricos
        for col, dtype_str in invs.items():
            assert "Categorical" not in dtype_str
            assert "Enum" not in dtype_str

    def test_assert_no_invariant_categorized_ok(
        self, sample_parquet: Path, tmp_codes_json: Path
    ) -> None:
        lf = cr.scan_parquet_typed(sample_parquet, codes_path=tmp_codes_json)
        # NÃ£o deve levantar
        cr.assert_no_invariant_categorized(lf)

    def test_assert_detecta_violacao(self, tmp_path: Path) -> None:
        # Construir manualmente um DataFrame com id_agrupado categorizado
        df = pl.DataFrame({
            "id_agrupado": ["a", "b", "c"],
            "outro": [1, 2, 3],
        }).with_columns(pl.col("id_agrupado").cast(pl.Categorical))
        with pytest.raises(AssertionError, match="Polars #24034"):
            cr.assert_no_invariant_categorized(df.lazy())


# =====================================================================
# Defesa em profundidade
# =====================================================================


class TestDefesaEmProfundidade:
    def test_invariante_em_extra_enum_map_e_ignorada(
        self, sample_parquet: Path, tmp_codes_json: Path, caplog
    ) -> None:
        """Mesmo se alguÃ©m tenta adicionar invariante ao mapa, Ã© bloqueado."""
        import logging
        caplog.set_level(logging.WARNING)
        lf = cr.scan_parquet_typed(
            sample_parquet,
            codes_path=tmp_codes_json,
            extra_enum_map={
                "id_agrupado": pl.Enum(["a", "b", "c", "d", "e"]),
            },
        )
        schema = lf.collect_schema()
        # id_agrupado deve continuar String, nÃ£o Enum
        assert not isinstance(schema["id_agrupado"], pl.Enum)
        # Deve ter logado warning
        assert any(
            "INVARIANT_BLOCKLIST" in record.message
            for record in caplog.records
        )

    def test_invariante_em_extra_categorical_e_ignorada(
        self, sample_parquet: Path, tmp_codes_json: Path
    ) -> None:
        lf = cr.scan_parquet_typed(
            sample_parquet,
            codes_path=tmp_codes_json,
            extra_categorical_columns={"q_conv"},
        )
        schema = lf.collect_schema()
        # q_conv deve permanecer Float64
        assert schema["q_conv"] == pl.Float64
