"""Testes unitarios para main_window_helpers (sem dependencia de Qt)."""

from __future__ import annotations

import polars as pl
import pytest

from interface_grafica.ui.main_window_helpers import (
    aba_anual_background,
    aba_anual_foreground,
    aba_mensal_background,
    aba_mensal_foreground,
    estilo_botao_destacar,
    filtrar_intervalo_numerico,
    filtrar_texto_em_colunas,
    formatar_resumo_filtros,
    mov_estoque_background,
    mov_estoque_foreground,
    parse_numero_filtro,
    split_terms,
)


# ---------------------------------------------------------------------------
# estilo_botao_destacar
# ---------------------------------------------------------------------------


class TestEstiloBotaoDestacar:
    def test_retorna_string(self):
        resultado = estilo_botao_destacar()
        assert isinstance(resultado, str)

    def test_contem_qpushbutton(self):
        assert "QPushButton" in estilo_botao_destacar()

    def test_contem_cor_background(self):
        assert "#0e639c" in estilo_botao_destacar()

    def test_contem_hover(self):
        assert ":hover" in estilo_botao_destacar()

    def test_contem_pressed(self):
        assert ":pressed" in estilo_botao_destacar()


# ---------------------------------------------------------------------------
# parse_numero_filtro
# ---------------------------------------------------------------------------


class TestParseNumeroFiltro:
    def test_inteiro(self):
        assert parse_numero_filtro("42") == 42.0

    def test_decimal_ponto(self):
        assert parse_numero_filtro("3.14") == pytest.approx(3.14)

    def test_decimal_virgula(self):
        assert parse_numero_filtro("3,14") == pytest.approx(3.14)

    def test_vazio_retorna_none(self):
        assert parse_numero_filtro("") is None

    def test_none_str_retorna_none(self):
        assert parse_numero_filtro(None) is None  # type: ignore[arg-type]

    def test_texto_invalido_retorna_none(self):
        assert parse_numero_filtro("abc") is None

    def test_negativo(self):
        assert parse_numero_filtro("-5.5") == pytest.approx(-5.5)

    def test_apenas_espaco(self):
        assert parse_numero_filtro("   ") is None


# ---------------------------------------------------------------------------
# filtrar_texto_em_colunas
# ---------------------------------------------------------------------------


class TestFiltrarTextoEmColunas:
    def _df(self) -> pl.DataFrame:
        return pl.DataFrame(
            {
                "nome": ["alpha", "beta", "gamma"],
                "cod": ["A1", "B2", "C3"],
                "valor": [1.0, 2.0, 3.0],
            }
        )

    def test_filtro_encontra_linha(self):
        df = self._df()
        resultado = filtrar_texto_em_colunas(df, "alpha")
        assert len(resultado) == 1
        assert resultado["nome"][0] == "alpha"

    def test_filtro_case_insensitive(self):
        df = self._df()
        resultado = filtrar_texto_em_colunas(df, "ALPHA")
        assert len(resultado) == 1

    def test_filtro_parcial(self):
        df = self._df()
        resultado = filtrar_texto_em_colunas(df, "a")
        # alpha e gamma contem 'a'
        assert len(resultado) >= 1

    def test_texto_vazio_retorna_tudo(self):
        df = self._df()
        resultado = filtrar_texto_em_colunas(df, "")
        assert len(resultado) == 3

    def test_texto_none_retorna_tudo(self):
        df = self._df()
        resultado = filtrar_texto_em_colunas(df, None)  # type: ignore[arg-type]
        assert len(resultado) == 3

    def test_df_vazio_retorna_vazio(self):
        df = pl.DataFrame({"nome": pl.Series([], dtype=pl.Utf8)})
        resultado = filtrar_texto_em_colunas(df, "x")
        assert len(resultado) == 0

    def test_sem_colunas_texto_retorna_original(self):
        df = pl.DataFrame({"valor": [1.0, 2.0]})
        resultado = filtrar_texto_em_colunas(df, "x")
        assert len(resultado) == 2

    def test_busca_em_codigo(self):
        df = self._df()
        resultado = filtrar_texto_em_colunas(df, "B2")
        assert len(resultado) == 1
        assert resultado["cod"][0] == "B2"


# ---------------------------------------------------------------------------
# filtrar_intervalo_numerico
# ---------------------------------------------------------------------------


class TestFiltrarIntervaloNumerico:
    def _df(self) -> pl.DataFrame:
        return pl.DataFrame({"valor": [1.0, 5.0, 10.0, 20.0]})

    def test_minimo(self):
        df = self._df()
        resultado = filtrar_intervalo_numerico(df, "valor", "5", "")
        assert list(resultado["valor"]) == [5.0, 10.0, 20.0]

    def test_maximo(self):
        df = self._df()
        resultado = filtrar_intervalo_numerico(df, "valor", "", "10")
        assert list(resultado["valor"]) == [1.0, 5.0, 10.0]

    def test_intervalo_completo(self):
        df = self._df()
        resultado = filtrar_intervalo_numerico(df, "valor", "5", "10")
        assert list(resultado["valor"]) == [5.0, 10.0]

    def test_sem_limites_retorna_tudo(self):
        df = self._df()
        resultado = filtrar_intervalo_numerico(df, "valor", "", "")
        assert len(resultado) == 4

    def test_coluna_inexistente_retorna_original(self):
        df = self._df()
        resultado = filtrar_intervalo_numerico(df, "nao_existe", "1", "10")
        assert len(resultado) == 4

    def test_coluna_none_retorna_original(self):
        df = self._df()
        resultado = filtrar_intervalo_numerico(df, None, "1", "10")
        assert len(resultado) == 4

    def test_virgula_como_separador_decimal(self):
        df = self._df()
        resultado = filtrar_intervalo_numerico(df, "valor", "4,5", "10,5")
        assert list(resultado["valor"]) == [5.0, 10.0]


# ---------------------------------------------------------------------------
# formatar_resumo_filtros
# ---------------------------------------------------------------------------


class TestFormatarResumoFiltros:
    def test_sem_filtros(self):
        resultado = formatar_resumo_filtros([])
        assert resultado == "Filtros ativos: nenhum"

    def test_todos_vazios(self):
        resultado = formatar_resumo_filtros([("campo", ""), ("outro", "")])
        assert resultado == "Filtros ativos: nenhum"

    def test_um_filtro(self):
        resultado = formatar_resumo_filtros([("nome", "alpha")])
        assert "nome: alpha" in resultado

    def test_varios_filtros(self):
        resultado = formatar_resumo_filtros([("nome", "x"), ("cod", "A1")])
        assert "nome: x" in resultado
        assert "cod: A1" in resultado
        assert "|" in resultado

    def test_mistura_vazios_e_preenchidos(self):
        resultado = formatar_resumo_filtros([("nome", "x"), ("cod", "")])
        assert "cod" not in resultado
        assert "nome: x" in resultado


# ---------------------------------------------------------------------------
# split_terms
# ---------------------------------------------------------------------------


class TestSplitTerms:
    def test_vazio_retorna_lista_vazia(self):
        assert split_terms("") == []

    def test_none_retorna_lista_vazia(self):
        assert split_terms(None) == []  # type: ignore[arg-type]

    def test_termo_unico(self):
        assert split_terms("buch") == ["buch"]

    def test_separador_ponto_virgula(self):
        assert split_terms("buch;18") == ["buch", "18"]

    def test_separador_virgula(self):
        assert split_terms("buch,18") == ["buch", "18"]

    def test_dois_espacos(self):
        assert split_terms("buch  18") == ["buch", "18"]

    def test_espaco_simples_divide(self):
        assert split_terms("buch 18") == ["buch", "18"]

    def test_varios_termos_com_espacos(self):
        termos = split_terms("a b c")
        assert len(termos) == 3

    def test_remove_espacos_extras(self):
        assert split_terms("  buch  ") == ["buch"]


# ---------------------------------------------------------------------------
# aba_mensal_foreground / aba_mensal_background
# ---------------------------------------------------------------------------


class TestAbaMensalForeground:
    def test_entradas_desacob_positivo_retorna_cor_destaque(self):
        row = {"entradas_desacob": 1.0, "ICMS_entr_desacob": 0.0, "mes": 1}
        assert aba_mensal_foreground(row, "x") == "#fff7ed"

    def test_icms_positivo_retorna_cor_destaque(self):
        row = {"entradas_desacob": 0.0, "ICMS_entr_desacob": 5.0, "mes": 1}
        assert aba_mensal_foreground(row, "x") == "#fff7ed"

    def test_sem_desacob_retorna_cor_padrao(self):
        row = {"entradas_desacob": 0.0, "ICMS_entr_desacob": 0.0, "mes": 1}
        assert aba_mensal_foreground(row, "x") == "#f5f5f5"

    def test_campos_faltantes_tratados_como_zero(self):
        assert aba_mensal_foreground({}, "x") == "#f5f5f5"


class TestAbaMensalBackground:
    def test_desacob_positivo_retorna_fundo_destaque(self):
        row = {"entradas_desacob": 1.0, "ICMS_entr_desacob": 0.0, "mes": 1}
        assert aba_mensal_background(row, "x") == "#5b3a06"

    def test_mes_par_retorna_cor_escura(self):
        row = {"entradas_desacob": 0.0, "ICMS_entr_desacob": 0.0, "mes": 2}
        assert aba_mensal_background(row, "x") == "#1f1f1f"

    def test_mes_impar_retorna_cor_alternada(self):
        row = {"entradas_desacob": 0.0, "ICMS_entr_desacob": 0.0, "mes": 1}
        assert aba_mensal_background(row, "x") == "#262626"


# ---------------------------------------------------------------------------
# aba_anual_foreground / aba_anual_background
# ---------------------------------------------------------------------------


class TestAbaAnualForeground:
    def test_entradas_positivas_retorna_destaque(self):
        row = {"entradas_desacob": 1.0, "saidas_desacob": 0.0, "estoque_final_desacob": 0.0}
        assert aba_anual_foreground(row, "x") == "#fff7ed"

    def test_saidas_positivas_retorna_destaque(self):
        row = {"entradas_desacob": 0.0, "saidas_desacob": 2.0, "estoque_final_desacob": 0.0}
        assert aba_anual_foreground(row, "x") == "#fff7ed"

    def test_estoque_positivo_retorna_destaque(self):
        row = {"entradas_desacob": 0.0, "saidas_desacob": 0.0, "estoque_final_desacob": 3.0}
        assert aba_anual_foreground(row, "x") == "#fff7ed"

    def test_sem_desacob_retorna_padrao(self):
        row = {"entradas_desacob": 0.0, "saidas_desacob": 0.0, "estoque_final_desacob": 0.0}
        assert aba_anual_foreground(row, "x") == "#f5f5f5"

    def test_campos_faltantes_sem_erro(self):
        assert aba_anual_foreground({}, "x") == "#f5f5f5"


class TestAbaAnualBackground:
    def test_desacob_positivo_retorna_fundo_destaque(self):
        row = {
            "entradas_desacob": 1.0,
            "saidas_desacob": 0.0,
            "estoque_final_desacob": 0.0,
            "id_agregado": "abc",
        }
        assert aba_anual_background(row, "x") == "#5b3a06"

    def test_sem_desacob_retorna_cor_baseada_em_hash(self):
        row = {
            "entradas_desacob": 0.0,
            "saidas_desacob": 0.0,
            "estoque_final_desacob": 0.0,
            "id_agregado": "x",
        }
        cor = aba_anual_background(row, "x")
        assert cor in ("#1f1f1f", "#262626")

    def test_resultado_deterministico(self):
        row = {
            "entradas_desacob": 0.0,
            "saidas_desacob": 0.0,
            "estoque_final_desacob": 0.0,
            "id_agregado": "produto_42",
        }
        assert aba_anual_background(row, "x") == aba_anual_background(row, "x")


# ---------------------------------------------------------------------------
# mov_estoque_foreground / mov_estoque_background
# ---------------------------------------------------------------------------


class TestMovEstoqueForeground:
    def test_entr_desac_anual_retorna_laranja(self):
        row = {"entr_desac_anual": 1.0, "Tipo_operacao": "ENTRADA", "excluir_estoque": ""}
        assert mov_estoque_foreground(row, "x") == "#fdba74"

    def test_excluir_sim_retorna_cinza(self):
        row = {"entr_desac_anual": 0.0, "Tipo_operacao": "ENTRADA", "excluir_estoque": "SIM"}
        assert mov_estoque_foreground(row, "x") == "#94a3b8"

    def test_estoque_final_retorna_amarelo(self):
        row = {"entr_desac_anual": 0.0, "Tipo_operacao": "ESTOQUE FINAL", "excluir_estoque": ""}
        assert mov_estoque_foreground(row, "x") == "#fde68a"

    def test_estoque_inicial_retorna_azul_claro(self):
        row = {"entr_desac_anual": 0.0, "Tipo_operacao": "ESTOQUE INICIAL", "excluir_estoque": ""}
        assert mov_estoque_foreground(row, "x") == "#bfdbfe"

    def test_entrada_retorna_azul(self):
        row = {"entr_desac_anual": 0.0, "Tipo_operacao": "ENTRADA", "excluir_estoque": ""}
        assert mov_estoque_foreground(row, "x") == "#93c5fd"

    def test_saida_retorna_vermelho(self):
        row = {"entr_desac_anual": 0.0, "Tipo_operacao": "SAIDA", "excluir_estoque": ""}
        assert mov_estoque_foreground(row, "x") == "#fca5a5"

    def test_desconhecido_retorna_none(self):
        row = {"entr_desac_anual": 0.0, "Tipo_operacao": "OUTRO", "excluir_estoque": ""}
        assert mov_estoque_foreground(row, "x") is None

    def test_campos_faltantes_retorna_none(self):
        assert mov_estoque_foreground({}, "x") is None


class TestMovEstoqueBackground:
    def test_entr_desac_anual_retorna_fundo_destaque(self):
        row = {"entr_desac_anual": 1.0, "Tipo_operacao": "", "excluir_estoque": "", "mov_rep": ""}
        assert mov_estoque_background(row, "x") == "#431407"

    def test_excluir_sim_retorna_fundo_cinza(self):
        row = {"entr_desac_anual": 0.0, "Tipo_operacao": "", "excluir_estoque": "SIM", "mov_rep": ""}
        assert mov_estoque_background(row, "x") == "#1e293b"

    def test_mov_rep_sim_retorna_fundo_escuro(self):
        row = {"entr_desac_anual": 0.0, "Tipo_operacao": "", "excluir_estoque": "", "mov_rep": "S"}
        assert mov_estoque_background(row, "x") == "#111827"

    def test_estoque_final_retorna_fundo_amarelo(self):
        row = {"entr_desac_anual": 0.0, "Tipo_operacao": "ESTOQUE FINAL", "excluir_estoque": "", "mov_rep": ""}
        assert mov_estoque_background(row, "x") == "#3f2f10"

    def test_estoque_inicial_retorna_fundo_azul_escuro(self):
        row = {"entr_desac_anual": 0.0, "Tipo_operacao": "ESTOQUE INICIAL", "excluir_estoque": "", "mov_rep": ""}
        assert mov_estoque_background(row, "x") == "#0f172a"

    def test_entrada_retorna_fundo_azul(self):
        row = {"entr_desac_anual": 0.0, "Tipo_operacao": "ENTRADA", "excluir_estoque": "", "mov_rep": ""}
        assert mov_estoque_background(row, "x") == "#10213f"

    def test_saida_retorna_fundo_vermelho(self):
        row = {"entr_desac_anual": 0.0, "Tipo_operacao": "SAIDA", "excluir_estoque": "", "mov_rep": ""}
        assert mov_estoque_background(row, "x") == "#3b1212"

    def test_desconhecido_retorna_none(self):
        row = {"entr_desac_anual": 0.0, "Tipo_operacao": "OUTRO", "excluir_estoque": "", "mov_rep": ""}
        assert mov_estoque_background(row, "x") is None

    def test_campos_faltantes_retorna_none(self):
        assert mov_estoque_background({}, "x") is None
