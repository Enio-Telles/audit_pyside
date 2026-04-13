"""
Teste para validar cálculo de estoque inicial e final na tabela anual

Valida que:
1. Estoque inicial (Tipo_operacao = 0) é capturado independente da data
2. Estoque final (Tipo_operacao = 3) é capturado independente da data
3. A soma na tabela anual reflete corretamente os valores
"""

import polars as pl
from pathlib import Path
import sys
from datetime import date
from src.utilitarios.project_paths import PROJECT_ROOT

# Adicionar src ao path para imports
ROOT_DIR = PROJECT_ROOT
sys.path.insert(0, str(ROOT_DIR / "src"))

from transformacao.calculos_anuais_pkg.calculos_anuais import calcular_aba_anual_dataframe


def test_estoque_inicial_final_qualquer_data():
    """Testa que estoque inicial e final são capturados em qualquer data do ano."""
    
    # Criar dados simulados de mov_estoque
    dados = {
        "id_agrupado": ["id_agg_1"] * 6,
        "ano": [2021] * 6,
        "Tipo_operacao": [
            "0 - ESTOQUE INICIAL",  # 15/06 - fora de 01/01
            "1 - ENTRADA",
            "2 - SAIDAS",
            "3 - ESTOQUE FINAL",    # 15/06 - fora de 31/12
            "3 - ESTOQUE FINAL",    # 30/06 - outro estoque final
            "3 - ESTOQUE FINAL",    # 31/12 - estoque final tradicional
        ],
        "Dt_doc": [
            date(2021, 6, 15),
            date(2021, 6, 20),
            date(2021, 6, 25),
            date(2021, 6, 15),
            date(2021, 6, 30),
            date(2021, 12, 31),
        ],
        "Dt_e_s": [
            date(2021, 6, 15),
            date(2021, 6, 20),
            date(2021, 6, 25),
            date(2021, 6, 15),
            date(2021, 6, 30),
            date(2021, 12, 31),
        ],
        "q_conv": [100.0, 50.0, 30.0, 0.0, 0.0, 0.0],  # ESTOQUE FINAL tem q_conv = 0
        "__qtd_decl_final_audit__": [0.0, 0.0, 0.0, 50.0, 30.0, 20.0],  # Valores declarados
        "entr_desac_anual": [0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
        "saldo_estoque_anual": [100.0, 150.0, 120.0, 120.0, 120.0, 120.0],
        "ordem_operacoes": [1, 2, 3, 4, 5, 6],
        "descr_padrao": ["Produto A"] * 6,
        "unid_ref": ["UN"] * 6,
        "co_sefin_agr": ["COD123"] * 6,
        "it_pc_interna": [18.0] * 6,
        "preco_item": [10.0, 12.0, 0.0, 0.0, 0.0, 0.0],
        "Vl_item": [10.0, 12.0, 0.0, 0.0, 0.0, 0.0],
        "dev_simples": [False] * 6,
        "excluir_estoque": [False] * 6,
    }
    
    df_mov = pl.DataFrame(dados)
    
    # Calcular tabela anual
    df_anual = calcular_aba_anual_dataframe(df_mov)
    
    print("=== DADOS DE ENTRADA (mov_estoque) ===")
    print(df_mov.select(["Tipo_operacao", "Dt_doc", "q_conv", "__qtd_decl_final_audit__"]))
    
    print("\n=== RESULTADO (tabela anual) ===")
    print(df_anual)
    
    # Validar resultados
    assert df_anual.height == 1, "Deve gerar 1 linha na tabela anual"
    
    linha = df_anual.row(0)
    cols = dict(zip(df_anual.columns, linha))
    
    estoque_inicial = cols["estoque_inicial"]
    entradas = cols["entradas"]
    saidas = cols["saidas"]
    estoque_final = cols["estoque_final"]
    saldo_final = cols["saldo_final"]
    saidas_calculadas = cols["saidas_calculadas"]
    
    print(f"\n=== VALIDAÇÃO ===")
    print(f"estoque_inicial: {estoque_inicial} (esperado: 100.0)")
    print(f"entradas: {entradas} (esperado: 50.0)")
    print(f"saidas: {saidas} (esperado: 30.0)")
    print(f"estoque_final: {estoque_final} (esperado: 100.0 = 50+30+20)")
    print(f"saldo_final: {saldo_final} (esperado: 120.0)")
    print(f"saidas_calculadas: {saidas_calculadas} (esperado: 50.0)")
    
    # Validações principais
    assert estoque_inicial == 100.0, f"Estoque inicial deve ser 100.0, veio {estoque_inicial}"
    assert entradas == 50.0, f"Entradas devem ser 50.0, veio {entradas}"
    assert saidas == 30.0, f"Saídas devem ser 30.0, veio {saidas}"
    
    # Estoque final deve ser soma de TODOS os estoques finais do ano (50+30+20=100)
    assert estoque_final == 100.0, f"Estoque final deve ser 100.0 (50+30+20), veio {estoque_final}"
    
    # saidas_calculadas = estoque_inicial + entradas + entradas_desacob - estoque_final
    # saidas_calculadas = 100 + 50 + 0 - 100 = 50
    assert saidas_calculadas == 50.0, f"Saídas calculadas devem ser 50.0, veio {saidas_calculadas}"
    
    print("\n✅ Todos os testes passaram!")
    return True


def test_estoque_inicial_fora_01_01():
    """Testa especificamente estoque inicial em data diferente de 01/01."""
    
    dados = {
        "id_agrupado": ["id_agg_2"],
        "ano": [2021],
        "Tipo_operacao": ["0 - ESTOQUE INICIAL"],
        "Dt_doc": [date(2021, 6, 15)],  # 15/06, não 01/01
        "Dt_e_s": [date(2021, 6, 15)],
        "q_conv": [250.0],  # Deve ser capturado
        "__qtd_decl_final_audit__": [0.0],
        "entr_desac_anual": [0.0],
        "saldo_estoque_anual": [250.0],
        "ordem_operacoes": [1],
        "descr_padrao": ["Produto B"],
        "unid_ref": ["UN"],
        "co_sefin_agr": ["COD456"],
        "it_pc_interna": [18.0],
        "preco_item": [15.0],
        "Vl_item": [15.0],
        "dev_simples": [False],
        "excluir_estoque": [False],
    }
    
    df_mov = pl.DataFrame(dados)
    df_anual = calcular_aba_anual_dataframe(df_mov)
    
    estoque_inicial = df_anual["estoque_inicial"][0]
    
    print(f"\n=== TESTE ESTOQUE INICIAL FORA DE 01/01 ===")
    print(f"estoque_inicial: {estoque_inicial} (esperado: 250.0)")
    
    assert estoque_inicial == 250.0, f"Estoque inicial em 15/06 deve ser capturado, veio {estoque_inicial}"
    print("✅ Teste estoque inicial fora de 01/01 passou!")
    return True


def test_estoque_final_fora_31_12():
    """Testa especificamente estoque final em data diferente de 31/12."""
    
    dados = {
        "id_agrupado": ["id_agg_3"],
        "ano": [2021],
        "Tipo_operacao": ["3 - ESTOQUE FINAL"],
        "Dt_doc": [date(2021, 6, 30)],  # 30/06, não 31/12
        "Dt_e_s": [date(2021, 6, 30)],
        "q_conv": [0.0],
        "__qtd_decl_final_audit__": [180.0],  # Deve ser capturado
        "entr_desac_anual": [0.0],
        "saldo_estoque_anual": [180.0],
        "ordem_operacoes": [1],
        "descr_padrao": ["Produto C"],
        "unid_ref": ["UN"],
        "co_sefin_agr": ["COD789"],
        "it_pc_interna": [18.0],
        "preco_item": [0.0],
        "Vl_item": [0.0],
        "dev_simples": [False],
        "excluir_estoque": [False],
    }
    
    df_mov = pl.DataFrame(dados)
    df_anual = calcular_aba_anual_dataframe(df_mov)
    
    estoque_final = df_anual["estoque_final"][0]
    
    print(f"\n=== TESTE ESTOQUE FINAL FORA DE 31/12 ===")
    print(f"estoque_final: {estoque_final} (esperado: 180.0)")
    
    assert estoque_final == 180.0, f"Estoque final em 30/06 deve ser capturado, veio {estoque_final}"
    print("✅ Teste estoque final fora de 31/12 passou!")
    return True


if __name__ == "__main__":
    print("=" * 60)
    print("TESTE: Cálculo de Estoque Inicial e Final na Tabela Anual")
    print("=" * 60)
    
    try:
        test_estoque_inicial_final_qualquer_data()
        test_estoque_inicial_fora_01_01()
        test_estoque_final_fora_31_12()
        
        print("\n" + "=" * 60)
        print("✅ TODOS OS TESTES PASSARAM!")
        print("=" * 60)
        
    except AssertionError as e:
        print(f"\n❌ TESTE FALHOU: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ ERRO: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
