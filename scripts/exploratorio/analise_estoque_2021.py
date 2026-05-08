"""
Script para diagnosticar cálculo do estoque inicial e final na tabela anual
Foco: id_agrupado_23, ano 2021
"""

import polars as pl
from src.utilitarios.project_paths import PROJECT_ROOT

ROOT_DIR = PROJECT_ROOT
CNPJ_ROOT = ROOT_DIR / "dados" / "CNPJ"

# Localizar CNPJ dos dados de exemplo
cnpjs_disponiveis = list(CNPJ_ROOT.glob("*"))
print(f"CNPJs disponíveis: {[c.name for c in cnpjs_disponiveis]}")

if not cnpjs_disponiveis:
    print("Nenhum CNPJ encontrado!")
    exit(1)

cnpj = cnpjs_disponiveis[0].name
pasta_analises = CNPJ_ROOT / cnpj / "analises" / "produtos"

print(f"\n=== Analisando CNPJ: {cnpj} ===\n")

# Carregar mov_estoque
arq_mov_estoque = pasta_analises / f"mov_estoque_{cnpj}.parquet"
if not arq_mov_estoque.exists():
    print(f"Arquivo não encontrado: {arq_mov_estoque}")
    exit(1)

df_mov = pl.read_parquet(arq_mov_estoque)

# Filtrar id_agrupado_23, ano 2021
df_mov_2021 = (
    df_mov.with_columns(
        pl.coalesce(
            [
                pl.col("Dt_e_s").cast(pl.Date, strict=False),
                pl.col("Dt_doc").cast(pl.Date, strict=False),
            ]
        )
        .cast(pl.Date)
        .alias("__data__")
    )
    .with_columns(pl.col("__data__").dt.year().alias("__ano__"))
    .filter((pl.col("id_agrupado") == "id_agrupado_23") & (pl.col("__ano__") == 2021))
    .sort("__data__", "ordem_operacoes")
)

print("=== MOVIMENTO ESTOQUE - id_agrupado_23, 2021 ===")
print(f"Total de linhas: {df_mov_2021.height}\n")

cols_exibir = [
    "ordem_operacoes",
    "fonte",
    "Tipo_operacao",
    "Dt_doc",
    "Dt_e_s",
    "Qtd",
    "q_conv",
    "__qtd_decl_final_audit__",
    "saldo_estoque_anual",
    "entr_desac_anual",
    "descr_padrao",
    "unid_ref",
]
cols_exibir = [c for c in cols_exibir if c in df_mov_2021.columns]

with pl.Config(tbl_rows=100, tbl_cols=30):
    print(df_mov_2021.select(cols_exibir))

# Resumo por Tipo_operacao
print("\n=== RESUMO POR TIPO_OPERACAO ===")
resumo = (
    df_mov_2021.group_by("Tipo_operacao")
    .agg(
        pl.col("q_conv").sum().alias("soma_q_conv"),
        pl.col("__qtd_decl_final_audit__").sum().alias("soma_qtd_decl_final"),
        pl.col("saldo_estoque_anual").last().alias("ultimo_saldo"),
        pl.len().alias("qtd_linhas"),
    )
    .sort("Tipo_operacao")
)
print(resumo)

# Carregar tabela anual
arq_aba_anual = pasta_analises / f"aba_anual_{cnpj}.parquet"
if arq_aba_anual.exists():
    df_anual = pl.read_parquet(arq_aba_anual)

    # Filtrar id_agrupado_23, 2021
    df_anual_2021 = df_anual.filter(
        (pl.col("id_agregado") == "id_agrupado_23") & (pl.col("ano") == 2021)
    )

    print("\n=== TABELA ANUAL - id_agrupado_23, 2021 ===")
    with pl.Config(tbl_rows=20, tbl_cols=30):
        print(df_anual_2021)

    # Verificar cálculos
    print("\n=== VERIFICAÇÃO DE CÁLCULOS ===")
    if not df_anual_2021.is_empty():
        linha = df_anual_2021.row(0)
        cols_dict = dict(zip(df_anual_2021.columns, linha))

        estoque_inicial = cols_dict.get("estoque_inicial", 0)
        entradas = cols_dict.get("entradas", 0)
        saidas = cols_dict.get("saidas", 0)
        estoque_final = cols_dict.get("estoque_final", 0)
        entradas_desacob = cols_dict.get("entradas_desacob", 0)
        saldo_final = cols_dict.get("saldo_final", 0)
        saidas_calculadas = cols_dict.get("saidas_calculadas", 0)

        print(f"estoque_inicial: {estoque_inicial}")
        print(f"entradas: {entradas}")
        print(f"saidas: {saidas}")
        print(f"estoque_final: {estoque_final}")
        print(f"entradas_desacob: {entradas_desacob}")
        print(f"saldo_final: {saldo_final}")
        print(f"saidas_calculadas: {saidas_calculadas}")

        # Fórmula: saidas_calculadas = estoque_inicial + entradas + entradas_desacob - estoque_final
        saidas_calc_esperado = (
            estoque_inicial + entradas + entradas_desacob - estoque_final
        )
        print(f"\nsaidas_calculadas (esperado): {saidas_calc_esperado}")
        print(f"saidas_calculadas (tabela): {saidas_calculadas}")
        print(f"Diferença: {abs(saidas_calculadas - saidas_calc_esperado)}")

        # Verificar equação de saldo
        print("\n=== EQUAÇÃO DE SALDO ===")
        print(
            f"estoque_inicial + entradas - saidas + entradas_desacob = {estoque_inicial + entradas - saidas + entradas_desacob}"
        )
        print(f"estoque_final = {estoque_final}")
        print(
            f"Diferença (deve ser 0): {estoque_inicial + entradas - saidas + entradas_desacob - estoque_final}"
        )
else:
    print("\nArquivo da tabela anual não encontrado!")
