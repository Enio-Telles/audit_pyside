# 00 — Plano Mestre DuckDB + GUI Paginada

## Objetivo
Estabilizar a visualizacao de grandes datasets Parquet (> 512 MB) na GUI PySide6 utilizando DuckDB como motor de consulta (query engine) e implementando paginacao obrigatoria.

## Regra Fundamental
"DuckDB e a janela; Polars e o motor."

- DuckDB: consulta, projecao, filtros de interface e exportacao.
- Polars: calculos fiscais, transformacoes de dados e core logic.

## Regras Inviolaveis
1. Nao substituir Polars no nucleo fiscal.
2. Preservar as 5 invariantes fiscais (`id_agrupado`, `id_agregado`, `__qtd_decl_final_audit__`, `q_conv`, `q_conv_fisica`).
3. Nunca carregar arquivos grandes integralmente em memoria na GUI.
