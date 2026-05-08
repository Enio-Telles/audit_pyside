# 00 — PLANO MESTRE: DuckDB para Parquets grandes e GUI paginada

## Problema
Arquivos Parquet acima de 2 GB causam travamento da interface gráfica ao serem carregados integralmente em memória via `pl.read_parquet()` dentro de workers Qt. O núcleo fiscal em Polars continua correto e não precisa ser substituído. A camada de consulta e renderização da GUI precisa de um backend que suporte lazy evaluation e pushdown de predicados para arquivos grandes sem coletar o DataFrame completo.

## Solução
Introduzir DuckDB como backend de consulta **somente** para a GUI (renderização, paginação, filtros e exportação). O núcleo fiscal em Polars permanece inalterado. Para datasets acima de 2 GB, adotar particionamento físico por ano e por bucket de `id_agrupado`. Oferecer um MCP local para diagnóstico e exploração assistida por agentes.

### Regra fundamental
> **DuckDB é a janela; Polars é o motor.**

Toda decisão que toque em cálculos fiscais (aba_periodos, mov_estoque, calculos_mensais, calculos_anuais, calculos_periodo, resumo_global) continua em Polars. DuckDB entra apenas na camada de consulta/renderização da GUI e nas exportações.

## Visão Geral das Ondas
- **Onda 0:** Higiene estrutural e baseline.
- **Onda 1:** Backend DuckDB e roteamento híbrido.
- **Onda 2:** Paginação das abas principais da GUI.
- **Onda 3:** Agregação escalável e exportação streaming.
- **Onda 4:** Particionamento e MCP.
