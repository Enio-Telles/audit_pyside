# Ordem de execução sugerida

## 1. Resolução de parâmetros e contribuinte
1. `consultas_sql/00_base/00_parametros_normalizados.sql`
2. `consultas_sql/00_base/01_lookup_contribuinte.sql`
3. `consultas_sql/00_base/02_base_contribuinte.sql`

## 2. Cadastro e histórico
4. `consultas_sql/10_cadastro/*`

## 3. Relações societárias
5. `consultas_sql/20_societario/*`

## 4. Documentos fiscais
6. `consultas_sql/30_documentos_fiscais/*`

## 5. Arrecadação e regularidade
7. `consultas_sql/40_arrecadacao_regularidade/*`

## 6. Fiscalização e conformidade
8. `consultas_sql/50_fiscalizacao_conformidade/*`

## 7. Consolidação
9. Persistir cada consulta em parquet
10. Usar Polars para construir `gold_dossie_resumo`, `gold_dossie_timeline` e visões temáticas

## Observação
As consultas de `90_orquestracao` são exemplos de checagem e consolidação lógica. A materialização final recomendada é em Polars, não em uma SQL monolítica.
