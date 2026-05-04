# Plano validado — DuckDB para Parquets grandes, GUI paginada e MCP DuckDB

## Problema

Arquivos Parquet acima de 2 GB causam travamento da interface grafica ao serem
carregados integralmente em memoria via `pl.read_parquet()` dentro de workers Qt.
O nucleo fiscal em Polars continua correto e nao precisa ser substituido. A camada
de consulta e renderizacao da GUI precisa de um backend que suporte lazy evaluation
e pushdown de predicados para arquivos grandes sem coletar o DataFrame completo.

## Solucao

Introduzir DuckDB como backend de consulta **somente** para a GUI (renderizacao,
paginacao, filtros e exportacao). O nucleo fiscal em Polars permanece inalterado.
Para datasets acima de 2 GB, adotar particionamento fisico por ano e por bucket de
`id_agrupado`. Oferecer um MCP local para diagnostico e exploracao assistida por
agentes.

### Regra fundamental

```
DuckDB e a janela; Polars e o motor.
```

Toda decisao que toque em calculos fiscais (aba_periodos, mov_estoque,
calculos_mensais, calculos_anuais, calculos_periodo, resumo_global) continua
em Polars. DuckDB entra apenas na camada de consulta/renderizacao da GUI e nas
exportacoes.

---

## Fases e PRs

### Fase 0 — Guard rails anti-travamento

| PR | Branch | Objetivo |
|----|--------|----------|
| 0.1 | `perf/gui-large-parquet-instrumentation` | Instrumentar leitura de Parquet na GUI: `log_parquet_open`, `LARGE_PARQUET_THRESHOLD_MB = 512`, alerta visual. Sem bloquear ainda. |
| 0.2 | `fix/gui-block-full-load-large-parquet` | Bloquear leitura integral de Parquet acima do threshold: `LargeParquetForbiddenError`, desativar caches para arquivos grandes. |

### Fase 1 — DuckDB como backend de consulta da GUI

| PR | Branch | Objetivo |
|----|--------|----------|
| 1.1 | `feat/gui-duckdb-parquet-service` | `DuckDBParquetService` com interface minima: schema, count, page, distinct, export. Testado isoladamente, sem uso na GUI ainda. |
| 1.2 | `perf/gui-auto-select-parquet-backend` | `ParquetQueryService` — roteador que escolhe Polars (≤ 512 MB) ou DuckDB (> 512 MB / diretorio). |

### Fase 2 — Aba Consulta sem travar

| PR | Branch | Objetivo |
|----|--------|----------|
| 2.1 | `perf/gui-query-large-parquet-with-duckdb` | Aba Consulta usa o servico hibrido. Projection pushdown: somente colunas visiveis + filtros/sort. |
| 2.2 | `perf/gui-count-large-parquet-in-background` | Count assincrono; primeira pagina antes da contagem total. |

### Fase 3 — Abas especializadas paginadas

| PR | Branch | Objetivo |
|----|--------|----------|
| 3.1 | `perf/gui-replace-full-parquet-async-loader` | Substituir `_carregar_dados_parquet_async` por `_carregar_pagina_parquet_async` + `_carregar_distinct_async`. |
| 3.2 | `perf/gui-page-mov-estoque` | mov_estoque deixa de manter DataFrame inteiro; filtros viram SQL/DuckDB. |
| 3.3 | `perf/gui-page-nfe-entrada` | Mesmo padrao para nfe_entrada. |
| 3.4 | `perf/gui-page-stock-summary-tabs` | Paginar aba_mensal, aba_anual e aba_periodos (apenas renderizacao; regra fiscal inalterada). Requer `fix/periodos-validar-ei-ef-obrigatorias` em main primeiro. |

### Fase 4 — Agregacao escalavel

| PR | Branch | Objetivo |
|----|--------|----------|
| 4.1 | `perf/agregacao-page-produtos-agrupados` | Tabelas superior/inferior da agregacao ficam paginadas. |
| 4.2 | `perf/agregacao-selection-by-key` | Selecao por `id_agrupado` sobrevive a paginacao (`selected_ids_agrupados: set[str]`). |
| 4.3 | `perf/agregacao-load-only-selected-ids` | Agregar consultando apenas IDs selecionados via `WHERE id_agrupado IN (...)`. |
| 4.4 | `feat/agregacao-manual-delta-map` | Ajuste manual vira delta pequeno; reprocessamento pesado e opcional e assincrono. |

### Fase 5 — Exportacao grande sem DataFrame inteiro

| PR | Branch | Objetivo |
|----|--------|----------|
| 5.1 | `perf/export-duckdb-copy-large-results` | Exportar via `COPY (...) TO ... (FORMAT PARQUET, COMPRESSION ZSTD)`. Excel bloqueado acima do limite. |

### Fase 6 — Parquet particionado

| PR | Branch | Objetivo |
|----|--------|----------|
| 6.1 | `feat/parquet-query-file-or-dataset-dir` | Servico aceita arquivo unico ou diretorio particionado. Hive partitioning. |
| 6.2 | `feat/parquet-partition-mov-estoque` | Salvar mov_estoque particionado por `ano / id_bucket`. |
| 6.3 | `feat/parquet-partition-large-source-tables` | Aplicar particionamento a fontes grandes (>= 2 GB). |

### Fase 7 — Escrita streaming

| PR | Branch | Objetivo |
|----|--------|----------|
| 7.1 | `perf/parquet-streaming-writes` | `salvar_para_parquet_streaming(lf, path, ...)` via `sink_parquet` ou PyArrow Dataset. |

### Fase 8 — MCP DuckDB e benchmarks grandes

| PR | Branch | Objetivo |
|----|--------|----------|
| 8.1 | `feat/tools-duckdb-mcp` | MCP DuckDB local (`tools/duckdb-mcp/server.py`) com tools: `healthcheck`, `execute_sql`, `query_preview`, `explain_sql`, `list_tables`, `describe_table`, `create_table_from_file`, `export_query`, `run_maintenance`, `inspect_parquet`, `preview_parquet`. |
| 8.2 | `test/large-parquet-gui-benchmarks` | Benchmarks reproduziveis para arquivos 256 MB / 1 GB / 2 GB. |

---

## Invariantes preservadas (obrigatorio em todas as PRs)

| Coluna | Significado |
|--------|-------------|
| `id_agrupado` | Chave de agrupamento de produtos |
| `id_agregado` | Chave de agregacao |
| `__qtd_decl_final_audit__` | Quantidade declarada final de auditoria |
| `q_conv` | Quantidade convertida |
| `q_conv_fisica` | Quantidade convertida fisica |

DuckDB **nunca** modifica essas colunas. Sao consumidas apenas por leitura.

---

## Regras inviolaveis

1. Nao substituir Polars no nucleo fiscal.
2. Nao alterar regra fiscal de `aba_periodos`, `mov_estoque`, `calculos_mensais`,
   `calculos_anuais`, `calculos_periodo`, `resumo_global`.
3. Preservar as 5 invariantes acima.
4. Nao usar `pl.read_parquet(path)` em arquivo de usuario dentro da GUI.
5. Nao usar `load_dataset()` para arquivo grande (acima de 512 MB).
6. Nao guardar DataFrame completo em modelo Qt.
7. Nao popular combo com `unique()` integral.
8. Nao exportar resultado gigante via DataFrame em memoria.
9. Nao aplicar filtros grandes depois da materializacao.
10. Cada fase e uma PR pequena, com escopo unico e testes verdes.
11. Nada de merge automatico. Sempre abrir PR Draft e pedir review humano.
12. Cada worker Qt deve abrir sua propria conexao DuckDB.
    Nao compartilhar conexao DuckDB entre threads.
    Nao usar `duckdb.sql()` global em runtime de GUI.
13. Toda query SQL usa parametros, nunca interpolacao de strings.
14. Ao salvar Parquet grande, configurar `row_group_size` entre 100_000 e 1_000_000
    linhas; preferir compressao `zstd`.
15. Datasets particionados devem evitar milhares de arquivos minusculos e tambem
    arquivo unico com row group gigante.

---

## Configuracao Parquet recomendada

```python
df.write_parquet(
    path,
    compression="zstd",
    row_group_size=200_000,   # faixa: 100_000 a 1_000_000
)
```

Para streaming (LazyFrame):

```python
lf.sink_parquet(
    path,
    compression="zstd",
    row_group_size=200_000,
)
```

---

## Dependencias novas (aprovadas)

| Pacote | Versao minima | Adicionado em |
|--------|--------------|---------------|
| `duckdb` | `>=1.1.0` | PR 1.1 |
| `mcp` | `>=1.7.0` | PR 8.1 |
| `pydantic` | `>=2.0.0` | PR 8.1 |

`python-dotenv>=1.0.0` ja esta em `pyproject.toml`.

---

## Deteccao arquivo unico vs. diretorio particionado

```python
source = str(path / "**/*.parquet") if path.is_dir() else str(path)
# Com Hive partitioning:
# conn.execute("SELECT * FROM read_parquet(?, hive_partitioning=true)", [source])
```

---

## Criterios de performance (benchmarks — Fase 8.2)

| Operacao | Criterio |
|----------|----------|
| Abrir primeira pagina (2 GB) | < 5 s em SSD local |
| RAM adicional (2 GB arquivo) | < 1 GB |
| Trocar pagina (sem sort global) | < 2 s |
| Filtro por `id_agrupado` | < 5 s |
| Cache de DataFrame completo | Nao permitido para arquivo grande |

---

## Pre-requisitos antes de comecar

- `fix/periodos-validar-ei-ef-obrigatorias` deve estar em `main` antes da Fase 3.4.
- O harness diferencial (#197) deve estar saudavel para validar PRs de performance
  que toquem qualquer caminho fiscal.
- `main` deve estar verde e sem dividas de CI no runner Windows antes de migrar
  abas com smoke GUI.
- Este documento (`docs/duckdb_plano.md`) deve estar em `main` antes da Fase 0.

---

## Fluxo obrigatorio por PR

1. `git fetch && git checkout main && git pull`
2. Criar branch da fase a partir de `main`.
3. Implementar somente o escopo desta fase.
4. Adicionar/atualizar testes unitarios.
5. Rodar:
   ```bash
   uv run pytest -q -m "not oracle and not gui_smoke"
   uv run ruff check <arquivos alterados>
   uv run mypy src/interface_grafica
   ```
6. Atualizar docstrings e este documento se necessario.
7. Abrir PR Draft com titulo, escopo, riscos e validacoes.
8. Reportar para Enio: link da PR, metricas observadas e proximos passos.

---

## Referencias tecnicas

- DuckDB Python API: https://duckdb.org/docs/api/python/overview
- DuckDB `read_parquet`: https://duckdb.org/docs/data/parquet/overview
- DuckDB `hive_partitioning`: https://duckdb.org/docs/data/partitioning/hive_partitioning
- Polars `sink_parquet`: https://docs.pola.rs/api/python/stable/reference/lazyframe/index.html
- MCP Python SDK: https://github.com/modelcontextprotocol/python-sdk
- PyArrow Dataset (fallback streaming): https://arrow.apache.org/docs/python/dataset.html
