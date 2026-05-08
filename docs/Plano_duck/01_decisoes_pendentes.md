# Decisoes Pendentes (D1–D7) — Plano DuckDB

Este documento registra as decisoes arquiteturais e operacionais pendentes para o Plano DuckDB.

| ID | Decisao | Status | Data |
|---|---|---|---|
| D1 | Nome oficial do componente DuckDB | PENDENTE | — |
| D2 | Threshold de chaveamento Polars/DuckDB | PENDENTE | — |
| D3 | Politica de cache de conexao DuckDB | PENDENTE | — |
| D4 | Formato de log para auditoria DuckDB | PENDENTE | — |
| D5 | Estrategia de teste para datasets > 2GB | PENDENTE | — |
| **D6** | ** Roadmap e Escopo do MCP DuckDB** | **RESOLVIDA** | 2026-05-05 |
| D7 | Configuracao de concorrencia DuckDB (Threads) | PENDENTE | — |

---

## D6 — Roadmap e Escopo do MCP DuckDB

**Pergunta:** Quando o MCP DuckDB entra no roadmap, quem e o consumidor e qual o escopo?

**Status:** RESOLVIDA (2026-05-05)

### Decisao:
A fase **E6 — MCP DuckDB** entra no roadmap de **2026**. O objetivo e fornecer uma interface de ferramentas (tools) para agentes de IA e usuarios avancados explorarem os arquivos Parquet do projeto de forma eficiente e segura, sem depender exclusivamente da GUI.

### Especificacao:

1. **Roadmap:** Fase E6, com inicio previsto para 2026 (apos estabilizacao da paginacao na GUI).
2. **Consumidores:**
   - Claude Desktop (via configuracao de servidor MCP local).
   - Agentes autonomos de diagnostico.
   - Ferramentas de linha de comando para auditores.
3. **Arquitetura:** O servidor MCP sera **Standalone** (executavel separado ou via `mcp_server/server_duck.py`), mas podera ser invocado a partir da aplicacao principal em modo "debug/expert".
4. **Tool Schema (Escopo Basico):**
   - `healthcheck`: Status do backend DuckDB.
   - `execute_sql`: Execucao de queries SELECT limitadas contra Parquets.
   - `query_preview`: Amostra de dados rapida.
   - `explain_sql`: Plano de execucao DuckDB.
   - `list_tables`: Listagem de arquivos Parquet disponiveis no diretorio de dados.
   - `describe_table`: Schema e metadados de um arquivo especifico.
   - `create_table_from_file`: Importacao rapida de arquivos externos para o ecossistema DuckDB.
   - `export_query`: Exportacao rapida de resultados para novo Parquet/CSV.
   - `run_maintenance`: Otimizacao e limpeza de arquivos temporarios/metadados.
   - `inspect_parquet`: Metadados fisicos do arquivo (row groups, compressao).
   - `preview_parquet`: Visualizacao rapida das primeiras/ultimas linhas de um arquivo.

### Justificativa:
Separar o servidor MCP permite que ele seja usado em ambientes de CI/CD para validacao de dados e por agentes de IA sem a necessidade de instanciar a GUI completa (PySide6), reduzindo o consumo de recursos e aumentando a versatilidade do ecossistema de ferramentas de auditoria.
