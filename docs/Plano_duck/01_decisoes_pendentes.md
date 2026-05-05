# Decisões Pendentes (D1–D7) — Plano DuckDB

Este documento registra as decisões arquiteturais e operacionais pendentes para o Plano DuckDB.

| ID | Decisão | Status | Data |
|---|---|---|---|
| D1 | Nome oficial do componente DuckDB | PENDENTE | — |
| D2 | Threshold de chaveamento Polars/DuckDB | PENDENTE | — |
| D3 | Política de cache de conexão DuckDB | PENDENTE | — |
| D4 | Formato de log para auditoria DuckDB | PENDENTE | — |
| D5 | Estratégia de teste para datasets > 2GB | PENDENTE | — |
| **D6** | ** Roadmap e Escopo do MCP DuckDB** | **RESOLVIDA** | 2026-05-05 |
| D7 | Configuração de concorrência DuckDB (Threads) | PENDENTE | — |

---

## D6 — Roadmap e Escopo do MCP DuckDB

**Pergunta:** Quando o MCP DuckDB entra no roadmap, quem é o consumidor e qual o escopo?

**Status:** RESOLVIDA (2026-05-05)

### Decisão:
A fase **E6 — MCP DuckDB** entra no roadmap de **2026**. O objetivo é fornecer uma interface de ferramentas (tools) para agentes de IA e usuários avançados explorarem os arquivos Parquet do projeto de forma eficiente e segura, sem depender exclusivamente da GUI.

### Especificação:

1. **Roadmap:** Fase E6, com início previsto para 2026 (após estabilização da paginação na GUI).
2. **Consumidores:**
   - Claude Desktop (via configuração de servidor MCP local).
   - Agentes autônomos de diagnóstico.
   - Ferramentas de linha de comando para auditores.
3. **Arquitetura:** O servidor MCP será **Standalone** (executável separado ou via `mcp_server/server_duck.py`), mas poderá ser invocado a partir da aplicação principal em modo "debug/expert".
4. **Tool Schema (Escopo Básico):**
   - `healthcheck`: Status do backend DuckDB.
   - `execute_sql`: Execução de queries SELECT limitadas contra Parquets.
   - `query_preview`: Amostra de dados rápida.
   - `explain_sql`: Plano de execução DuckDB.
   - `list_tables`: Listagem de arquivos Parquet disponíveis no diretório de dados.
   - `describe_table`: Schema e metadados de um arquivo específico.
    - `create_table_from_file`: Importação rápida de arquivos externos para o ecossistema DuckDB.
   - `export_query`: Exportação rápida de resultados para novo Parquet/CSV.
    - `run_maintenance`: Otimização e limpeza de arquivos temporários/metadados.
   - `inspect_parquet`: Metadados físicos do arquivo (row groups, compressão).
    - `preview_parquet`: Visualização rápida das primeiras/últimas linhas de um arquivo.

### Justificativa:
Separar o servidor MCP permite que ele seja usado em ambientes de CI/CD para validação de dados e por agentes de IA sem a necessidade de instanciar a GUI completa (PySide6), reduzindo o consumo de recursos e aumentando a versatilidade do ecossistema de ferramentas de auditoria.
