# Guia de Integração MCP — audit_pyside

Data: 2026-05-01  
Stack: Python + PySide6 + Polars + Oracle  
Status: Ativo

## Visão geral

O projeto `audit_pyside` agora usa **5 MCPs coordenados** para potencializar análise, validação e desenvolvimento com agentes de IA:

| MCP | Função | Tipo | Status |
|---|---|---|---|
| `ask_polars` | Consulta a IA sobre código Polars correto | MCP oficial remoto | ✅ Ativo |
| `audit-pyside-perf-mcp` | Tools seguras para auditoria, performance e dados | MCP customizado local | ✅ Ativo |
| `github-mcp-server` | Integração com GitHub (branches, PRs, issues) | Docker | ✅ Ativo |
| `filesystem` | Acesso seguro a arquivos do projeto | MCP oficial local | ✅ Ativo |
| `oracle` (via perf-mcp) | Queries Oracle seguras (read-only) | Integrado localmente | ✅ Ativo |

---

## 1. `ask_polars` — MCP oficial Polars

### O quê?
IA especializada em Polars, respondendo perguntas sobre sintaxe, performance e best practices.

### Como usar?
Pergunte ao Copilot:
- "Como usar `.group_by()` em Polars 1.x para agregar sem `.groupby()`?"
- "Qual é a forma correta de fazer lazy evaluation em Parquet?"
- "Como validar schema de duas DataFrames antes de join?"

### Exemplo de uso esperado
```python
# Copilot (via ask_polars) sugere:
lf = pl.scan_parquet("data.parquet")
result = lf.filter(pl.col("id").is_not_null()).group_by("categoria").agg(pl.col("valor").sum()).collect()
```

---

## 2. `audit-pyside-perf-mcp` — Servidor de ferramentas seguras

**Servidor MCP local com 40+ tools para auditoria, performance e otimização.**

Reside em `mcp_server/` com arquitetura modular:

```
mcp_server/
├─ server.py
├─ README.md
└─ src/audit_pyside_perf_mcp/
   ├─ config.py        (Configuração e limites)
   ├─ security.py      (Guardrails de SQL e paths)
   ├─ tools_project.py (Ruff, pytest, typecheck, estrutura)
   ├─ tools_oracle.py  (Metadados, EXPLAIN PLAN, SELECT read-only)
   ├─ tools_pyside.py  (Detecção de padrões ruins de performance)
   ├─ tools_perf.py    (Profiling com pyinstrument, benchmarks)
   └─ tools_polars.py  (Análise Polars: profile, schema, duplicatas, etc.)
```

### 2.1 Grupo A — Qualidade do Projeto

Tools para checks, lint, testes:

| Tool | O que faz |
|---|---|
| `run_ruff()` | Lint do projeto |
| `run_pytest()` | Executa testes (com marker opcional) |
| `run_typecheck()` | Type checking (pyright/mypy) |
| `project_tree()` | Lista arquivos relevantes |
| `detect_large_files()` | Detecta arquivos > 1MB |

**Uso:**
```
"Execute ruff no projeto"
  → audit-pyside-perf-mcp.run_ruff()
  → Retorna lista de erros/avisos
```

---

### 2.2 Grupo B — Performance (Profiling)

Tools para medir onde o tempo é gasto:

| Tool | O que faz |
|---|---|
| `run_pyinstrument_entrypoint()` | Profiling com pyinstrument |
| `run_pytest_benchmark()` | Benchmarks versionáveis |
| `profile_import_time()` | Custo de importação |

**Uso:**
```
"Mede onde o app gasta tempo"
  → audit-pyside-perf-mcp.run_pyinstrument_entrypoint("app.py")
  → Retorna árvore de tempo por função
```

---

### 2.3 Grupo C — Oracle Read-Only (CRÍTICO)

**Segurança de primeiro nível:**

| Tool | O que permite | O que bloqueia |
|---|---|---|
| `oracle_ping()` | Testa conexão | — |
| `oracle_describe_table()` | Ver colunas/tipos (metadados) | Acesso a dados |
| `oracle_index_report()` | Ver índices | — |
| `oracle_explain_select()` | EXPLAIN PLAN para SELECT | DDL/DML |
| `oracle_readonly_query()` | SELECT/WITH (max 200 linhas) | INSERT/UPDATE/DELETE/DROP/etc. |
| `oracle_table_stats()` | Ver estatísticas (num_rows, blocks) | — |

**SQL bloqueado:**
- `INSERT`, `UPDATE`, `DELETE`
- `DROP`, `ALTER`, `TRUNCATE`, `MERGE`
- `GRANT`, `REVOKE`, `CREATE`, `EXECUTE`, `CALL`, `BEGIN`, `DECLARE`
- Múltiplos statements (`;`)

**SQL permitido:**
```sql
SELECT id, descricao FROM tab WHERE data > '2026-01-01'
WITH cte AS (SELECT ...) SELECT * FROM cte
```

**Uso:**
```
audit-pyside-perf-mcp.oracle_readonly_query(
  "SELECT id_agrupado, SUM(q_conv) FROM tab_mov WHERE data > TRUNC(SYSDATE) - 30 GROUP BY id_agrupado",
  max_rows=100
)
```

---

### 2.4 Grupo D — PySide Audit

Detecta padrões comuns que degradam performance em Qt:

| Tool | Procura | Sugestão |
|---|---|---|
| `detect_pyside_performance_smells()` | QTableWidget, fetchall(), SELECT *, sleep() | Usar QTableView + modelo, lazy loading, iterator |
| `detect_qtableview_models()` | Classes derivadas de QAbstractTableModel | Verificar se há suporte a canFetchMore()/fetchMore() |
| `detect_fetchall_in_ui_thread()` | `.fetchall()` no código Python/UI | Mover para thread worker |
| `inspect_table_models()` | Analisa implementação de modelos | Verificar lazy loading |

**Uso:**
```
"Detecte gargalos de performance na GUI"
  → audit-pyside-perf-mcp.detect_pyside_performance_smells()
  → Retorna lista de padrões ruins com hints
```

---

### 2.5 Grupo E — Polars/Dados

Tools para validar Parquet, CSV, comparar exports:

| Tool | O que faz |
|---|---|
| `polars_profile_csv()` | Perfil rápido de CSV (linhas, colunas, nulos) |
| `polars_profile_parquet()` | Perfil de Parquet com LazyFrame |
| `polars_schema_csv()` | Lê schema sem carregar dados |
| `polars_schema_parquet()` | Schema de Parquet (super rápido) |
| `polars_validate_nulls()` | % de nulos por coluna |
| `polars_detect_duplicates()` | Detecta duplicatas |
| `polars_compare_exports()` | Compara dois arquivos (CSV/Parquet) |
| `polars_run_lazy_query()` | Executa query lazy contra Parquet |

**Uso:**
```
"Valide dados/estoque_raw.parquet"
  → polars_profile_parquet("dados/estoque_raw.parquet")
  → {
      "ok": true,
      "rows": 50000,
      "columns": 12,
      "schema": {"id_agrupado": "String", "q_conv": "Float64"},
      "null_counts": {"id_agrupado": 0, "q_conv": 120}
    }
```

---

## 3. GitHub MCP — `github-mcp-server`

Já ativo via Docker. Expõe:
- Criar/comentar issues
- Ler/abrir PRs
- Gerenciar branches
- Listar commits

**Caso de uso:** Agentes criam automaticamente issues de validação e PRs de correção.

---

## 4. Filesystem MCP — acesso seguro

Restrito a:
- `c:\audit_pyside`
- `C:\Users\eniot\Downloads`

**Caso de uso:** Ler/escrever arquivos temporários de teste.

---

## Fluxo recomendado de uso

### Cenário 1: Validar um Parquet novo

```
1. Agente recebe pedido para validar dados
2. audit-pyside-perf-mcp.polars_profile_parquet("output/novo_dataset.parquet")
3. Se houver anomalia, ask_polars ajuda a escrever query customizada
4. polars_run_lazy_query verifica filtros
5. polars_validate_nulls garante qualidade (especialmente __qtd_decl_final_audit__)
6. GitHub MCP cria issue com resultado
```

### Cenário 2: Comparar exportação Oracle vs Parquet

```
1. ask_polars: "Como validar que meu Polars export casou com Oracle?"
2. audit-pyside-perf-mcp.oracle_describe_table("DEFN", "TAB_DATA")
3. audit-pyside-perf-mcp.oracle_explain_select("SELECT ... FROM defn.tab_data WHERE ...")
4. audit-pyside-perf-mcp.polars_compare_exports("data_oracle.csv", "data_polars.parquet")
5. Se houver divergências, agente abre PR com correção
6. Testes rodam automaticamente
```

### Cenário 3: Debug de performance em PySide

```
1. UI relatou travamento ao carregar tabela com 50k linhas
2. audit-pyside-perf-mcp.detect_pyside_performance_smells()
3. Encontra QTableWidget ou fetchall() em thread principal
4. ask_polars: "Como usar canFetchMore/fetchMore em PySide?"
5. Agente refatora para modelo lazy
6. run_pytest_benchmark() compara performance antes/depois
7. GitHub MCP cria PR com refactoring
```

### Cenário 4: Debug de duplicatas em agregação

```
1. Usuário relatou duplicatas após "Agregar Descrições"
2. audit-pyside-perf-mcp.polars_detect_duplicates("output/agrupados.parquet")
3. Se `duplicate_pct > 0`:
   - ask_polars sugere `.drop_duplicates(subset=["id_agrupado"])`
   - polars_run_lazy_query testa a correção
   - GitHub MCP cria issue de refactor
```

---

## Configuração (já feita)

### `mcp_servers.json`
```json
{
  "ask_polars": {
    "command": "npx",
    "args": ["-y", "mcp-remote", "https://mcp.pola.rs/mcp"]
  },
  "audit-pyside-perf-mcp": {
    "command": "python",
    "args": ["c:\\audit_pyside\\mcp_server\\server.py"],
    "env": {
      "AUDIT_PYSIDE_ROOT": "c:\\audit_pyside",
      "ORACLE_USER": "...",
      "ORACLE_PASSWORD": "...",
      "ORACLE_DSN": "..."
    }
  }
}
```

---

## Diferença: MCP Customizado vs Shell Livre

| Aspecto | Shell Livre | audit-pyside-perf-mcp |
|---|---|---|
| Segurança | Baixa (agente inventa comandos) | Alta (tools explícitas) |
| Previsibilidade | Imprevisível | Determinística |
| Auditoria | Difícil rastrear | Cada tool é explícita |
| Performance | Pode travar (SELECT * infinito) | Limitado (max 200 linhas) |
| Autorização | Nenhuma | SQL bloqueado no código |
| SQL destrutivo | Possível | Bloqueado no guardrail |

---

## Troubleshooting

| Problema | Solução |
|---|---|
| `ask_polars` não responde | Verificar conexão https://mcp.pola.rs/mcp |
| `polars_run_lazy_query` falha com path | Usar caminho relativo a `PROJECT_ROOT` |
| `oracle_readonly_query` bloqueada | Verificar que SQL é SELECT/WITH, sem `;` |
| Parquet não encontrado | Verificar que arquivo existe em `c:\audit_pyside\<path>` |
| Polars/oracledb não importam | `pip install polars oracledb` |
| Credenciais Oracle erradas | Verificar variáveis de ambiente |

---

## Próximos passos

1. **Tests automáticos:** Criar testes que chamam `audit-pyside-perf-mcp` tools para validar schema em CI/CD.
2. **Dashboard:** Integrar tools com GUI para exibir relatório de qualidade de Parquet.
3. **Alert fiscal:** Se `__qtd_decl_final_audit__` tiver nulos, GitHub MCP cria issue crítica.
4. **Extended tools:** Detecção de anomalias, profiling de memória, sugestões de índices Oracle.

---

## Referências

- [MCP Protocol](https://modelcontextprotocol.io/)
- [FastMCP (Python SDK)](https://github.com/modelcontextprotocol/python-sdk)
- [Polars Docs](https://docs.pola.rs/)
- [Oracle EXPLAIN PLAN](https://docs.oracle.com/cd/B19306_01/server.102/b14211/ex_plan.htm)
- [PySide6 Model/View](https://doc.qt.io/qt-6/model-view-programming.html)
- [audit-pyside-perf-mcp README](../mcp_server/README.md)

---

## 1. `ask_polars` — MCP oficial Polars

### O quê?
IA especializada em Polars, respondendo perguntas sobre sintaxe, performance e best practices.

### Como usar?
Pergunte ao Copilot:
- "Como usar `.group_by()` em Polars 1.x para agregar sem `.groupby()`?"
- "Qual é a forma correta de fazer lazy evaluation em Parquet?"
- "Como validar schema de duas DataFrames antes de join?"

### Exemplo de uso esperado
```python
# Copilot (via ask_polars) sugere:
lf = pl.scan_parquet("data.parquet")
result = lf.filter(pl.col("id").is_not_null()).group_by("categoria").agg(pl.col("valor").sum()).collect()
```

---

## 2. `audit-pyside-mcp` — Tools Polars customizadas

Reside em `mcp_server/server.py`. Expõe 8 ferramentas para **validar dados de verdade**.

### 2.1 `polars_profile_csv` / `polars_profile_parquet`
Gera perfil rápido de um arquivo.

**Input:**
- `path`: caminho relativo ao `PROJECT_ROOT` (ex: `dados/raw/estoque.csv`)
- `max_rows`: limite de linhas a ler (default: 1000)

**Output:**
```json
{
  "rows": 50000,
  "columns": 12,
  "schema": {
    "id_agrupado": "String",
    "q_conv": "Float64",
    "data": "Date"
  },
  "null_counts": {
    "id_agrupado": 0,
    "q_conv": 120,
    "data": 5
  }
}
```

**Caso de uso:** Validar que um CSV/Parquet foi exportado corretamente antes de pipeline.

---

### 2.2 `polars_schema_csv` / `polars_schema_parquet`
Lê **apenas o schema** (super rápido, sem carga de dados).

**Input:**
- `path`: caminho relativo

**Output:**
```json
{
  "schema": {
    "id_agrupado": "String",
    "q_conv": "Float64"
  }
}
```

**Caso de uso:** Verificar compatibilidade de schema antes de merge ou join.

---

### 2.3 `polars_validate_nulls`
Detecta padrão de nulos por coluna.

**Input:**
- `path`: caminho relativo
- `file_type`: `"parquet"` ou `"csv"`

**Output:**
```json
{
  "total_rows": 50000,
  "null_per_column": {
    "id_agrupado": 0,
    "q_conv": 120
  },
  "null_pct_per_column": {
    "id_agrupado": 0.0,
    "q_conv": 0.24
  }
}
```

**Caso de uso:** Audit fiscal — garantir que colunas críticas (`id_agrupado`, `__qtd_decl_final_audit__`) têm 0% nulos.

---

### 2.4 `polars_detect_duplicates`
Detecta linhas duplicadas.

**Input:**
- `path`: caminho relativo
- `subset`: `null` (todas as colunas) ou lista `["id_agrupado", "data"]`
- `file_type`: `"parquet"` ou `"csv"`

**Output:**
```json
{
  "total_rows": 50000,
  "duplicate_rows": 42,
  "duplicate_pct": 0.084
}
```

**Caso de uso:** Validar que a agregação de produtos não criou duplicatas não intencionais.

---

### 2.5 `polars_compare_oracle_export`
Compara schema e amostra entre Parquet e Oracle direto.

**Input:**
- `parquet_path`: ex: `output/estoque_curated.parquet`
- `oracle_table`: ex: `DEFN.TAB_ESTOQUE` ou `TAB_ESTOQUE`
- `join_cols`: colunas para reconciliação (ex: `["id_agrupado"]`)
- `sample_size`: quantas linhas comparar (default: 100)

**Output:**
```json
{
  "parquet_rows": 8500,
  "oracle_rows": 8500,
  "parquet_schema": { "id_agrupado": "String", ... },
  "oracle_schema": { "id_agrupado": "String", ... }
}
```

**Caso de uso:** Validar que a exportação do Oracle casou com o Parquet curated.

---

### 2.6 `polars_run_lazy_query`
Executa query Polars lazy contra um Parquet.

**Input:**
- `parquet_path`: ex: `output/estoque_marts.parquet`
- `query_expr`: expressão Polars lazy, ex: `.filter(pl.col("id_agrupado").is_not_null()).head(100)`
- `output_format`: `"json"` (default) ou `"csv"`

**Output (JSON):**
```json
[
  { "id_agrupado": "ABC123", "q_conv": 1000, "data": "2026-01-15" },
  ...
]
```

**Caso de uso:** Testar transformações Polars contra dados reais sem rodar todo o pipeline.

---

## 3. Oracle Read-Only via `audit-pyside-mcp`

### `oracle_readonly_query`
Executa SELECT/WITH no Oracle com limite de linhas (max 200).

**Input:**
```sql
with cte as (
  select id_agrupado, sum(q_conv) as total
  from defn.tab_movimentacao
  where data >= '2026-01-01'
  group by id_agrupado
)
select * from cte
```

**Output:** Lista de dicts.

**Segurança:**
- ✅ Permite: `SELECT`, `WITH`
- ❌ Bloqueia: `INSERT`, `UPDATE`, `DELETE`, `DROP`, `ALTER`, etc.
- ❌ Bloqueia: múltiplos statements (`;`)

**Caso de uso:** Validar dados no Oracle antes de exportá-los para Parquet.

---

## 4. GitHub MCP — `github-mcp-server`

Já ativo via Docker. Expõe:
- Criar/comentar issues
- Ler/abrir PRs
- Gerenciar branches
- Listar commits

**Caso de uso:** Agentes criam automaticamente issues de validação e PRs de correção.

---

## 5. Filesystem MCP — acesso seguro

Restrito a:
- `c:\audit_pyside`
- `C:\Users\eniot\Downloads`

**Caso de uso:** Ler/escrever arquivos temporários de teste.

---

## Fluxo recomendado de uso

### Cenário 1: Validar um Parquet novo

```
1. Agente tira screenshot do usuário pedindo validação
2. Copilot chama polars_profile_parquet("output/novo_dataset.parquet")
3. ask_polars ajuda a escrever query se houver anomalia
4. polars_run_lazy_query verifica filtros customizados
5. polars_validate_nulls garante qualidade
6. GitHub MCP cria issue de QA com resultado
```

### Cenário 2: Comparar exportação Oracle vs Parquet

```
1. ask_polars: "Como validar que meu Polars export casou com Oracle?"
2. polars_compare_oracle_export("data.parquet", "DEFN.TAB_DATA", ["id_agrupado"])
3. Se houver divergências, agente abre PR com correção
4. Testes rodam automaticamente
```

### Cenário 3: Debug de duplicatas em agregação

```
1. UI relatou duplicatas após "Agregar Descrições"
2. Copilot chama polars_detect_duplicates("output/agrupados.parquet", subset=["id_agrupado"])
3. Se `duplicate_pct > 0`, ask_polars sugere `.drop_duplicates(subset=["id_agrupado"])`
4. polars_run_lazy_query testa a correção
5. GitHub MCP cria issue de refactor
```

---

## Configuração (já feita)

### `mcp_servers.json`
```json
{
  "ask_polars": {
    "command": "npx",
    "args": ["-y", "mcp-remote", "https://mcp.pola.rs/mcp"]
  },
  "audit-pyside-mcp": {
    "command": "python",
    "args": ["c:\\audit_pyside\\mcp_server\\server.py"],
    "env": {
      "AUDIT_PYSIDE_ROOT": "c:\\audit_pyside",
      "ORACLE_USER": "03002693901",
      ...
    }
  }
}
```

### Variáveis de ambiente
Já set em `mcp_servers.json`:
- `AUDIT_PYSIDE_ROOT` → root do projeto
- `ORACLE_USER`, `ORACLE_PASSWORD`, `ORACLE_DSN` → conexão

---

## Troubleshooting

| Problema | Solução |
|---|---|
| `ask_polars` não responde sobre Polars | Verificar conexão https://mcp.pola.rs/mcp |
| `polars_run_lazy_query` falha com path | Usar caminho relativo a `PROJECT_ROOT` (ex: `output/file.parquet`, não `c:\...`) |
| Oracle query bloqueada | Verificar que usa apenas `SELECT`/`WITH`, sem `;` no final |
| Parquet não encontrado | Verificar que arquivo existe em `c:\audit_pyside\<path>` |
| Import de polars falha | Rodar `pip install polars oracledb` no venv |

---

## Próximos passos

1. **Tests automáticos:** Criar testes que chamam `polars_*` para validar schema em CI/CD.
2. **Dashboard:** Integrar tools com GUI para exibir relatório de qualidade de Parquet.
3. **Alert fiscal:** Se `__qtd_decl_final_audit__` tiver nulos, GitHub MCP cria issue crítica.
4. **Ask backend:** Estender `audit-pyside-mcp` com tools de pipeline (ex: `validate_pipeline_output`).

---

## Referências

- [MCP Protocol](https://modelcontextprotocol.io/)
- [Polars Docs](https://docs.pola.rs/)
- [FastMCP (Python SDK)](https://github.com/modelcontextprotocol/python-sdk)
- [audit-pyside AGENTS.md](../AGENTS.md)
