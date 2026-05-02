# audit-pyside-perf-mcp

**Servidor MCP para auditoria, performance e otimização do projeto audit_pyside.**

Fornece ferramentas seguras e controladas para agentes de IA (Claude, Codex, Copilot, Antigravity) executarem diagnósticos, medições e validações sem permitir comandos perigosos ou acesso não autorizado.

## O que faz

Expõe **40+ ferramentas** organizadas em 5 grupos:

### 1. **Projeto Python** (4 tools)
- `run_ruff()` — Lint do projeto
- `run_pytest()` — Execução de testes
- `run_typecheck()` — Type checking (pyright/mypy)
- `project_tree()` — Estrutura de arquivos
- `detect_large_files()` — Detecta arquivos suspeitos

### 2. **Performance** (4 tools)
- `run_pyinstrument_entrypoint()` — Profiling de tempo
- `run_pytest_benchmark()` — Benchmarks versionáveis
- `profile_import_time()` — Custo de importação

### 3. **Oracle (Read-Only)** (6 tools)
- `oracle_ping()` — Testa conexão
- `oracle_describe_table()` — Metadados de colunas
- `oracle_index_report()` — Índices de tabela
- `oracle_explain_select()` — Plano de execução (EXPLAIN PLAN)
- `oracle_readonly_query()` — SELECT/WITH limitado (max 200 linhas)
- `oracle_table_stats()` — Estatísticas de tabela

### 4. **PySide Audit** (5 tools)
- `detect_pyside_performance_smells()` — Padrões ruins (QTableWidget, fetchall(), etc.)
- `detect_qtableview_models()` — Lista modelos de tabela
- `detect_fetchall_in_ui_thread()` — Identifica gargalos de UI
- `inspect_table_models()` — Analisa suporte a lazy loading
- (Mais em desenvolvimento)

### 5. **Polars/Dados** (8 tools)
- `polars_profile_csv()` — Perfil de CSV
- `polars_profile_parquet()` — Perfil de Parquet
- `polars_schema_csv()` / `polars_schema_parquet()` — Schema apenas
- `polars_validate_nulls()` — Validação de nulos
- `polars_detect_duplicates()` — Detecta duplicatas
- `polars_compare_exports()` — Compara dois arquivos
- `polars_run_lazy_query()` — Query lazy contra Parquet

## Segurança explícita

### O que permite
✅ SELECT / WITH (sem limite de dados)  
✅ Metadados Oracle (colunas, índices, stats)  
✅ Rodar testes, lint, profiling  
✅ Ler arquivos Parquet/CSV com Polars  
✅ Medições de performance  

### O que NÃO permite
❌ INSERT, UPDATE, DELETE, DROP, ALTER, TRUNCATE  
❌ DDL (CREATE, GRANT, REVOKE, etc.)  
❌ Múltiplos statements (`;`)  
❌ Acesso a arquivos fora da raiz do projeto  
❌ Comandos shell livres  
❌ Armazenar senhas/tokens no código  

## Arquitetura

```
mcp_server/
├─ server.py              (entrypoint simples)
└─ src/
   └─ audit_pyside_perf_mcp/
      ├─ __init__.py
      ├─ config.py           (Config centralizada)
      ├─ security.py         (Guardrails de segurança)
      ├─ tools_project.py    (Qualidade do projeto)
      ├─ tools_oracle.py     (Oracle read-only)
      ├─ tools_pyside.py     (Detecção PySide)
      ├─ tools_perf.py       (Profiling)
      └─ tools_polars.py     (Análise de dados)
```

## Configuração

### Variáveis de ambiente

```bash
export AUDIT_PYSIDE_ROOT="/path/to/audit_pyside"
export ORACLE_USER="user"
export ORACLE_PASSWORD="pass"
export ORACLE_DSN="host:1521/service"
```

### Em `mcp_servers.json`

```json
{
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

## Uso

### Exemplo 1: Validar um Parquet antes de pipeline

```
Agente: "Valide dados/estoque_raw.parquet"
  ↓
audit-pyside-perf-mcp.polars_profile_parquet("dados/estoque_raw.parquet")
  ↓
Resposta: {
  "ok": true,
  "rows": 50000,
  "columns": 12,
  "schema": {...},
  "null_counts": {...}
}
```

### Exemplo 2: Encontrar gargalo em PySide

```
Agente: "Detecte problemas de performance na GUI"
  ↓
audit-pyside-perf-mcp.detect_pyside_performance_smells()
  ↓
Resposta: [
  {
    "file": "src/interface_grafica/main_window.py",
    "line": 145,
    "pattern": "fetchall",
    "hint": "fetchall() carrega tudo; prefira iterator ou limite"
  },
  ...
]
```

### Exemplo 3: Explicar plano Oracle antes de otimizar

```
Agente: "Explique o plano dessa query"
  ↓
audit-pyside-perf-mcp.oracle_explain_select(
  "SELECT * FROM defn.tab_movimentacao WHERE data > TRUNC(SYSDATE) - 30"
)
  ↓
Resposta: {
  "ok": true,
  "plan": "Plan hash value: ...\nFull table scan..."
}
```

## Fluxo recomendado

Antes de alterar código:

1. **Medir** → `run_pyinstrument_entrypoint()` ou `run_pytest_benchmark()`
2. **Diagnosticar** → `detect_pyside_performance_smells()` ou `oracle_explain_select()`
3. **Validar schema** → `polars_schema_parquet()` ou `oracle_describe_table()`

Depois de alterar:

1. **Lint** → `run_ruff()`
2. **Test** → `run_pytest()`
3. **Compare** → `run_pytest_benchmark()` (medir novamente)

## Comportamento esperado

Toda mudança deve responder:

- **Qual gargalo foi medido?** (pyinstrument, benchmark)
- **Qual era o tempo antes?** (baseline)
- **Qual é o tempo depois?** (novo resultado)
- **O banco rodou melhor?** (EXPLAIN PLAN antes/depois)
- **Os testes passam?** (pytest)

Sem isso, é "otimização por palpite", não por dados.

## Limitações intencionais

| Limitação | Razão |
|---|---|
| Max 200 linhas em SELECT | Evitar travamento de rede/app |
| Sem acesso a produção poderosa | Risco de alteração acidental |
| Sem shell livre | Prevenir comandos perigosos |
| Sem armazenar secrets | Segurança |
| Sem múltiplos statements | Bloquear injections |

## Troubleshooting

| Problema | Solução |
|---|---|
| "oracledb não instalado" | `pip install oracledb` |
| "Polars não instalado" | `pip install polars` |
| "Credenciais não configuradas" | Verificar variáveis de ambiente |
| "Caminho bloqueado" | Usar caminho relativo a `AUDIT_PYSIDE_ROOT` |
| "SQL bloqueado" | Verificar que é SELECT/WITH sem `;` |

## Próximos passos

- [ ] Tool para comparar schemaS entre Parquet e Oracle automaticamente
- [ ] Tool para detectar anomalias em dados (outliers, distribuição)
- [ ] Integração com GitHub para criar issues automáticas
- [ ] Profiling de memória com tracemalloc
- [ ] Tool para sugerir índices Oracle baseado em EXPLAIN PLAN

## Referências

- [MCP Protocol](https://modelcontextprotocol.io/)
- [FastMCP (Python SDK)](https://github.com/modelcontextprotocol/python-sdk)
- [Polars Docs](https://docs.pola.rs/)
- [Oracle EXPLAIN PLAN](https://docs.oracle.com/cd/B19306_01/server.102/b14211/ex_plan.htm)
