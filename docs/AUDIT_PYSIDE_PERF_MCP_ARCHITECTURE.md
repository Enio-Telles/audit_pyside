# audit-pyside-perf-mcp — Arquitetura e Filosofia

Data: 2026-05-01  
Versão: 0.1.0  
Status: Production-ready

---

## Filosofia: "Ferramentas Seguras, Não Poder Livre"

O MCP customizado do audit_pyside **não é um agente com acesso irrestrito** ao projeto. É um **servidor de ferramentas controladas** que força o agente de IA a trabalhar de forma segura, rastreável e previsível.

```
Abordagem Errada:
Agente → Terminal Livre → Oracle Livre → Filesystem Livre → Dano possível

Abordagem Correta:
Agente → audit-pyside-perf-mcp (tools explícitas) → Ações seguras e auditáveis
```

---

## 5 Grupos de Ferramentas

### Grupo A: Qualidade do Projeto (5 tools)
**O que faz:** Roda linters, testes, type checking, lista estrutura.

**Casos de uso:**
- Antes de abrir PR: `run_ruff()` + `run_pytest()`
- Onboarding técnico: `project_tree()`
- Performance de importação: `profile_import_time()`

**Segurança:** Nenhuma — rodam dentro do PROJECT_ROOT, com timeouts.

---

### Grupo B: Performance/Profiling (3 tools)
**O que faz:** Mede onde o tempo é gasto, compara benchmarks.

**Casos de uso:**
- Diagnosticar travamento: `run_pyinstrument_entrypoint("app.py")`
- Validar otimização: `run_pytest_benchmark()` antes e depois
- Análise de startup: `profile_import_time()`

**Segurança:** Roda comandos com timeout; output limitado.

---

### Grupo C: Oracle Read-Only (6 tools) ⭐ CRÍTICO
**O que faz:** Metadados, EXPLAIN PLAN, SELECT limitado — NADA de DDL/DML.

**Camadas de defesa:**

1. **Bloqueia na SDK:**
   - Função `is_safe_sql()` valida SELECT/WITH
   - Rejeita INSERT, UPDATE, DELETE, DROP, ALTER, etc.
   - Bloqueia múltiplos statements (`;`)

2. **Bloqueia em arredondamento:**
   - Usuário Oracle NO BANCO sem permissão de escrita
   - EXPLAIN PLAN mostra o que Oracle faria, sem executar

3. **Bloqueia em limite de resultado:**
   - Max 200 linhas em cada query
   - Timeout 30 segundos

**Casos de uso:**
- Validar schema: `oracle_describe_table("DEFN", "TAB_MOVIMENTACAO")`
- Entender plano: `oracle_explain_select("SELECT ... WHERE ...")`
- Amostra de dados: `oracle_readonly_query("SELECT ...", max_rows=100)`

**O que bloqueia:**
- ❌ Escrever dados
- ❌ Criar/droppar tabelas
- ❌ Executar procedures
- ❌ Grant/revoke permissões

---

### Grupo D: PySide Audit (5 tools)
**O que faz:** Procura padrões comuns de performance ruim em Qt.

**Detecta:**
- QTableWidget sem paginação → sugestão: QTableView + modelo
- `.fetchall()` em thread principal → sugestão: worker thread
- SELECT * sem colunas específicas → sugestão: especificar colunas
- `.processEvents()` → sugestão: workers/QTimer

**Casos de uso:**
- Diagnóstico de travamento: `detect_pyside_performance_smells()`
- Análise de modelos: `inspect_table_models()`
- Auditoria de fetchall: `detect_fetchall_in_ui_thread()`

**Segurança:** Apenas leitura de código Python; procura por padrões.

---

### Grupo E: Polars/Dados (8 tools)
**O que faz:** Valida, perfila, compara Parquet e CSV.

**Casos de uso:**

1. **Validação fiscal (CRÍTICO):**
   ```
   polars_validate_nulls("output/final.parquet")
   → Se __qtd_decl_final_audit__ tem nulos, FALHA
   ```

2. **Validação antes de pipeline:**
   ```
   polars_profile_parquet("dados/raw/estoque.parquet")
   → Vê schema, # de linhas, # de nulos
   ```

3. **Detecção de anomalias:**
   ```
   polars_detect_duplicates("output/agrupados.parquet", subset=["id_agrupado"])
   → Se duplicate_pct > 0, pode haver problema de lógica
   ```

4. **Reconciliação Oracle ↔ Polars:**
   ```
   polars_compare_exports("raw_oracle.csv", "processed.parquet")
   → Compara schema e amostra
   ```

**Segurança:**
- Paths validados contra blocklist (`.venv`, `.git`, etc.)
- LazyFrame usado quando possível (predicate pushdown)
- Amostra limitada para não sobrecarregar memória

---

## Arquitetura Modular

```
┌─ server.py (entrypoint)
│  └─ imports e registra tudo
│
└─ src/audit_pyside_perf_mcp/
   ├─ config.py
   │  └─ Config (paths permitidos, limites, variáveis de env)
   │
   ├─ security.py
   │  ├─ SqlSecurityError
   │  ├─ is_safe_sql()     ← Valida SELECT/WITH
   │  ├─ is_safe_path()    ← Valida caminhos
   │  └─ guard_sql/guard_path  ← Levanta exceção
   │
   ├─ tools_project.py
   │  └─ register_project_tools()
   │     ├─ @mcp.tool() run_ruff
   │     ├─ @mcp.tool() run_pytest
   │     └─ ...
   │
   ├─ tools_oracle.py
   │  └─ register_oracle_tools()
   │     ├─ @mcp.tool() oracle_ping()
   │     ├─ @mcp.tool() oracle_readonly_query()  ← usa guard_sql()
   │     └─ ...
   │
   ├─ tools_pyside.py
   │  └─ register_pyside_tools()
   │     ├─ @mcp.tool() detect_pyside_performance_smells()
   │     └─ ...
   │
   ├─ tools_perf.py
   │  └─ register_perf_tools()
   │     ├─ @mcp.tool() run_pyinstrument_entrypoint()
   │     └─ ...
   │
   └─ tools_polars.py
      └─ register_polars_tools()
         ├─ @mcp.tool() polars_profile_parquet()  ← usa guard_path()
         └─ ...
```

---

## Fluxo de Segurança: Exemplo Prático

### Cenário: Agente tenta SQL malicioso

```python
# Agente tenta:
oracle_readonly_query("DELETE FROM defn.tab_movimentacao")

# Fluxo:
1. tools_oracle.py recebe a string
2. Chama guard_sql("DELETE FROM defn.tab_movimentacao")
3. security.py verifica:
   - Não começa com SELECT/WITH? ❌
   - Tem palavra-chave bloqueada (DELETE)? ❌
4. Levanta SqlSecurityError
5. Agente recebe: {"ok": false, "error": "SQL bloqueado..."}
```

---

## Fluxo de Análise Recomendado

### Antes de qualquer mudança:

```
1. MEDIR (baseline)
   └─ run_pyinstrument_entrypoint("app.py")
   └─ run_pytest_benchmark()

2. DIAGNOSTICAR
   └─ detect_pyside_performance_smells()
   └─ oracle_explain_select("SELECT ...")
   └─ polars_validate_nulls("data.parquet")

3. DECIDIR
   └─ ask_polars: "Como fazer isso corretamente?"
```

### Depois de alterar:

```
1. VALIDAR
   └─ run_ruff()
   └─ run_pytest()

2. COMPARAR
   └─ run_pytest_benchmark()
   └─ ask_polars: "Isso está correto?"

3. ACEITAR OU REJEITAR
   ├─ Se melhorou: abrir PR
   └─ Se piorou: reverter e diagnosticar
```

---

## Contrato Esperado com Agentes

### O que agentes NÃO podem fazer:

❌ Executar comandos shell livres  
❌ Fazer SELECT * sem limite  
❌ Alterar dados no Oracle  
❌ Criar/droppar tabelas  
❌ Acessar arquivos fora de PROJECT_ROOT  
❌ Guardar secrets no código  

### O que agentes PODEM fazer:

✅ Chamar tools explícitas  
✅ Ler metadados Oracle  
✅ Rodar testes e lint  
✅ Medir performance  
✅ Validar schemas e dados  
✅ Comparar antes/depois  
✅ Sugerir refactoring (via PRs)  

---

## Evolução Esperada

### MVP (agora):
- ✅ 5 grupos de tools
- ✅ Segurança de SQL
- ✅ Modularização limpa
- ✅ Integração com ask_polars

### Curto prazo (semana):
- [ ] Tool para detectar anomalias em dados (outliers)
- [ ] Tool para sugerir índices Oracle baseado em EXPLAIN PLAN
- [ ] CI/CD que roda tools automaticamente antes de merge

### Médio prazo (mês):
- [ ] Dashboard que exibe relatório de qualidade de Parquet
- [ ] Integração com GitHub para criar issues automáticas
- [ ] Profiling de memória com tracemalloc

### Longo prazo:
- [ ] MCP compartilhado com backend para validação de pipeline
- [ ] Alert automático se invariantes (`id_agrupado`, `__qtd_decl_final_audit__`) estão ruins

---

## Comparação com Alternativas

| Abordagem | Segurança | Auditoria | Performance | Previsibilidade |
|---|---|---|---|---|
| Shell Livre | ❌ Baixa | ❌ Ruim | ❌ Pode travar | ❌ Imprevisível |
| **audit-pyside-perf-mcp** | ✅ Alta | ✅ Explícita | ✅ Limitada | ✅ Determinística |
| Oracle read-only usuário | ✅ Alta | ⚠️ Média | ✅ Limitada | ✅ Boa |
| ask_polars | N/A | ⚠️ Conselho | N/A | ✅ Determinística |

---

## Referências

- [MCP Protocol](https://modelcontextprotocol.io/)
- [FastMCP (Python SDK)](https://github.com/modelcontextprotocol/python-sdk)
- [OWASP SQL Injection](https://owasp.org/www-community/attacks/SQL_Injection)
- [NIST Secure Development Practices](https://csrc.nist.gov/publications/detail/sp/800-218/final)
