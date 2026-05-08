# Plano de Otimização — Fiscal Parquet Analyzer

> **Data:** 07/04/2026
> **Objetivo:** Melhorar performance, manutenibilidade e confiabilidade do projeto
> **Escopo:** ETL Python/Polars, Backend FastAPI, Frontend React, Infraestrutura

---

## Sumário Executivo

| Categoria | Oportunidades | Impacto Estimado |
|---|---|---|
| **Performance ETL** | 8 itens | ⬆️ 30-50% mais rápido |
| **Código Duplicado** | 10 itens | ⬇️ -25% linhas de código |
| **Estabilidade Backend** | 4 itens | 🔒 Zero crashes em produção |
| **Frontend** | 5 itens | ⬆️ UX mais fluida |
| **Infraestrutura/DevOps** | 6 itens | 🚀 Deploy confiável |
| **Qualidade de Código** | 8 itens | ✅ Menos bugs |

**Total: 41 itens de otimização**

---

## 📋 Lista de Todos (Master Todo List)

### P0 — Crítico (Semana 1-2)

- [ ] **T01:** Substituir `sys.exit(1)` por `RuntimeError` em módulos ETL importáveis
- [ ] **T02:** Criar `backend/routers/_common.py` com funções compartilhadas
- [ ] **T03:** Resolver concorrência em `patch_fatores_conversao` (file locking)
- [ ] **T04:** Substituir `map_elements` por operações vetorizadas em `calculos_mensais.py`
- [ ] **T05:** Substituir `map_elements` por operações vetorizadas em `calculos_anuais.py`

### P1 — Alto Impacto (Semana 3-4)

- [ ] **T06:** Unificar normalização de descrição em `src/utilitarios/text.py`
- [ ] **T07:** Criar `src/utilitarios/polars_utils.py` com expressões Polars reutilizáveis
- [ ] **T08:** Migrar `read_parquet` para `scan_parquet` em módulos de movimentação
- [ ] **T09:** Remover acoplamento backend → `interface_grafica`
- [ ] **T10:** Substituir `map_elements` em `fatores_conversao.py` (reconciliação)
- [ ] **T11:** Otimizar cálculo de similaridade de descrição em `c170_xml.py`
- [ ] **T12:** Adicionar validação de CNPJ (14 dígitos) em todos os routers

### P2 — Médio Impacto (Semana 5-6)

- [ ] **T13:** Consolidar proxies de compatibilidade em `src/transformacao/`
- [ ] **T14:** Criar cache para tabelas de referência (CO_SEFIN, CFOP)
- [ ] **T15:** Mover `map_estoque.json` para `dados/referencias/`
- [ ] **T16:** Adicionar type hints em utilitários sem tipagem
- [ ] **T17:** Padronizar tratamento de erros com middleware FastAPI
- [ ] **T18:** Refatorar `fatores_conversao.py` em sub-módulos
- [ ] **T19:** Substituir `print` por `logging` em `salvar_para_parquet.py`
- [ ] **T20:** Otimizar `to_dicts()` nos routers do backend

### P3 — Melhorias (Semana 7-8)

- [ ] **T21:** Remover Oracle host hardcoded do fallback
- [ ] **T22:** Adicionar `.env.example` ao projeto
- [ ] **T23:** Criar Dockerfile para backend
- [ ] **T24:** Criar Dockerfile para frontend
- [ ] **T25:** Configurar CI com GitHub Actions
- [ ] **T26:** Adicionar métricas de performance no pipeline
- [ ] **T27:** Criar dashboard de fallback de preço de venda
- [ ] **T28:** Virtualizar listas longas no frontend (TanStack Virtual)
- [ ] **T29:** Adicionar retry logic no frontend para queries Oracle
- [ ] **T30:** Otimizar `map_groups` em `calculo_saldos.py`

### P4 — Cleanup (Semana 9+)

- [ ] **T31:** Limpar scripts avulsos na raiz do projeto
- [ ] **T32:** Consolidar logs de validação de schema
- [ ] **T33:** Adicionar testes de integração E2E
- [ ] **T34:** Documentar API com OpenAPI/Swagger avançado
- [ ] **T35:** Adicionar rate limiting nos routers
- [ ] **T36:** Criar script de migração de banco de dados (se necessário)
- [ ] **T37:** Adicionar health checks para Oracle e filesystem
- [ ] **T38:** Criar pipeline de dados de teste automatizado
- [ ] **T39:** Otimizar bundle size do frontend
- [ ] **T40:** Adicionar cache HTTP no frontend (TanStack Query staleTime)
- [ ] **T41:** Criar sistema de notificações para falhas de pipeline

---

## 🔍 Detalhamento por Item

---

### T01: Substituir `sys.exit(1)` por `RuntimeError`

**Status:** 🔴 Crítico
**Local:** Múltiplos módulos ETL
**Impacto:** Backend crasha ao importar módulos

**Problema:**
```python
# Atual (em tabela_documentos.py, fontes_produtos.py, etc.)
try:
    from utilitarios.conectar_oracle import conectar
except ImportError:
    print("Erro ao conectar ao Oracle")
    sys.exit(1)  # ❌ Mata o processo inteiro
```

**Solução:**
```python
# Proposto
try:
    from utilitarios.conectar_oracle import conectar
except ImportError as e:
    raise RuntimeError("Falha ao importar módulo Oracle") from e
```

**Arquivos afetados:**
- `src/transformacao/tabela_documentos.py`
- `src/transformacao/fontes_produtos.py`
- `src/transformacao/fatores_conversao.py`
- `src/transformacao/calculos_mensais.py`
- `src/transformacao/calculos_anuais.py`
- `src/transformacao/c170_xml.py`
- `src/transformacao/c176_xml.py`
- `src/transformacao/movimentacao_estoque.py`
- `src/transformacao/rastreabilidade_produtos/03_descricao_produtos.py`
- `src/transformacao/rastreabilidade_produtos/04_produtos_final.py`

**Estimativa:** 2 horas

---

### T02: Criar `backend/routers/_common.py`

**Status:** 🔴 Crítico
**Local:** `backend/routers/`
**Impacto:** -30% código duplicado nos routers

**Problema:** 4 routers têm implementações idênticas de `_sanitize`, `_safe_value`, `_df_to_response`.

**Solução:** Criar módulo compartilhado:

```python
# backend/routers/_common.py
from __future__ import annotations
import re
import math
from pathlib import Path
from fastapi import HTTPException

def sanitize_cnpj(cnpj: str) -> str:
    limpo = re.sub(r"\D", "", cnpj)
    if len(limpo) != 14:
        raise HTTPException(status_code=400, detail=f"CNPJ inválido: {cnpj}")
    return limpo

def safe_value(val):
    if val is None or (isinstance(val, float) and (math.isnan(val) or math.isinf(val))):
        return None
    return val

def df_to_response(df, page: int, page_size: int, colunas: list[str] | None = None):
    """Padroniza paginação e serialização de DataFrames."""
    # ... implementação única
```

**Arquivos afetados:**
- `backend/routers/pipeline.py`
- `backend/routers/aggregation.py`
- `backend/routers/estoque.py`
- `backend/routers/ressarcimento.py`
- `backend/routers/parquet.py`

**Estimativa:** 4 horas

---

### T03: Resolver concorrência em `patch_fatores_conversao`

**Status:** 🔴 Crítico
**Local:** `backend/routers/estoque.py`
**Impacto:** Corrupção de dados em requisições simultâneas

**Problema:**
```python
# Dois requests simultâneos leem, modificam e escrevem o mesmo arquivo
df = pl.read_parquet(path)
df = df.with_columns(...)
df.write_parquet(path)  # ❌ Race condition
```

**Solução:** Usar file locking por CNPJ:

```python
import filelock

def patch_fatores_conversao(cnpj: str, ...):
    lock_path = Path(f"/tmp/lock_{cnpj}.lock")
    with filelock.FileLock(lock_path, timeout=30):
        df = pl.read_parquet(path)
        df = df.with_columns(...)
        df.write_parquet(path)
```

**Estimativa:** 2 horas

---

### T04/T05: Vetorizar `map_elements` em cálculos mensais/anuais

**Status:** 🟡 Alto Impacto
**Local:** `calculos_mensais.py`, `calculos_anuais.py`
**Impacto:** 3-5x mais rápido em datasets grandes

**Problema:**
```python
# Atual — loop Python puro
df = df.with_columns(
    pl.struct("mes").map_elements(
        lambda registro: date(registro["mes"] // 100, registro["mes"] % 100, 1),
        return_dtype=pl.Date
    )
)
```

**Solução:** Operações vetorizadas:

```python
# Proposto — Polars nativo
df = df.with_columns([
    (pl.col("mes") // 100).cast(pl.Int32).alias("ano"),
    (pl.col("mes") % 100).cast(pl.Int32).alias("mes_dia"),
])
df = df.with_columns(
    pl.date(pl.col("ano"), pl.col("mes_dia"), pl.lit(1)).alias("data_inicio")
)
```

**Estimativa:** 4 horas cada

---

### T06: Unificar normalização de descrição

**Status:** 🟡 Alto Impacto
**Local:** 7+ arquivos
**Impacto:** -200 linhas duplicadas

**Problema:** 7 implementações quase idênticas de `_normalizar_descricao_expr`:

| Arquivo | Nome da função |
|---|---|
| `tabela_documentos.py` | inline |
| `rastreabilidade_produtos/03_descricao_produtos.py` | `_normalizar_descricao_expr` |
| `rastreabilidade_produtos/fatores_conversao.py` | `_normalizar_descricao_expr` |
| `movimentacao_estoque_pkg/mapeamento_fontes.py` | `normalizar_descricao_expr` |
| `c170_xml.py` | `_norm_text_expr` |
| `c176_xml.py` | `_norm_text_expr` |
| `rastreabilidade_produtos/fontes_produtos.py` | `_normalizar_descricao_expr` |

**Solução:** Criar `src/utilitarios/text.py`:

```python
def normalizar_descricao_expr(col_name: str, alias: str | None = None) -> pl.Expr:
    """Expressão Polars para normalizar descrições (remove acentos, lowercase, etc.)"""
    col = pl.col(col_name)
    alias_name = alias or f"{col_name}_normalizado"

    return (
        col.cast(pl.Utf8)
        .str.to_lowercase()
        .str.replace_all(r"[áàãâä]", "a")
        .str.replace_all(r"[éèêë]", "e")
        .str.replace_all(r"[íìîï]", "i")
        .str.replace_all(r"[óòõôö]", "o")
        .str.replace_all(r"[úùûü]", "u")
        .str.replace_all(r"[ç]", "c")
        .str.replace_all(r"[^a-z09\s\-/]", " ")
        .str.replace_all(r"\s+", " ")
        .str.strip_chars()
        .alias(alias_name)
    )
```

**Estimativa:** 3 horas

---

### T07: Criar `src/utilitarios/polars_utils.py`

**Status:** 🟡 Alto Impacto
**Local:** Novo arquivo
**Impacto:** -150 linhas duplicadas, consistência

**Funções a incluir:**
- `normalizar_descricao_expr(col, alias)`
- `boolish_expr(col)` — unificar 3 implementações
- `to_float_expr(col, alias)`
- `clean_digits_expr(col, alias)`
- `to_int_expr(col, alias)`
- `sanitize_cnpj(cnpj)` — unificar 4 implementações
- `safe_value(val)` — para NaN/Inf

**Estimativa:** 4 horas

---

### T08: Migrar `read_parquet` para `scan_parquet`

**Status:** 🟡 Alto Impacto
**Local:** 8+ arquivos
**Impacto:** -60% memória em datasets grandes

**Problema:** `read_parquet` carrega tudo na memória antes de filtrar.

**Solução:**
```python
# Antes
df = pl.read_parquet(caminho)
df = df.filter(pl.col("cnpj") == cnpj_alvo)

# Depois
lf = pl.scan_parquet(caminho)
lf = lf.filter(pl.col("cnpj") == cnpj_alvo)
df = lf.collect()  # Só após filtros
```

**Arquivos afetados:**
- `src/transformacao/tabelas_base/tabela_documentos.py`
- `src/transformacao/rastreabilidade_produtos/fatores_conversao.py`
- `src/transformacao/movimentacao_estoque_pkg/movimentacao_estoque.py`
- `src/transformacao/movimentacao_estoque_pkg/c170_xml.py`
- `src/transformacao/movimentacao_estoque_pkg/c176_xml.py`
- `src/transformacao/calculos_mensais.py`
- `src/transformacao/calculos_anuais.py`
- `src/transformacao/movimentacao_estoque.py`

**Estimativa:** 6 horas

---

### T09: Remover acoplamento backend → interface_grafica

**Status:** 🟡 Alto Impacto
**Local:** `backend/routers/pipeline.py`
**Impacto:** Arquitetura limpa, testes independentes

**Problema:**
```python
# backend/routers/pipeline.py importa da UI desktop ❌
from interface_grafica.services.registry_service import RegistryService
from interface_grafica.services.pipeline_funcoes_service import ServicoPipelineCompleto
from interface_grafica.config import CNPJ_ROOT
```

**Solução:**
1. Criar `backend/services/` com versões agnósticas dos serviços
2. Ou mover serviços compartilhados para `src/services/` (nível acima)
3. Ambos (UI e backend) importam de `src/services/`

**Estimativa:** 6 horas

---

### T10: Vetorizar `map_elements` em `fatores_conversao.py`

**Status:** 🟡 Alto Impacto
**Local:** `rastreabilidade_produtos/fatores_conversao.py`
**Impacto:** 2-3x mais rápido em reconciliação

**Problema:** Múltiplos `map_elements` para normalização e reconciliação de overrides.

**Solução:** Reescrever usando `.str.*` nativos e joins em vez de loops.

**Estimativa:** 6 horas

---

### T11: Otimizar similaridade de descrição em `c170_xml.py`

**Status:** 🟡 Alto Impacto
**Local:** `movimentacao_estoque_pkg/c170_xml.py`
**Impacto:** Evita OOM em notas com muitos itens

**Problema:** Produto cartesiano C170 × XML itens × similaridade Python.

**Solução:**
1. Filtrar candidatos por código NCM antes do join
2. Usar `str.similarity` do Polars (se disponível) ou implementar em Rust
3. Limitar candidatos por nota (top-N por similaridade inicial)

**Estimativa:** 8 horas

---

### T12: Validação de CNPJ em todos os routers

**Status:** 🟡 Alto Impacto
**Local:** Todos os routers
**Impacto:** Erros 400 em vez de 500 silenciosos

**Solução:** Usar `sanitize_cnpj()` do `_common.py` (T02) com validação:

```python
@router.post("/estoque")
def get_estoque(cnpj: str):
    cnpj_limpo = sanitize_cnpj(cnpj)  # Raises 400 se inválido
    # ... resto da lógica
```

**Estimativa:** 2 horas (aproveita T02)

---

### T13: Consolidar proxies de compatibilidade

**Status:** 🟢 Médio
**Local:** `src/transformacao/*.py`
**Impacto:** Robustez, clareza

**Problema:** Proxies usam `importlib.util.spec_from_file_location` (frágil).

**Solução:** Converter para imports diretos ou reexportar explicitamente:

```python
# Em vez de importlib dinâmico
from transformacao.tabelas_base.01_item_unidades import gerar_item_unidades as _gen

def gerar_item_unidades(cnpj: str) -> bool:
    return _gen(cnpj)
```

**Estimativa:** 3 horas

---

### T14: Cache para tabelas de referência

**Status:** 🟢 Médio
**Local:** Múltiplos módulos
**Impacto:** -30% I/O em pipelines multi-CNPJ

**Solução:** Singleton com `functools.lru_cache` ou cache em módulo:

```python
from functools import cache

@cache
def carregar_co_sefin() -> pl.DataFrame:
    return pl.read_parquet(caminho_referencia)
```

**Estimativa:** 3 horas

---

### T15: Mover `map_estoque.json`

**Status:** 🟢 Médio
**Local:** Raiz → `dados/referencias/`
**Impacto:** Organização

**Estimativa:** 30 minutos

---

### T16: Adicionar type hints

**Status:** 🟢 Médio
**Local:** Utilitários sem tipagem
**Impacto:** Melhor IDE, menos bugs

**Arquivos:**
- `src/utilitarios/salvar_para_parquet.py`
- `src/utilitarios/conectar_oracle.py`
- `src/utilitarios/ler_sql.py`
- `src/transformacao/co_sefin_class.py`

**Estimativa:** 4 horas

---

### T17: Middleware de erros FastAPI

**Status:** 🟢 Médio
**Local:** `backend/main.py`
**Impacto:** Respostas de erro padronizadas

**Solução:**
```python
@app.exception_handler(Exception)
async def global_exception_handler(request, exc):
    logger.error(f"Erro não tratado: {exc}", exc_info=True)
    return JSONResponse(
        status_code=500,
        content={"detail": "Erro interno do servidor", "trace_id": generate_trace_id()}
    )
```

**Estimativa:** 2 horas

---

### T18: Refatorar `fatores_conversao.py`

**Status:** 🟢 Médio
**Local:** `rastreabilidade_produtos/fatores_conversao.py` (777 linhas)
**Impacto:** Manutenibilidade

**Estrutura proposta:**
```
fatores_conversao/
├── __init__.py
├── calculo.py        # Lógica pura de cálculo
├── reconciliacao.py  # Overrides e reconciliação
├── io.py             # Leitura/gravação
└── validacoes.py     # Schema e integridade
```

**Estimativa:** 8 horas

---

### T19: Substituir `print` por `logging`

**Status:** 🟢 Médio
**Local:** `salvar_para_parquet.py`, módulos diversos
**Impacto:** Logs estruturados, debugging

**Estimativa:** 2 horas

---

### T20: Otimizar `to_dicts()` nos routers

**Status:** 🟢 Médio
**Local:** Todos os routers
**Impacto:** -40% tempo de serialização

**Problema:**
```python
result = [{col: _safe_value(row[col]) for col in cols} for row in df.to_dicts()]
```

**Solução:**
```python
# Usar write_json do Polars + parse, ou serialização otimizada
import json
result = json.loads(df.write_json())
```

**Estimativa:** 3 horas

---

### T21: Remover Oracle host hardcoded

**Status:** 🔵 Baixo
**Local:** `src/utilitarios/conectar_oracle.py`
**Impacto:** Segurança

**Solução:** Exigir variável de ambiente, sem fallback:

```python
HOST = os.getenv("ORACLE_HOST")
if not HOST:
    raise ValueError("ORACLE_HOST não configurado")
```

**Estimativa:** 1 hora

---

### T22: Adicionar `.env.example`

**Status:** 🔵 Baixo
**Local:** Raiz do projeto
**Impacto:** Onboarding

**Conteúdo:**
```env
ORACLE_HOST=
ORACLE_PORT=1521
ORACLE_SERVICE=
ORACLE_USER=
ORACLE_PASSWORD=
```

**Estimativa:** 30 minutos

---

### T23/T24: Criar Dockerfiles

**Status:** 🔵 Baixo
**Local:** `backend/Dockerfile`, `frontend/Dockerfile`
**Impacto:** Deploy consistente

**Estimativa:** 4 horas

---

### T25: Configurar CI com GitHub Actions

**Status:** 🔵 Baixo
**Local:** `.github/workflows/ci.yml`
**Impacto:** Qualidade automática

**Pipeline:**
```yaml
name: CI
on: [push, pull_request]
jobs:
  test-backend:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - run: pip install -r requirements.txt
      - run: pytest tests/
  test-frontend:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - run: cd frontend && pnpm install && pnpm test && pnpm lint
```

**Estimativa:** 3 horas

---

### T26: Métricas de performance no pipeline

**Status:** 🔵 Baixo
**Local:** `src/utilitarios/perf_monitor.py`
**Impacto:** Visibilidade

**Solução:** Salvar métricas em JSON por execução:

```json
{
  "cnpj": "12345678901234",
  "etapa": "fatores_conversao",
  "duracao_seg": 45.2,
  "memoria_mb": 512,
  "linhas_processadas": 150000,
  "timestamp": "2026-04-07T10:00:00Z"
}
```

**Estimativa:** 4 horas

---

### T27: Dashboard de fallback de preço

**Status:** 🔵 Baixo
**Local:** Frontend + Backend
**Impacto:** Visibilidade de dados faltantes

**Solução:** Endpoint `/api/parquet/fallbacks/{cnpj}` + tab no frontend.

**Estimativa:** 6 horas

---

### T28: Virtualizar listas longas

**Status:** 🔵 Baixo
**Local:** Frontend (`DataTable.tsx`)
**Impacto:** Performance em tabelas >10k linhas

**Solução:** `@tanstack/react-virtual` para renderização virtualizada.

**Estimativa:** 4 horas

---

### T29: Retry logic no frontend

**Status:** 🔵 Baixo
**Local:** `frontend/src/api/client.ts`
**Impacto:** Resiliência

**Solução:**
```typescript
import axiosRetry from 'axios-retry';

axiosRetry(apiClient, {
  retries: 3,
  retryDelay: axiosRetry.exponentialDelay,
  retryCondition: (error) => error.code === 'ECONNRESET'
});
```

**Estimativa:** 2 horas

---

### T30: Otimizar `map_groups` em `calculo_saldos.py`

**Status:** 🔵 Baixo
**Local:** `movimentacao_estoque_pkg/calculo_saldos.py`
**Impacto:** Performance

**Solução:** Explorar `cum_sum` com correções condicionais:

```python
df = df.with_columns([
    pl.col("entrada").cum_sum().over("produto").alias("acum_entrada"),
    pl.col("saida").cum_sum().over("produto").alias("acum_saida"),
])
df = df.with_columns(
    (pl.col("acum_entrada") - pl.col("acum_saida")).alias("saldo")
)
```

**Estimativa:** 6 horas

---

### T31-T41: Cleanup e melhorias finais

| Item | Descrição | Estimativa |
|---|---|---|
| **T31** | Limpar scripts avulsos na raiz | 2h |
| **T32** | Consolidar logs de validação | 2h |
| **T33** | Testes E2E | 8h |
| **T34** | OpenAPI avançado | 3h |
| **T35** | Rate limiting | 2h |
| **T36** | Migração DB (se necessário) | Variável |
| **T37** | Health checks | 2h |
| **T38** | Pipeline dados teste | 4h |
| **T39** | Otimizar bundle frontend | 3h |
| **T40** | Cache TanStack Query | 2h |
| **T41** | Notificações falhas pipeline | 6h |

---

## 📊 Métricas de Sucesso

| Métrica | Atual | Meta |
|---|---|---|
| Tempo pipeline completo | ~15 min | <10 min |
| Memória peak (ETL) | ~2 GB | <1.2 GB |
| Código duplicado | ~15% | <5% |
| Test coverage | ~60% | >80% |
| Lighthouse frontend | ~75 | >90 |
| Erros 500/mês | ~20 | <5 |

---

## 🗺️ Roadmap Sugerido

### Sprint 1 (Semana 1-2) — Estabilidade
- T01, T02, T03, T04, T05
- **Objetivo:** Backend estável, sem crashes

### Sprint 2 (Semana 3-4) — Performance ETL
- T06, T07, T08, T09, T10, T11, T12
- **Objetivo:** ETL 2x mais rápido, código limpo

### Sprint 3 (Semana 5-6) — Qualidade
- T13, T14, T15, T16, T17, T18, T19, T20
- **Objetivo:** Código tipado, logs estruturados

### Sprint 4 (Semana 7-8) — Infraestrutura
- T21, T22, T23, T24, T25, T26, T27
- **Objetivo:** Deploy confiável, métricas

### Sprint 5 (Semana 9+) — Refinamento
- T28, T29, T30, T31-T41
- **Objetivo:** UX polida, cleanup

---

## 📁 Referências

- [AGENTS.md](./AGENTS.md) — Guia operacional do projeto
- [FRONTEND.md](./FRONTEND.md) — Documentação frontend
- [docs/PLAN.md](./docs/PLAN.md) — Planos existentes

---

> **Nota:** Este plano é vivo. Atualize conforme novas oportunidades são identificadas.
