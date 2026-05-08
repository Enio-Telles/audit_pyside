# 01 — Decisões Operacionais (D1–D11)

Este documento centraliza as decisões arquiteturais e operacionais validadas para o projeto.

| ID | Decisão | Status | Contexto |
|---|---|---|---|
| **D1** | Propriedade de Performance | ✅ Resolvido | Performance é responsabilidade transversal, validada via benchmarks em `bench/`. |
| **D2** | Métricas de Baseline | ✅ Resolvido | KPIs definidos: TTFP, Delta-RSS, p95 Page Change, Filter Time, Export Time. |
| **D3** | Ferramenta de Benchmark | ✅ Resolvido | Uso de scripts customizados em `bench/` e logs de performance. |
| **D4** | Política de Fixtures | ✅ Resolvido | **Híbrida:** 100% sintético para CI; dados reais anonimizados para benchmarks locais. |
| **D5** | Threshold de Arquivo Grande | ✅ Resolvido | Definido em **512 MB**. Acima disso, o roteador prefere DuckDB. |
| **D6** | MCP DuckDB | ✅ Resolvido | Servidor MCP para agentes explorarem Parquets via SQL, priorizado para 2026. |
| **D7** | Backend Routing | ✅ Resolvido | `ParquetQueryService` decide entre Polars (memória) e DuckDB (lazy/pushdown). |
| **D8** | Cleanup da Raiz | ✅ Resolvido | Higiene estrutural (Onda 0) aprovada para reduzir ruído na raiz. |
| **D9** | Entrypoint Único | ✅ Resolvido | `app.py` é o único lançador. `app_safe.py` foi removido (ADR-0003). |
| **D10** | Pasta de Benchmarks | ✅ Resolvido | `bench/` é a pasta canônica. `benchmarks/` foi consolidada nela. |
| **D11** | Lifecycle de Branches | ✅ Resolvido | Limpeza automática de branches stale integrada ao CI. |

---

## Histórico de Aprovações
- **D1-D7:** Aprovadas durante a definição do plano de performance (2026-05).
- **D8-D11:** Aprovadas no plano de limpeza da Onda 0 (2026-05-05).
