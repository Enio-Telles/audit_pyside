# Plano de Otimização — Fiscal Parquet Analyzer

**Data:** 14 de abril de 2026  
**Objetivo:** Melhorar performance, qualidade e manutenibilidade do código

---

## Estratégia de Execução

As otimizações estão organizadas por prioridade:
- **P0 (Crítico):** Impacta estabilidade ou integridade dos dados — executar imediatamente
- **P1 (Alto):** Melhora significativa de arquitetura ou DX — executar na sequência
- **P2 (Médio):** Boas práticas e organização — executar quando possível
- **P3 (Baixo):** Otimizações avançadas — planejar para futuro

---

## P0 — Crítico

### T01: Substituir `sys.exit(1)` por `RuntimeError`
**Problema:** Módulos ETL importáveis chamam `sys.exit(1)` quando imports falham, matando o processo FastAPI.

**Arquivos afetados:**
- `src/transformacao/calculos_mensais_pkg/calculos_mensais.py` (linhas 17-20)
- `src/transformacao/calculos_anuais_pkg/calculos_anuais.py` (linhas 17-20)
- `src/transformacao/rastreabilidade_produtos/fatores_conversao.py` (linhas 29-32)

**Solução:** Substituir por `raise RuntimeError(...)` ou `logger.error()` + return early.

**Impacto:** Evita crash do backend em produção  
**Esforço:** 2h

---

### T02: Criar `backend/routers/_common.py`
**Problema:** 4 routers replicam as mesmas funções `_sanitize`, `_safe_value`, `_df_to_response`, `_resposta_vazia`.

**Arquivos afetados:**
- `backend/routers/estoque.py`
- `backend/routers/aggregation.py`
- `backend/routers/parquet.py`
- `backend/routers/ressarcimento.py`

**Solução:** Extrair funções compartilhadas para `_common.py` e importar nos routers.

**Impacto:** -30% código duplicado, manutenção centralizada  
**Esforço:** 4h

---

### T03: File locking em `patch_fatores_conversao`
**Problema:** Race condition em operações read-modify-write concorrentes para o mesmo CNPJ.

**Arquivo afetado:** `backend/routers/estoque.py` (~linhas 310-340)

**Solução:** Usar `filelock.FileLock` para serializar acesso por CNPJ.

**Impacto:** Evita perda de dados em concorrência  
**Esforço:** 2h

---

## P1 — Alto

### T04: Consolidar funções duplicadas
**Problema:** `_boolish_expr` e `_resolver_ref` duplicados em múltiplos módulos.

**Arquivos afetados:**
- `src/transformacao/calculos_mensais_pkg/calculos_mensais.py`
- `src/transformacao/calculos_anuais_pkg/calculos_anuais.py`

**Solução:** Mover para `src/utilitarios/` e importar.

**Impacto:** -50 linhas duplicadas, consistência  
**Esforço:** 2h

---

### T05: Validação de CNPJ em todos os routers
**Problema:** Routers não validam CNPJ antes de processar, gerando erros 500 em vez de 400.

**Solução:** Criar middleware/decorator de validação em `_common.py`.

**Impacto:** Melhor UX e debugging  
**Esforço:** 2h

---

## P2 — Médio

### T06: Substituir `print` por `logging`
**Problema:** `src/utilitarios/salvar_para_parquet.py` usa `print` para logs.

**Solução:** Usar módulo `src/transformacao/auxiliares/logs.py`.

**Impacto:** Logs estruturados e auditáveis  
**Esforço:** 2h

---

### T07: Criar `.env.example` e atualizar `requirements.txt`
**Problema:** Falta configuração de ambiente e dependencies desatualizadas.

**Solução:**
- Criar `.env.example` com variáveis Oracle
- Atualizar `requirements.txt` (remover pandas, adicionar uvicorn, filelock)

**Impacto:** Melhor onboarding e deps corretas  
**Esforço:** 1h

---

## Critérios de Aceite

- [ ] Todos os testes `pytest` passam
- [ ] `tsc --noEmit` sem erros
- [ ] `pnpm lint` sem erros
- [ ] Backend inicia sem crashes
- [ ] Nenhum `sys.exit()` em módulos importáveis
- [ ] Code coverage mantido ou melhorado

---

## Métricas de Sucesso

| Métrica | Antes | Depois (Alvo) |
|---|---|---|
| Linhas duplicadas nos routers | ~400 | <100 |
| Crashes do backend por imports | Sim | Não |
| Race conditions em patches | Sim | Não |
| Uso de print vs logging | 50/50 | 100% logging |
| Requisitos desatualizados | Sim | Atualizados |

---

## Riscos e Mitigação

| Risco | Mitigação |
|---|---|
| Quebra de compatibilidade | Testes antes/depois |
| Regressão de performance | Benchmark pré/pós |
| Complexidade excessiva | Manter soluções simples |

---

**Status:** Criado em 14/04/2026  
**Próxima revisão:** Após implementação das tarefas P0
