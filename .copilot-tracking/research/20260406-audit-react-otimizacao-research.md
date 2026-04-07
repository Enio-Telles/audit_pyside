# Research: Audit React — Otimização (Fase P2–P5)

**Data:** 2026-04-06
**Repositório:** Enio-Telles/audit_pyside · branch `master`
**Workspace:** `C:\Sistema_pysisde`

---

## 1. Estado Atual — O que já foi implementado

### P0 (concluído) ✅

| Item | Arquivo | Status |
|------|---------|--------|
| ParquetService singleton | `backend/routers/parquet.py` linha 14 | ✅ |
| SQL paginação real | `backend/routers/sql_query.py` — page, page_size, total_pages | ✅ |
| React.lazy + Suspense | `frontend/src/App.tsx` linhas 7–28, 91, 171 | ✅ |
| ConversaoTab paginação | `ConversaoTab.tsx` — ROWS_PER_PAGE=150, page, pagedRows | ✅ |
| LeftPanel estilos Tailwind | `LeftPanel.tsx` — 3 inline styles → Tailwind | ✅ |

### P1 (concluído) ✅

| Item | Arquivo | Status |
|------|---------|--------|
| sqlApi.execute com page | `frontend/src/api/client.ts` linha ~176 | ✅ |
| ConsultaSqlTab paginação | `ConsultaSqlTab.tsx` — SqlResult, page, botões paginação | ✅ |
| QueryClient gcTime + refetchOnWindowFocus | `App.tsx` linhas 32–40 | ✅ |
| enabled guards auditado | Todos corretos com !!selectedCnpj / !!selectedFile | ✅ |
| TableSkeleton | `frontend/src/components/ui/TableSkeleton.tsx` + DataTable usa | ✅ |
| usePipelineStatus hook | `frontend/src/hooks/usePipelineStatus.ts` + LeftPanel usa | ✅ |

---

## 2. O que NÃO foi implementado

### 2.1 Backend — Fase 1 (Pendente)

- **Metadata endpoint** — sem endpoint de metadados (total linhas, tipos, amostras, estatísticas)
  - `backend/routers/parquet.py` só tem `query_parquet` (linha 44)
- **Export endpoint** — exportação CSV/Excel do resultado filtrado completo
  - CSV atual em `ConsultaTab.tsx` linha 164 é client-side e exporta só a página atual
- **Cache invalidation** — sem lógica explícita de invalidação quando parquet muda
- **Medição de tempo** — sem telemetria por operação

### 2.2 Frontend — Fase 2 (Pendente)

- **@tanstack/react-virtual** — não instalado; DataTable renderiza toda página (200 linhas) no DOM
- **URL sync** — tab/cnpj/file apenas no Zustand, sem URLSearchParams; perde estado ao recarregar
- **Zustand store split** — AppStore monolítico mistura shell + consulta + pipeline + preferências
- **placeholderData no ConsultaSqlTab** — apenas ConsultaTab tem; ConsultaSqlTab não tem
- **Feedback diferenciado por ação** — carregando/salvando/exportando/processando não distinguidos

### 2.3 Frontend — Fase 3 (Tabelas — Pendente)

- **DataTable** — sem freeze/pin de colunas, sem density config, sem context menu, sem row detail drawer
- **ConversaoTab** — tabela 100% customizada (~700 linhas), não compartilha base com DataTable
- **Saved views** — sem persistência de view (filtros, ordem, densidade) além de larguras atuais

### 2.4 Frontend — Fase 4 (Análise — Pendente)

- **Summary cards** — sem linha acima da tabela com total/filtrado/somas/nulos
- **Estatísticas por coluna** — sem painel min/max/avg/nulos
- **Auto-highlight** — HighlightRule existe mas é manual; sem detecção nulos/duplicados/outliers
- **Multi-sort** — sem UI de multi-sort (TanStack Table já suporta internamente)
- **Filter presets** — não existe
- **XLSX export** — não existe (apenas CSV página atual)
- **Pivot view** — não existe

### 2.5 Frontend — Fase 5 (UX — Pendente)

- **Persistência inter-sessão** — último CNPJ/aba/arquivo perdidos ao fechar
- **Empty states** — apenas mensagens de texto, sem rich empty state
- **Keyboard shortcuts** — não existem
- **Toolbar padronizada** — cada tab tem padrão próprio

### 2.6 Qualidade/CI — Fase 6 (Pendente)

- **Testes frontend** — setupTests.ts existe mas sem testes relevantes
- **Testes backend** — sem testes de rotas paginadas
- **CI** — sem .github/workflows/ci.yml
- **Docs** — README padrão Vite; sem docs/arquitetura.md

---

## 3. Dependências a instalar

| Pacote | Uso | Fase |
|--------|-----|------|
| `@tanstack/react-virtual` | Virtualização de linhas no DataTable | P2 |
| `xlsx` | Exportação XLSX | P3 |
| `recharts` | Mini gráficos por coluna | P5 |

---

## 4. Arquivos-chave

| Arquivo | Linhas relevantes |
|---------|------------------|
| `frontend/src/App.tsx` | 7–28 lazy; 32–40 QueryClient |
| `frontend/src/store/appStore.ts` | 11–67 AppStore interface |
| `frontend/src/api/client.ts` | ~176 sqlApi; ~155 parquetApi |
| `frontend/src/components/table/DataTable.tsx` | 17–45 props; 354 TableSkeleton |
| `frontend/src/components/tabs/ConsultaTab.tsx` | 83 placeholderData; 164 CSV export |
| `frontend/src/components/tabs/ConsultaSqlTab.tsx` | 8 SqlResult; 97 pagination |
| `frontend/src/components/tabs/ConversaoTab.tsx` | 16 ROWS_PER_PAGE; 658 paginator |
| `backend/routers/parquet.py` | 14 singleton; 44 query_parquet |
| `backend/routers/sql_query.py` | 30 SqlRequest; 49 total_pages |

---

## 5. Priorização P2–P5

### P2 — Alto impacto, menor esforço (Sprint 3)
1. `@tanstack/react-virtual` no DataTable
2. Summary cards no ConsultaTab (total_rows, filtrado, selecionados, nulos-chave)
3. `/parquet/metadata` endpoint no backend
4. Endpoint `/parquet/export-csv` para exportação filtrada completa
5. `placeholderData` no ConsultaSqlTab

### P3 — Impacto alto, esforço médio (Sprint 4)
6. Density configuration no DataTable (compact/normal/comfortable)
7. Row detail drawer (slide-in para inspecionar linha)
8. URL sync com URLSearchParams (tab, cnpj, file)
9. Persistência inter-sessão (localStorage: último CNPJ + lastFile)
10. XLSX export (instalar xlsx)

### P4 — Valor analítico (Sprint 5)
11. Column statistics panel (min/max/avg/nulos por coluna)
12. ConversaoTab migração para DataTable base
13. Zustand store split (appShellStore, consultaStore, pipelineStore)
14. Multi-sort na ConsultaTab
15. Filter presets ("somente nulos", "somente divergentes")

### P5 — Features avançadas (Sprint 6)
16. Auto-highlight (nulos, outliers, duplicados)
17. Mini gráficos por coluna (recharts)
18. Pivot view simplificado
19. CI básico (.github/workflows/ci.yml)
20. Test suite mínimo
