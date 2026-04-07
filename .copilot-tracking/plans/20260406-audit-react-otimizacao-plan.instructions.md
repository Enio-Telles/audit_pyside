---
applyTo: ".copilot-tracking/changes/20260406-audit-react-otimizacao-changes.md"
---

<!-- markdownlint-disable-file -->

# Task Checklist: Audit React - Otimizacao P2-P5

## Overview

Implementar as fases P2-P5 do plano de melhorias do audit_react: virtualizacao
de renderizacao, visualizacao analitica, UX, persistencia e qualidade de codigo.

## Objetivos

- Virtualizar linhas no DataTable (@tanstack/react-virtual)
- Summary cards acima das tabelas com total/filtrado/selecionados/nulos
- Endpoints de metadata e exportacao filtrada no backend
- Density config e row detail drawer no DataTable
- URL sync e persistencia inter-sessao (localStorage)
- XLSX export e exportacao filtrada completa
- Zustand store split por dominio
- Multi-sort, filter presets, auto-highlight
- CI basico e test suite minimo

## Research Summary

### Project Files

- frontend/src/components/table/DataTable.tsx - props linhas 17-45; TableSkeleton linha 354
- frontend/src/components/tabs/ConsultaTab.tsx - export CSV linha 164; placeholderData linha 83
- frontend/src/components/tabs/ConversaoTab.tsx - tabela customizada; paginacao linha 658
- frontend/src/store/appStore.ts - AppStore monolitico interface linhas 11-67
- backend/routers/parquet.py - singleton linha 14; query_parquet linha 44
- frontend/src/api/client.ts - parquetApi linha ~155; sqlApi linha ~176

### External References

- #file:../research/20260406-audit-react-otimizacao-research.md - Auditoria completa e priorizacao P2-P5

## Implementation Checklist

### [ ] Fase P2: Performance de renderizacao e backend essencial

- [ ] Tarefa P2.1: Instalar e integrar @tanstack/react-virtual no DataTable
  - Details: .copilot-tracking/details/20260406-audit-react-otimizacao-details.md (Linhas 10-55)

- [ ] Tarefa P2.2: Criar endpoint /parquet/metadata no backend
  - Details: .copilot-tracking/details/20260406-audit-react-otimizacao-details.md (Linhas 57-100)

- [ ] Tarefa P2.3: Criar endpoint /parquet/export-csv no backend
  - Details: .copilot-tracking/details/20260406-audit-react-otimizacao-details.md (Linhas 102-135)

- [ ] Tarefa P2.4: Summary cards no ConsultaTab
  - Details: .copilot-tracking/details/20260406-audit-react-otimizacao-details.md (Linhas 137-180)

- [ ] Tarefa P2.5: placeholderData e useQuery no ConsultaSqlTab
  - Details: .copilot-tracking/details/20260406-audit-react-otimizacao-details.md (Linhas 182-200)

### [ ] Fase P3: DataTable evolution e persistencia

- [ ] Tarefa P3.1: Density configuration no DataTable
  - Details: .copilot-tracking/details/20260406-audit-react-otimizacao-details.md (Linhas 203-250)

- [ ] Tarefa P3.2: Row detail drawer no DataTable
  - Details: .copilot-tracking/details/20260406-audit-react-otimizacao-details.md (Linhas 252-295)

- [ ] Tarefa P3.3: URL sync com URLSearchParams
  - Details: .copilot-tracking/details/20260406-audit-react-otimizacao-details.md (Linhas 297-335)

- [ ] Tarefa P3.4: Persistencia inter-sessao com localStorage
  - Details: .copilot-tracking/details/20260406-audit-react-otimizacao-details.md (Linhas 337-365)

- [ ] Tarefa P3.5: XLSX export
  - Details: .copilot-tracking/details/20260406-audit-react-otimizacao-details.md (Linhas 367-410)

### [ ] Fase P4: Valor analitico

- [ ] Tarefa P4.1: Column statistics panel
  - Details: .copilot-tracking/details/20260406-audit-react-otimizacao-details.md (Linhas 413-455)

- [ ] Tarefa P4.2: ConversaoTab migracao para DataTable base
  - Details: .copilot-tracking/details/20260406-audit-react-otimizacao-details.md (Linhas 457-500)

- [ ] Tarefa P4.3: Zustand store split por dominio
  - Details: .copilot-tracking/details/20260406-audit-react-otimizacao-details.md (Linhas 502-550)

- [ ] Tarefa P4.4: Multi-sort e filter presets na ConsultaTab
  - Details: .copilot-tracking/details/20260406-audit-react-otimizacao-details.md (Linhas 552-590)

### [ ] Fase P5: Features avancadas e qualidade

- [ ] Tarefa P5.1: Auto-highlight de nulos, outliers e duplicados
  - Details: .copilot-tracking/details/20260406-audit-react-otimizacao-details.md (Linhas 593-630)

- [ ] Tarefa P5.2: Mini graficos por coluna (recharts)
  - Details: .copilot-tracking/details/20260406-audit-react-otimizacao-details.md (Linhas 632-665)

- [ ] Tarefa P5.3: CI basico (.github/workflows/ci.yml)
  - Details: .copilot-tracking/details/20260406-audit-react-otimizacao-details.md (Linhas 667-700)

- [ ] Tarefa P5.4: Test suite minimo
  - Details: .copilot-tracking/details/20260406-audit-react-otimizacao-details.md (Linhas 702-740)

## Dependencies

- @tanstack/react-virtual (pnpm add @tanstack/react-virtual)
- xlsx (pnpm add xlsx)
- recharts (pnpm add recharts) - apenas Fase P5

## Success Criteria

- DataTable com 5000 linhas rola suavemente sem travar a UI
- ConsultaTab mostra total/filtrado/selecionados acima da tabela
- Export CSV baixa resultado filtrado completo (nao apenas pagina atual)
- Estado sobrevive reload: CNPJ + ultima aba + ultimo arquivo restaurados
- tsc --noEmit e pnpm lint passam sem erros apos cada fase
