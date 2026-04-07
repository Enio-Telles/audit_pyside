---
mode: agent
model: Claude Sonnet 4.6
---

<!-- markdownlint-disable-file -->

# Implementation Prompt: Audit React - Otimizacao P2-P5

## Instrucoes de Implementacao

### Passo 1: Criar arquivo de tracking de mudancas

Criar `20260406-audit-react-otimizacao-changes.md` em #file:../changes/ se nao existir.

### Passo 2: Executar implementacao

Seguir #file:../plans/20260406-audit-react-otimizacao-plan.instructions.md
tarefa a tarefa, consultando os detalhes em
#file:../details/20260406-audit-react-otimizacao-details.md para cada item.

**Regras de execucao:**
- Apos cada tarefa: rodar `pnpm exec tsc --noEmit` e verificar que passa
- Apos cada Fase: rodar `pnpm lint` e registrar mudancas no arquivo de changes
- Nao implementar Fase P5 (CI/tests) antes das Fases P2-P4 estarem concluidas
- Para instalacao de pacotes usar `pnpm add <pacote>` (nunca npm ou yarn)
- Preservar toda logica de negocio fiscal existente (fallback de preco, id_agrupado, etc)

**CRITICO:** Parar apos cada Fase se ${input:phaseStop:true} for true.
**CRITICO:** Parar apos cada Tarefa se ${input:taskStop:false} for true.

### Passo 3: Verificacao final

Antes de finalizar cada Fase, executar:
1. `cd C:\Sistema_pysisde\frontend && pnpm exec tsc --noEmit`
2. `pnpm lint`
3. Confirmar que app_react.py sobe sem erro (python app_react.py)

### Passo 4: Cleanup

Quando TODAS as Fases estiverem com [x]:

1. Fornecer resumo das mudancas de #file:../changes/20260406-audit-react-otimizacao-changes.md

2. Fornecer links para:
   - .copilot-tracking/plans/20260406-audit-react-otimizacao-plan.instructions.md
   - .copilot-tracking/details/20260406-audit-react-otimizacao-details.md
   - .copilot-tracking/research/20260406-audit-react-otimizacao-research.md

3. Deletar este arquivo .copilot-tracking/prompts/implement-audit-react-otimizacao.prompt.md

## Success Criteria

- [ ] Arquivo de changes criado e atualizado continuamente
- [ ] Todas as fases implementadas com codigo funcional
- [ ] tsc --noEmit e pnpm lint passam sem erros
- [ ] DataTable virtualiza linhas sem travamento
- [ ] Summary cards exibem totais corretos no ConsultaTab
- [ ] Export CSV exporta resultado filtrado completo
- [ ] URL sync funcional (tab + cnpj persistem na URL)
- [ ] Estado restaura apos reload da pagina
