---
name: dependency-prs
description: Revisa pull requests automáticas de dependências com foco em risco, agrupamento, compatibilidade e prontidão para merge. Use esta skill para avaliar PRs do Dependabot, Renovate ou updates manuais de bibliotecas. Keywords: dependabot PR, renovate PR, dependency PR review, update bot, version bump review, package PR, merge safety.
---

Esta skill é focada em um caso específico: revisão de PRs de dependências.

## Quando usar
Use esta skill quando a tarefa envolver:
- revisar PR automática de atualização
- decidir se uma PR de dependência pode ser aprovada
- separar updates seguros de arriscados
- explicar impacto técnico de um version bump
- montar checklist de validação para merge

## Objetivo
Tomar decisões melhores sobre merge de updates automáticos, evitando tanto bloqueio excessivo quanto aprovação cega.

## Instruções
1. Identifique:
   - dependência alterada
   - versão atual e nova
   - tipo de bump
   - se é runtime, devDependency, tooling ou transitiva

2. Avalie:
   - frequência de uso no código
   - possibilidade de breaking change
   - impacto em build, testes e produção
   - presença de mudanças em lockfile além do esperado

3. Classifique a PR:
   - baixo risco, pronta para validar
   - risco moderado, exige revisão focal
   - alto risco, separar ou testar profundamente

4. Monte recomendação:
   - aprovar
   - aprovar após validação
   - pedir mudanças
   - separar em PR dedicada
