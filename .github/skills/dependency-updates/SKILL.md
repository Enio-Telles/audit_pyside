---
name: dependency-updates
description: Avalia e orienta atualização de dependências em repositórios GitHub, com foco em compatibilidade, risco, segurança e manutenção. Use esta skill para revisar upgrades, planejar updates graduais, analisar changelogs e evitar quebras desnecessárias. Keywords: dependency update, package upgrade, npm update, pip upgrade, semver, breaking change, renovate, dependabot, lockfile, library upgrade.
---

Esta skill ajuda a atualizar dependências com método, evitando tanto o congelamento do projeto quanto upgrades arriscados e mal validados.

## Quando usar
Use esta skill quando a tarefa envolver:
- atualizar bibliotecas
- revisar PRs do Dependabot ou Renovate
- decidir entre upgrade pontual ou em lote
- investigar quebra após update
- analisar impacto de major version
- revisar lockfile e compatibilidade
- reduzir risco de dependências defasadas

## Objetivo
Manter dependências atualizadas com segurança, previsibilidade e esforço proporcional ao risco.

## Instruções
1. Identifique o tipo de atualização:
   - patch
   - minor
   - major
   - transitive dependency
   - tooling dependency
   - runtime dependency

2. Avalie risco:
   - criticidade da biblioteca
   - impacto em runtime
   - impacto em build
   - breaking changes conhecidos
   - frequência de uso no código
   - cobertura de testes disponível

3. Ao analisar update:
   - verifique changelog e notas de migração, quando necessário
   - identifique APIs afetadas
   - revise configurações relacionadas
   - avalie impacto em typings, build, bundling e CI

4. Ao propor estratégia:
   - prefira updates pequenos e frequentes
   - agrupe só dependências de baixo risco ou altamente relacionadas
   - trate majors com validação mais cuidadosa
   - registre passos de rollback quando relevante

5. Após update:
   - rode testes
   - valide lint e build
   - verifique warnings novos
   - revise arquivos de lock
