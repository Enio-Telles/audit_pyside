---
name: monorepo-structure
description: Organiza e avalia a estrutura de monorepos, incluindo apps, packages, libs compartilhadas, boundaries, build, testes e fluxo de desenvolvimento. Use esta skill para melhorar clareza, escalabilidade e manutenção em monorepos. Keywords: monorepo, workspace, packages, apps, libs, pnpm workspace, turborepo, nx, shared packages, code boundaries, repo structure.
---

Esta skill orienta a organização de monorepos com foco em separação clara de responsabilidades, reaproveitamento saudável e baixo acoplamento.

## Quando usar
Use esta skill quando a tarefa envolver:
- revisar estrutura de monorepo
- separar apps e packages
- reorganizar bibliotecas compartilhadas
- definir boundaries entre módulos
- reduzir acoplamento interno
- melhorar scripts e fluxo de desenvolvimento
- analisar impacto estrutural de crescimento do repositório

## Objetivo
Manter o monorepo previsível, escalável e fácil de navegar, testar e evoluir.

## Instruções
1. Identifique os tipos de artefato existentes:
   - apps
   - packages
   - shared libraries
   - tooling
   - config central
   - documentação

2. Avalie a estrutura atual:
   - organização de diretórios
   - clareza de ownership
   - dependências entre pacotes
   - duplicação
   - fronteiras arquiteturais
   - dificuldade de build ou teste

3. Ao propor estrutura:
   - separe executáveis de bibliotecas
   - agrupe packages por responsabilidade
   - mantenha nomenclatura consistente
   - minimize dependências cruzadas desnecessárias
   - favoreça contratos explícitos entre módulos

4. Avalie tooling do monorepo:
   - workspaces
   - task runners
   - cache
   - pipelines afetadas
   - impacto no desenvolvimento local

5. Considere experiência do time:
   - onboarding
   - descoberta de código
   - tempo de feedback
   - previsibilidade de scripts

## Regras
- Não reestruture por estética apenas.
- Não criar camadas extras sem benefício claro.
- Não misture código compartilhado com app-specific sem justificativa.
- Não centralize tudo em pacotes “common” vagos e superacoplados.
