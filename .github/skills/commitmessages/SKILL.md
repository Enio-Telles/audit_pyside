---
name: commitmessages
description: Padroniza mensagens de commit com clareza, rastreabilidade e foco técnico. Use esta skill para escrever, revisar ou sugerir commits melhores, seguindo convenções como Conventional Commits ou padrões internos do repositório. Keywords: commit message, conventional commits, feat, fix, chore, refactor, docs, semantic commits, commit hygiene, git commit.
---

Esta skill ajuda a criar mensagens de commit claras, úteis para revisão, histórico, release notes e manutenção futura.

## Quando usar
Use esta skill quando a tarefa envolver:
- escrever mensagem de commit
- revisar commits ruins ou vagos
- aplicar Conventional Commits
- separar mudanças em commits melhores
- melhorar histórico antes de abrir PR
- padronizar convenção do repositório

## Objetivo
Produzir commits que expliquem com precisão o que mudou e por que isso importa.

## Instruções
1. Identifique a natureza principal da mudança:
   - feat
   - fix
   - refactor
   - docs
   - test
   - chore
   - ci
   - perf
   - build

2. Escreva o título do commit com foco em ação e escopo.
   - Bom padrão:
     - fix(auth): prevent token refresh loop
     - feat(search): add result pagination
     - docs(readme): clarify local setup steps

3. No corpo do commit, quando necessário:
   - explique o problema
   - explique a solução
   - explique impacto ou migração
   - cite issue ou contexto, se houver

4. Quando houver mudanças misturadas:
   - sugira separar em commits menores por intenção
   - evite commits gigantes e genéricos

5. Preserve a convenção do repositório.
   - Se o projeto já usa padrão próprio, siga esse padrão.

## Regras
- Não use mensagens vagas como “ajustes”, “update”, “fix stuff” ou “melhorias”.
- Não descreva só o arquivo alterado; descreva a intenção da mudança.
- Não force Conventional Commits se o repositório claramente usa outro padrão.
- Não misture várias intenções em um único commit quando isso puder ser evitado.
