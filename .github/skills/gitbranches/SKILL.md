---
name: gitbranches
description: Gerencia branches Git e fluxos de trabalho com branches em repositórios GitHub. Use esta skill para criar convenções de branch naming, definir estratégia de branching, orientar merge/rebase/cherry-pick, organizar trabalho paralelo e reduzir conflitos. Keywords: git branch, branching strategy, feature branch, hotfix, release branch, merge, rebase, cherry-pick, conflict resolution, branch naming, gitflow, trunk-based development.
---

Esta skill orienta o uso correto de branches em repositórios Git e GitHub, com foco em clareza operacional, segurança das mudanças e manutenção de um histórico saudável.

## Quando usar
Use esta skill quando a tarefa envolver:
- criar ou nomear branches
- definir estratégia de branches do projeto
- decidir entre merge, squash merge e rebase
- organizar branches de feature, hotfix, release ou experimentação
- resolver conflitos entre branches
- preparar branches para pull requests
- limpar branches antigas ou desatualizadas
- reduzir risco de divergência entre trabalho local e remoto

## Objetivo
Garantir que o fluxo de branches seja previsível, fácil de entender e adequado ao ritmo do time e do repositório.

## Instruções
1. Identifique o modelo de trabalho atual do repositório antes de propor mudanças.
   - Verifique se o projeto usa trunk-based development, Git Flow, release branches ou um fluxo híbrido.
   - Preserve o padrão existente, salvo se houver motivo claro para melhorar.

2. Ao criar ou sugerir nomes de branches, use nomes curtos, específicos e legíveis.
   - Prefira padrões como:
     - feature/add-login-rate-limit
     - fix/api-timeout-on-retry
     - hotfix/payment-webhook-null-check
     - chore/update-eslint-config
     - docs/readme-install-steps
   - Quando houver ID de issue, inclua-o se isso já for convenção do projeto:
     - feature/123-add-user-search

3. Ao orientar integração entre branches:
   - Use merge quando o histórico do branch precisa ser preservado.
   - Use squash merge quando o objetivo for manter o histórico principal limpo.
   - Use rebase quando fizer sentido alinhar um branch com a base mais recente sem introduzir merge commits desnecessários.
   - Não force push em branches compartilhadas sem avisar claramente o impacto.

4. Ao lidar com branches long-lived:
   - Avalie risco de drift em relação à branch principal.
   - Recomende sincronização frequente.
   - Evite deixar branches de longa duração sem dono claro.

5. Ao resolver conflitos:
   - Identifique arquivos afetados e a natureza do conflito.
   - Preserve comportamento correto, não apenas a compilação.
   - Oriente validação após a resolução: testes, lint, build e revisão dos pontos de integração.

6. Ao sugerir estratégia de branching:
   - Para times pequenos e deploy frequente, prefira trunk-based development.
   - Para fluxos com releases controladas, use release branches apenas se houver necessidade real.
   - Evite complexidade desnecessária.

## Regras
- Não proponha fluxos pesados se o projeto for pequeno.
- Não recomende rebase destrutivo em branches públicos sem alertar o risco.
- Não trate conflito resolvido como seguro sem validação.
- Não invente convenções; derive do repositório quando possível.

## Saída esperada
Sempre que relevante, responda com:
- estado atual
- estratégia recomendada
- comandos ou passos sugeridos
- riscos
- validação após a mudança
