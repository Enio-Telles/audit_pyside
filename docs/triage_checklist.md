# Checklist de Triagem Semanal de Pull Requests

Este documento define o processo padrão para triagem de Pull Requests (PRs) no repositório, com foco especial em PRs geradas por agentes automáticos (Jules, Bolt, Copilot, etc.).

## 🎯 Objetivos
- Reduzir ruído no backlog de PRs abertas.
- Garantir que apenas PRs com escopo claro e base atualizada permaneçam abertas.
- Evitar o merge acidental de código obsoleto ou com falhas de CI conhecidas.

## 📋 Checklist de Avaliação

Para cada PR aberta, classifique-a em uma das seguintes categorias:

### 1. MANTER (Keep)
- **Critérios:**
  - Tem um "dono" (humano ou agente ativo).
  - Escopo técnico claro e necessário.
  - Passando em todos os testes críticos de CI (incluindo fumaça e cobertura).
  - Base (branch) atualizada com a `main`.
- **Ação:** Nenhuma imediata, manter para review.

### 2. CORRIGIR / REBASE (Fix/Rebase)
- **Critérios:**
  - Conteúdo valioso, mas com conflitos ou falhas menores de CI.
- **Ação:** Solicitar rebase ou pequenos ajustes. Marcar com label `needs-fix` se necessário.

### 3. FECHAR COMO SUPERSEDED
- **Critérios:**
  - Outra PR (mais recente ou mais completa) já resolve o mesmo problema.
  - O código já foi mesclado por outra via.
- **Ação:** Fechar a PR com um comentário referenciando a PR substituta (ex: "Fechando em favor da #123").

### 4. FECHAR COMO STALE / BLOCKED
- **Critérios:**
  - PR em Draft/Hold há mais de 7 dias sem atividade.
  - Falhas críticas persistentes (ex: Erro fatal no Windows 0xc0000139) sem previsão de correção.
  - PRs de "debug" que já cumpriram seu papel.
- **Ação:** Fechar a PR. Se o trabalho ainda for relevante no futuro, recomendar "recriar de forma limpa" (clean start).

### 5. RECRIAR LIMPO (Recreate)
- **Critérios:**
  - O histórico de commits está muito sujo ou a branch está muito defasada para um rebase simples.
- **Ação:** Fechar a PR antiga e abrir uma nova com o conteúdo consolidado.

---

## 🧹 Processo de Cleanup de Branches

Após fechar uma PR que não será mesclada:
1. Identifique o nome da branch associada.
2. Adicione a branch à lista de "Candidatas a Remoção" na Issue de cleanup vigente (ex: #180).
3. Utilize o script `scripts/cleanup_stale_branches.py` para automatizar a deleção após validação manual.

## 🤖 Orientações para Agentes
- PRs geradas automaticamente **devem** ter um corpo descritivo (O que, Por que, Riscos, Validação).
- PRs com corpo padrão ou vazio ("test", "debug") devem ser fechadas imediatamente se não houver contexto adicional.
