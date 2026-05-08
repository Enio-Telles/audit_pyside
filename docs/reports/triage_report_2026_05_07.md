# Relatório de Triagem de Pull Requests - 2026-05-07

Este relatório documenta as ações tomadas durante a triagem realizada em 2026-05-07 para reduzir a entropia do backlog de PRs.

## 🏁 Sumário Executivo
- **PRs Fechadas:** 4
- **PRs Atualizadas:** 1
- **Issues Atualizadas:** 1 (#180)
- **Novos Documentos:** 1 (`docs/triage_checklist.md`)

## 🛠️ Detalhamento das Ações

### PRs Fechadas (Cleanup)

| PR # | Título | Motivo | Branch Associada |
|------|--------|--------|------------------|
| #251 | fix(security): harden Oracle SQL identifier validation... | Superseded by #252 | `fix/security-oracle-identifier-validation-8322277142990585362` |
| #230 | 🔒 [security fix] Fix SQL Injection risk... | Superseded by #252 | `fix/sql-injection-extrator-8852069188509921212` |
| #222 | fix: debug windows ci | Debug/Sem critério de aceite | `fix/issue-189-windows-ci` |
| #176 | refactor(p7-gui-omit): extract pure helpers... | Stale/Blocked (Fatal Error 0xc0000139) | `copilot/refactor-ui-main-window-helpers` |

### PRs Mantidas / Atualizadas

- **#224 (Draft):** Atualizada com motivo explícito de HOLD no corpo da PR (aguardando validação de performance em prod).
- **Outras PRs abertas:** Foram mantidas por possuírem donos ativos ou escopo atualizado (ex: #250, #246, #245, #244, #242, #241, #240).

### Atualização de Infraestrutura de Processos

- **Issue #180:** Atualizada com a lista de 4 branches candidatas a remoção imediata.
- **`docs/triage_checklist.md`:** Criado para servir como guia normativo para as próximas triagens semanais.

## ✅ Critérios de Aceite Atingidos
- [x] Classificar PRs abertas.
- [x] Fechar PRs superseded com comentário explicativo.
- [x] Fechar PRs de debug sem critério claro.
- [x] Marcar PRs em hold/draft com motivo explícito.
- [x] Atualizar #180 com lista de branches para remoção.
- [x] Criar checklist padrão para triagem.
