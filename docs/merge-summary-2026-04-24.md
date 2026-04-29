# Resumo de merges — 2026-04-24

Este arquivo registra os merges aplicados ao `main` em 2026-04-24 e recomendações de follow-up.

## PRs aplicadas

- PR #140 — fix(orquestrador): rename logger→log to match call sites
  - Correção 1-linha: restaurou o identificador `log` para evitar `NameError` nas chamadas `log.info()`.

- PR #141 — docs(p5-A): Google-style docstrings in tabelas_base — closes #124 batch 2
  - Adicionou docstrings estilo Google em `src/transformacao/tabelas_base/` para 23 símbolos.

- PR #143 — chore(ci): pin workflow actions to immutable SHAs
  - Atualizou referências de Actions em workflows para SHAs imutáveis.

- PR #144 — ci(p4-06): add dedicated ubuntu gui smoke job
  - Introduziu job `Test GUI Smoke (ubuntu)` e preservou a exclusão `gui_smoke` dos testes de cobertura.
  - Ajuste aplicado no arquivo: `.github/workflows/ci.yml`.

## Follow-ups recomendados

1. Adicionar `pytest-qt` ao grupo `dev` em `pyproject.toml` e atualizar o lockfile (`uv lock` / `uv sync --frozen`) para evitar instalações ad-hoc no CI.
2. Monitorar os workflows `Test` e `Test GUI Smoke` no GitHub Actions para validar estabilidade após as mudanças.
3. Separar mudanças de infra/CI em PRs pequenos (já aplicado aqui), e abrir PRs específicos para mudanças de dependência/lockfile.

---
Arquivo criado na branch `chore/merge-summary-2026-04-24`.
