# Branch cleanup 2026-05-01

Branches deletadas em 2026-05-01.
Cleanup de 6 branches stale que nao tinham PR ativa ou conteudo novo relevante.

| Branch | Ultimo commit | Commits ahead of main |
|---|---|---|
| `bolt/perf-produtos-agrupados-15388191500612887027` | 2026-04-15 | 5 |
| `bolt/perf-vectorize-aggregation-loop-2309104969869895276` | 2026-04-28 | 5 |
| `bolt-optimize-map-elements-10637525711587452946` | 2026-04-28 | 6 |
| `add-desc-similarity-tests-7840698207347739351` | 2026-04-29 | 8 |
| `chore/integracao-origin-main-20260430` | 2026-04-30 | 1 (backup de sync) |
| `feat/sincronizacao-trabalho-20260430` | 2026-04-30 | 1 (backup de trabalho local) |

## Dry-run do cleanup_stale_branches.py

O script mostrou 17 candidatas (incluindo branches ativas como #173, #176, main-remote-backup).
As 6 branches acima foram deletadas diretamente via `git push origin --delete` para evitar deleções inadvertidas.

## Branches preservadas (verificadas)

- `main`, `master`, `main-remote-backup-7c65f054-202604271237`
- `bolt/native-polars-normalize-desc-14787233493943387024` (#173, PR ativa)
- `copilot/refactor-ui-main-window-helpers` (#176, PR ativa)
- `copilot/docs-p7-docstrings-batch-5`, `copilot/p7-autoupdate-research`, `copilot/p7-sign-code-sign-windows-bundle`, `copilot/review-existing-drafts` (GitHub Agent)
- `docs/plano-otimizacao-estabilidade-estoque`, `feature/agregacao-similaridade-descricao` (inspecao manual pendente)
- `feat/p8-01-bench-harness` (#178), `feat/p10-02-differential-harness` (#179) (PRs novas criadas hoje)
