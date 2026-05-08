# Branch cleanup 2026-05-05

Analise de branches obsoletas identificadas em 2026-05-05.

## Candidatos identificados para remoção

| Branch | Último commit | Status | Razão |
|---|---|---|---|
| `docs/plano-tecnico` | 2026-05-03 | Merged | Já incorporada via P8-04 |
| `jules-docs-operational-decisions-7125381615100240099` | 2026-05-05 | Stale | Sessão finalizada (D1-D7 documentados) |
| `jules-queryworker-refactor-7614520702098751230` | 2026-05-05 | Stale | Sessão finalizada (Refactor QueryWorker) |
| `jules-18359405592451721585-12781355` | 2026-05-05 | Stale | Branch de trabalho temporário |
| `jules-13674073371832230422-b558650f` | 2026-05-05 | Stale | Branch de trabalho temporário |
| `jules-509290870124344749-e8bbfa3c` | 2026-05-05 | Stale | Branch de trabalho temporário |
| `jules-10106912866993454076-2d04d01a` | 2026-05-05 | Stale | Branch de trabalho temporário |
| `jules-543838100602943425-a1e844da` | 2026-05-05 | Stale | Branch de trabalho temporário |
| `jules-15385003481662677784-baf8d45d` | 2026-05-05 | Stale | Branch de trabalho temporário |
| `test-pipeline-funcoes-service-14900753902383807951` | 2026-05-05 | Stale | Teste de CI finalizado |
| `fix-test-inverted-index-empty-7142989372496027754` | 2026-05-05 | Stale | Correção de teste pontual |
| `fix/test-dataframe-vazio-inverted-index-3977889272862569682` | 2026-05-05 | Stale | Correção de teste pontual |

## Comandos de remoção (para execução manual)

Devido a restrições de permissão no ambiente de execução, a deleção remota deve ser feita via:

```bash
git push origin --delete docs/plano-tecnico
git push origin --delete jules-docs-operational-decisions-7125381615100240099
git push origin --delete jules-queryworker-refactor-7614520702098751230
git push origin --delete jules-18359405592451721585-12781355
git push origin --delete jules-13674073371832230422-b558650f
git push origin --delete jules-509290870124344749-e8bbfa3c
git push origin --delete jules-10106912866993454076-2d04d01a
git push origin --delete jules-543838100602943425-a1e844da
git push origin --delete jules-15385003481662677784-baf8d45d
git push origin --delete test-pipeline-funcoes-service-14900753902383807951
git push origin --delete fix-test-inverted-index-empty-7142989372496027754
git push origin --delete fix/test-dataframe-vazio-inverted-index-3977889272862569682
```

## Branches preservadas (verificadas)

- `main`, `master`: Protegidas.
- `feature/agregacao-similaridade-descricao`: Desenvolvimento ativo de similaridade.
- `bolt-perf-id-agrupados-3111501178008888405`: Pesquisa em andamento.
- `copilot/refactor-ui-main-window-helpers`: PR ativa (#176).
- `copilot/docs-p7-docstrings-batch-5`: PR ativa.
- `feat/p10-06-mypy-clean-non-gui`: Trabalho pendente de tipagem.
