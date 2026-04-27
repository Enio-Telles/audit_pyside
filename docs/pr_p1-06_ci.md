Objetivo: adicionar workflow CI GitHub Actions (P1-06) usando uv para lint e testes.

Contexto: consolidação P1-06 — providenciar pipeline reprodutível para lint/mypy/pytest em Linux e Windows.

Reaproveitamento: usa hooks existentes (pre-commit, ruff, mypy) e `uv` para orquestração de ambiente.

Arquitetura: workflow com jobs `lint`, `test-linux` (matrix 3.11/3.12) e `test-windows`; `astral-sh/setup-uv@v3` habilita cache de `uv`.

Implementação: (1) adicionado `.github/workflows/ci.yml`; (2) badge em `docs/README.md` e `AGENTS.md`; (3) nota em `src/transformacao/AGENTS.md` sobre gate mypy.

Validação local: `uv sync --all-extras`, `uv run pre-commit run --all-files` → pass; `uv run pytest -q` → 231 passed; `uv run mypy src/transformacao` → pass; `uv run python -c "import src"` → pass.

Riscos: `uv sync` pode alterar temporariamente o ambiente (restaure pip via ensurepip se necessário); workflow remoto roda em PR para validação final.

MVP: CI mínimo para `src/transformacao` com lint, type-check e testes, badge e instruções locais.
