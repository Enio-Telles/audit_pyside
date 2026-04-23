# Consolidacao de branches em main - 2026-04-23

## Objetivo

Registrar a consolidacao do `main` com os branches locais e remotos disponiveis em
`https://github.com/Enio-Telles/audit_pyside`, preservando a decisao desktop-only da
ADR-001 e evitando reintroduzir backend HTTP.

## Estado de referencia

- `main` local e `origin/main`: `2bb0778 merge: unify p1 and p3 branches into main`.
- Remotes `origin` e `upstream` apontam para `https://github.com/Enio-Telles/audit_pyside.git`.
- Branches P1/P3 recentes ja estavam incorporados ao `main` antes desta consolidacao.
- Branch de backup local criado antes da analise: `backup/main-before-consolidation-*`.

## Reaproveitado

- De `origin/chore/p1-base-eng`: atualizacao segura do workflow
  `.github/workflows/cleanup-snapshots.yml` para usar `uv sync` e `uv run`, alinhando o
  job ao contrato atual do `pyproject.toml`/`uv.lock`.
- De branches Sentinel de seguranca: mantida a abordagem ja presente em `main` de
  registrar erros em log interno e exibir mensagens sanitizadas. O ponto remanescente em
  `src/transformacao/calculos_periodo_pkg/calculos_periodo.py` foi alinhado ao padrao dos
  modulos irmaos, removendo a escrita direta de traceback em arquivo de trabalho.

## Nao reaproveitado

- Branches que reintroduzem `backend/`, FastAPI ou superficie HTTP foram descartados por
  conflito com a ADR-001 Opcao B.
- Branches `origin/master` e derivados com `frontend/` ou planos React foram descartados
  por estarem fora do escopo atual do produto desktop PySide6.
- Branches Bolt antigos com diffs amplos em regras fiscais, SQL, artefatos de runtime ou
  logs foram descartados por alto risco de regressao e por estarem muito atrasados em
  relacao ao `main`.
- Branches P3 locais anteriores foram considerados obsoletos porque o `main` ja contem a
  linha P3 consolidada em `refactor/p3-main-window-codex-gpt54`.

## Validacao esperada

- `uv sync`
- `uv run pre-commit run --all-files`
- `uv run pytest -q`
- `uv run mypy src/transformacao`
- `uv run python -c "import src"`
