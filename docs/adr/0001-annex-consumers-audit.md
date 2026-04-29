# ADR-001 Annex: auditoria de consumidores reais do `backend/`

Data da auditoria: 2026-04-22
Branch: `feat/p2-remove-backend-adr-b`

## Comandos executados

```text
git grep -n "from backend"
# sem matches

git grep -n "import backend"
# sem matches

git grep -nE "fastapi|uvicorn|starlette"
backend/AGENTS.md:8:> **ADR-001** (`docs/adr/0001-futuro-backend-fastapi.md`). Não expanda contratos aqui
backend/routers/aggregation.py:6:from fastapi import APIRouter, HTTPException
backend/routers/sql_query.py:8:from fastapi import APIRouter, HTTPException, Query
docs/README.md:8:A decisão sobre o futuro do backend FastAPI está em discussão — veja [ADR-001](adr/0001-futuro-backend-fastapi.md) quando disponível.
docs/README.md:18:| Backend FastAPI (stub) | `backend/` | [`backend/AGENTS.md`](../backend/AGENTS.md) | [ADR-001](adr/0001-futuro-backend-fastapi.md) |
docs/README.md:78:| [ADR-001](adr/0001-futuro-backend-fastapi.md) | Decisão sobre o futuro do backend FastAPI |
output/log_review_report.txt:69:    from fastapi import HTTPException
output/log_review_report.txt:156:    from fastapi import APIRouter, HTTPException
output/log_review_report.txt:190:    from fastapi import APIRouter, HTTPException, Request
output/log_review_report.txt:224:    from fastapi import APIRouter, HTTPException
output/log_review_report.txt:252:    from fastapi import APIRouter, HTTPException
output/log_review_report.txt:254:    from starlette.responses import StreamingResponse
output/log_review_report.txt:287:    from fastapi import APIRouter, HTTPException
output/log_review_report.txt:320:    from fastapi import APIRouter, HTTPException, Query
output/log_review_report.txt:348:    from fastapi import APIRouter, BackgroundTasks, HTTPException
output/log_review_report.txt:383:    from fastapi import APIRouter, HTTPException, Query
output/log_review_report.txt:416:    from fastapi import HTTPException
output/log_review_report.txt:423:    from fastapi import HTTPException
output/log_review_report.txt:530:    from fastapi import HTTPException
plan/feature-notificacao-react-1.md:8:tags: [feature, react, fastapi, fisconforme, notificacao]
plan/feature-notificacao-react-1.md:148:- **RISK-003**: Portabilidade do path do modelo TXT — usar `Path(__file__).resolve().parent.parent.parent / "modelo"` para garantir resolução correta independente do CWD do processo uvicorn.
plano_otimizacao_q.md:145:from fastapi import HTTPException
server/python/api.py:2:from fastapi import HTTPException

git grep -n "http://localhost:8000"
# sem matches

git grep -nE "requests\\.(get|post|put|delete)"
scripts/notion_export.py:54:        resp = requests.get(url, headers=headers, timeout=30)

rg -n "backend/" src/ tests/ scripts/
# sem matches
```

## 1. Consumidores Python (importa `backend.*`)

- `git grep -n "from backend"` não encontrou imports.
- `git grep -n "import backend"` não encontrou imports.
- `rg -n "backend/" src/ tests/ scripts/` não encontrou referências de uso em `src/`, `tests/` ou `scripts/`.

Conclusão:
não existe consumidor Python real do diretório `backend/` no código de produção do repositório atual.

## 2. Consumidores HTTP (localhost:8000 ou endpoint nomeado)

- `git grep -n "http://localhost:8000"` não encontrou clientes HTTP apontando para o backend local.
- `git grep -nE "requests\\.(get|post|put|delete)"` retornou apenas `scripts/notion_export.py:54`, que chama a API do Notion, não o backend local.
- Não foi encontrado consumidor HTTP nomeado para as rotas de `backend/routers/aggregation.py` ou `backend/routers/sql_query.py`.

Conclusão:
o backend FastAPI não possui cliente HTTP real dentro do repositório.

## 3. Testes ligados ao backend

- Não há testes que importem `backend.*`.
- Os testes relacionados à área cobrem serviços Python usados pela GUI, não rotas HTTP:
  - `tests/test_aggregation_service.py`
  - `tests/test_aggregation_service_contract.py`
  - `tests/test_aggregation_service_periodos.py`
  - `tests/test_registry_service.py`
- `tests/test_app.py` valida o launcher `app.py` da GUI PySide6 e não depende do backend.

Conclusão:
os testes existentes protegem serviços reaproveitados pela GUI desktop; não existe harness de teste que exija o `backend/` como superfície HTTP viva.

## 4. Configurações / docs que mencionam FastAPI

- Menções arquiteturais e documentais:
  - `docs/README.md`
  - `docs/adr/0001-futuro-backend-fastapi.md`
  - `backend/AGENTS.md`
- Implementação stub a ser removida:
  - `backend/routers/aggregation.py`
  - `backend/routers/sql_query.py`
- Resíduos paralelos fora de `backend/`:
  - `server/python/api.py` importa `fastapi.HTTPException`, mas não tem consumidores no repositório.
  - `output/log_review_report.txt` contém trechos históricos de review com `fastapi` e `starlette`.
  - `plan/feature-notificacao-react-1.md` e `plano_otimizacao_q.md` mencionam FastAPI/Uvicorn apenas em documentação/plano.

Conclusão:
as referências a FastAPI fora de `backend/` são documentais ou residuais. Elas não configuram consumidor real de produção do stub.
A validação final deve focar caminhos de código e configuração do produto, com exclusões explícitas para `output/`, `artifacts/`, `tmp/` e `.venv/`.

## Decisão operacional desta auditoria

- `backend/` é um stub sem consumidores reais de produção no repositório atual.
- A remoção do backend pode prosseguir.
- Ajustes adicionais necessários no Commit 3 para cumprir a validação final (escopo de código/produto):
  - remover `backend/`;
  - eliminar o resíduo `fastapi` de `server/python/api.py`;
  - manter artefatos históricos fora do escopo de bloqueio conforme exclusões documentadas (`output/`, `artifacts/`, `tmp/`, `.venv/`).
