# AGENTS.md — backend/

Estas instruções valem para toda a árvore `backend/`.
Para regras transversais (chaves invariantes, anti-padrões gerais, formato de resposta),
veja `AGENTS.md` na raiz.

> ⚠️ **DECISÃO ARQUITETURAL PENDENTE**: o futuro deste backend está em discussão no
> **ADR-001** (`docs/adr/0001-futuro-backend-fastapi.md`). Não expanda contratos aqui
> antes da decisão ser tomada. O P2 inteiro depende do ADR-001.

---

## Estado atual (stub)

O backend é um **stub FastAPI** com dois roteadores:

| Roteador | Arquivo | Rotas | Consumidores conhecidos |
|---|---|---|---|
| Aggregation | `routers/aggregation.py` | `GET /{cnpj}/tabela_agrupada`, `POST /merge`, `POST /unmerge`, `GET /{cnpj}/historico_agregacoes` | `ServicoAgregacao` (GUI) via chamada direta ao serviço (não via HTTP atualmente) |
| SQL Query | `routers/sql_query.py` | `GET /files`, `POST /execute`, `GET /file`, `POST /files`, `DELETE /files` | Interno; SQL restrito ao catálogo `sql/` (sem SQL ad hoc) |

**Consumidores reais identificados via `git grep`:**
- `src/interface_grafica/services/aggregation_service.py` — importa `ServicoAgregacao`
  (acoplamento direto, não via HTTP)
- `backend/routers/aggregation.py` — importa `ServicoAgregacao` e `interface_grafica.config`
- Nenhum frontend externo ou cliente HTTP identificado no repositório atual.

---

## Contratos atuais

### `GET /{cnpj}/tabela_agrupada`
- Lê `produtos_agrupados_{cnpj}.parquet` ou `produtos_final_{cnpj}.parquet`.
- Enriquece com `lista_descr_compl` do `c170_xml_{cnpj}.parquet`.
- Retorna página (default: `page=1`, `page_size=300`).
- Chave exposta: `id_agrupado`.

### `POST /merge`
- Corpo: `{ cnpj, id_agrupado_destino, ids_origem: [...] }`.
- Delega a `ServicoAgregacao.agregar_linhas()`.
- Preserva `id_agrupado` canônico como destino.

### `POST /unmerge`
- Corpo: `{ cnpj, id_agrupado }`.
- Reverte último merge via `log_agregacoes_{cnpj}.json`.

### `POST /execute` (SQL)
- Aceita `sql_id` (identificador de query no catálogo) — **não** aceita SQL arbitrário.
- Mitiga SQL Injection: queries são lidas do catálogo `sql/`, nunca do cliente.

---

## Regras específicas

- **Não expanda este backend** sem antes resolver o ADR-001.
- Toda rota nova deve preservar e expor corretamente as 5 chaves invariantes
  (`id_agrupado`, `id_agregado`, `__qtd_decl_final_audit__`, `q_conv`, `q_conv_fisica`).
- Use `sanitize_cnpj()` (de `routers/_common.py`) em todas as rotas que recebem CNPJ.
- Nunca aceite SQL arbitrário via API — sempre use o catálogo `sql/`.
- Não exponha stack traces ou detalhes de infraestrutura em respostas de erro (Information Disclosure).
- Se adicionar autenticação, aplicar em todas as rotas de escrita antes de qualquer outra mudança.

---

## Segurança (lições de PRs anteriores)

- SQL Injection mitigado: `/execute` aceita `sql_id`, não string SQL direta.
- Information Disclosure: não expor `str(exc)` ao cliente; logar internamente e retornar mensagem genérica.
- Caminhos de arquivo: validar com `_VALID_NAME_RE` e checar `is_relative_to(SQL_ROOT)`.

---

## Testes

Testes relacionados ao backend ficam em `tests/`:
- `tests/test_aggregation_service.py`
- `tests/test_aggregation_service_contract.py`
- `tests/test_aggregation_service_periodos.py`
- `tests/test_registry_service.py`

---

## Anti-padrões

- Aceitar SQL arbitrário via API.
- Expandir rotas sem resolver ADR-001 primeiro.
- Expor detalhes de exceção ao cliente.
- Ignorar `sanitize_cnpj()` em novas rotas.
- Misturar lógica de negócio fiscal nas rotas (delegue a `src/transformacao/` ou serviços da GUI).

---

## Formato de resposta

Use o formato padrão definido em `AGENTS.md` da raiz:
**Objetivo / Contexto / Reaproveitamento / Arquitetura / Implementação / Validação / Riscos / MVP**
