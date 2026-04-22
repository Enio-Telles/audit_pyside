# ADR-001: Futuro do Backend FastAPI

| Campo | Valor |
|---|---|
| **Status** | Proposed |
| **Data** | 2026-04-22 |
| **Deciders** | `<!-- TODO: preencher com nomes/usernames dos decisores -->` |
| **PR de origem** | chore/p1-consolidacao-docs |

> ⚠️ **O P2 inteiro depende da decisão deste ADR.**
> Nenhuma expansão do backend deve ocorrer até que uma opção seja escolhida e este ADR
> seja atualizado para Status: **Accepted**.

---

## Contexto

### Estado atual do backend

O diretório `backend/` contém um **stub FastAPI** com dois roteadores:

**`backend/routers/aggregation.py`** — 4 rotas:
- `GET /{cnpj}/tabela_agrupada` — lê `produtos_agrupados_{cnpj}.parquet` ou `produtos_final_{cnpj}.parquet` e retorna paginado.
- `POST /merge` — delega a `ServicoAgregacao.agregar_linhas()`.
- `POST /unmerge` — reverte último merge via `log_agregacoes_{cnpj}.json`.
- `GET /{cnpj}/historico_agregacoes` — lê `log_agregacoes_{cnpj}.json`.

**`backend/routers/sql_query.py`** — 5 rotas:
- `GET /files` — lista arquivos SQL do catálogo.
- `POST /execute` — executa SQL pré-catalogado (sem SQL arbitrário).
- `GET /file` — lê conteúdo de arquivo SQL.
- `POST /files` — cria arquivo SQL no catálogo.
- `DELETE /files` — remove arquivo SQL.

### Consumidores identificados (via `git grep`)

| Consumidor | Tipo | Acoplamento |
|---|---|---|
| `src/interface_grafica/ui/main_window.py` | GUI | **Direto** — importa `ServicoAgregacao` e `SqlService` em Python, sem HTTP |
| `src/interface_grafica/services/aggregation_service.py` | Serviço | **Direto** — implementa a lógica; o roteador `aggregation.py` a chama via import |
| `src/interface_grafica/services/sql_service.py` | Serviço | **Direto** — o roteador `sql_query.py` a chama via import |
| `backend/routers/aggregation.py` | Roteador | Importa `ServicoAgregacao` e `interface_grafica.config` |
| `backend/routers/sql_query.py` | Roteador | Importa `SqlService`, `utilitarios.sql_catalog` |

**Nenhum frontend externo** (React, JS, mobile) faz chamadas HTTP ao backend.
O backend **não está em execução** como serviço — é código que _poderia_ ser ligado, mas atualmente
a GUI consume os mesmos serviços por acoplamento direto (import Python), não via HTTP.

### Testes relacionados ao backend

- `tests/test_aggregation_service.py`
- `tests/test_aggregation_service_contract.py`
- `tests/test_aggregation_service_periodos.py`
- `tests/test_registry_service.py`

Os testes cobrem `ServicoAgregacao` (lógica de negócio), não as rotas HTTP.

---

## Drivers de decisão

1. **Custo de manutenção atual**: o backend duplica a superfície de mudança — qualquer alteração em `ServicoAgregacao` pode afetar tanto a GUI quanto o roteador.
2. **Ausência de cliente HTTP real**: nenhum consumidor chama o backend via HTTP hoje.
3. **Multi-tenant futuro (P4)**: se o produto evoluir para múltiplos usuários simultâneos (web/SaaS), um backend HTTP com autenticação será essencial.
4. **Autenticação**: sem HTTP, auth é trivial (app local); com HTTP, requer JWT/OAuth e toda a infraestrutura.
5. **Endpoints de sync**: futuro suporte a sincronização de dados entre instâncias exige contratos HTTP estáveis.
6. **Acoplamento GUI ↔ pipeline**: hoje a GUI acessa o pipeline por import direto — simples, mas não escalável para múltiplos usuários.
7. **Impacto nas chaves invariantes**: qualquer rota HTTP deve serializar/deserializar `id_agrupado`, `id_agregado`, `__qtd_decl_final_audit__`, `q_conv`, `q_conv_fisica` sem perda de precisão ou tipagem.

---

## Opções consideradas

### Opção A — Manter e evoluir o backend FastAPI

**Descrição:** Continuar desenvolvendo o backend com contratos HTTP estáveis, adicionar autenticação (JWT/OAuth2), preparar para multi-tenant.

| Aspecto | Detalhe |
|---|---|
| **Prós** | Pronto para web/multi-tenant. Desacopla GUI do pipeline. Contratos HTTP versionáveis. Permite múltiplos clientes (web, mobile, CLI). |
| **Contras** | Esforço significativo: auth, testes de rota, documentação OpenAPI, deploy. Hoje sem cliente real, o valor é potencial. Duplicação temporária de superfície. |
| **Impacto em chaves** | Requer serialização explícita de `id_agrupado`, `__qtd_decl_final_audit__`, `q_conv`, `q_conv_fisica` em JSON (risco de precisão em Float64). |
| **Esforço estimado** | P2 completo: auth (3–5 dias) + testes de rota (2–3 dias) + CI/deploy (2–3 dias) = ~2 semanas. |
| **Reversibilidade** | Alta: os serviços (`ServicoAgregacao`, `SqlService`) continuam existindo. Pode-se remover as rotas e manter os serviços. |

---

### Opção B — Remover o backend, manter pipeline + GUI desktop como única superfície

**Descrição:** Deletar `backend/` inteiramente. GUI continua consumindo serviços via import Python direto.

| Aspecto | Detalhe |
|---|---|
| **Prós** | Elimina duplicação. Simplifica stack. Reduz superfície de ataque. Menor esforço de manutenção. Foco no valor desktop. |
| **Contras** | Sem caminho para multi-tenant/web sem reescrita futura. Se a necessidade de API aparecer, será retrabalho. |
| **Impacto em chaves** | Nenhum — as chaves continuam em Polars/Parquet sem serialização HTTP. |
| **Esforço estimado** | Remoção de `backend/` + atualização de docs: ~0.5 dia. |
| **Reversibilidade** | Média: recriar o backend é viável (serviços existem), mas requer reescrita das rotas e testes. |

---

### Opção C — Híbrido minimalista: manter apenas endpoints de leitura, escrita permanece na GUI

**Descrição:** Conservar apenas rotas de leitura (`GET /{cnpj}/tabela_agrupada`, `GET /files`, `GET /file`) como API somente-leitura. Remover rotas de escrita (`POST /merge`, `POST /unmerge`, `POST /files`, `DELETE /files`). Escrita continua exclusivamente na GUI.

| Aspecto | Detalhe |
|---|---|
| **Prós** | Permite integração de leitura (dashboards, relatórios externos) sem a complexidade de auth completo. Reduz superfície de escrita. Menor esforço que Opção A. |
| **Contras** | Estado intermediário: ainda requer manutenção do backend. Pode criar expectativa de expansão futura não planejada. Não resolve multi-tenant. |
| **Impacto em chaves** | Serialização de `id_agrupado`, `q_conv`, `q_conv_fisica` em JSON (risco menor por ser leitura). |
| **Esforço estimado** | Remoção de rotas de escrita + testes: ~1 dia. |
| **Reversibilidade** | Alta — rotas de escrita podem ser readicionadas quando necessário. |

---

## Análise comparativa

| Critério | Opção A | Opção B | Opção C |
|---|---|---|---|
| Custo de manutenção atual | Alto | Muito baixo | Médio |
| Valor entregue hoje | Baixo (sem cliente HTTP) | Zero (sem backend) | Baixo (leitura apenas) |
| Preparação para multi-tenant | Alta | Baixa | Média |
| Risco de perda de chaves invariantes | Médio (serialização) | Nenhum | Baixo |
| Reversibilidade | Alta | Média | Alta |
| Complexidade para P2 | Alta | Baixa | Média |

---

## Consequências da decisão

`<!-- TODO: preencher após a escolha ser feita -->`

- **Se Opção A:** Definir contratos OpenAPI estáveis antes de implementar. Escolher estratégia de auth (JWT/OAuth2). Garantir precisão de Float64 na serialização JSON de q_conv/q_conv_fisica.
- **Se Opção B:** Deletar `backend/` em PR separado. Atualizar `backend/AGENTS.md` e este ADR. Arquivar testes de rota.
- **Se Opção C:** Definir quais rotas de leitura sobrevivem. Documentar contrato de leitura. Remover rotas de escrita.

---

## Recomendação do autor do draft

> **Esta é uma sugestão do Claude — a decisão final é exclusivamente do mantenedor humano.**

Com base no estado atual (nenhum cliente HTTP real, GUI como única superfície, P4 multi-tenant ainda distante), a recomendação é **Opção B** com uma ressalva estratégica:

- **Curto prazo**: remover o backend (`backend/`) para reduzir custo de manutenção e complexidade. Os serviços `ServicoAgregacao` e `SqlService` sobrevivem em `src/interface_grafica/services/` e continuam testados.
- **Quando P4 (multi-tenant) for iniciado**: recriar o backend com contratos bem definidos, autenticação desde o início, e separação clara de serviços.

A Opção C é aceitável se houver uma necessidade concreta de leitura por cliente externo no curto prazo (ex.: dashboard Power BI lendo a tabela agrupada). Nesse caso, documente o cliente antes de manter as rotas de leitura.

A Opção A aumenta a carga de manutenção do P2 sem valor imediato, dado que nenhum cliente HTTP existe hoje.

---

## Referências

- `backend/routers/aggregation.py`
- `backend/routers/sql_query.py`
- `backend/AGENTS.md`
- `src/interface_grafica/services/aggregation_service.py`
- `src/interface_grafica/services/sql_service.py`
- `docs/PLAN.md` — roadmap P0–P5
- `.jules/bolt.md` e `.jules/sentinel.md` — lições de segurança SQL/Info Disclosure
