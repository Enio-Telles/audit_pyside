# Documentação Técnica — audit_pyside

Este arquivo é o **índice vivo** de toda a documentação técnica do repositório.
Sempre que um documento for criado, movido ou removido, este índice deve ser atualizado no mesmo PR.

O projeto é uma **aplicação desktop Python/PySide6** com pipeline analítico-fiscal.
Para regras de agente e convenções de desenvolvimento, veja [`AGENTS.md`](../AGENTS.md) na raiz.
O backend FastAPI foi removido em 2026-04-22 — veja [ADR-001](adr/0001-futuro-backend-fastapi.md) e o [anexo de auditoria](adr/0001-annex-consumers-audit.md).

---

## Arquitetura

| Camada | Pasta | AGENTS.md responsável | Doc canônica |
|---|---|---|---|
| Pipeline (raw → marts) | `src/transformacao/` | [`src/transformacao/AGENTS.md`](../src/transformacao/AGENTS.md) | [Catálogo de tabelas](tabelas/README.md) |
| Interface PySide6 | `src/interface_grafica/` | [`src/interface_grafica/AGENTS.md`](../src/interface_grafica/AGENTS.md) | — |
| SQL / extração | `sql/` | `AGENTS.md` (raiz) | [scripts_usage.md](scripts_usage.md) |
| Testes | `tests/` | `AGENTS.md` (raiz) | — |
| Documentação | `docs/` | `AGENTS.md` (raiz) | Este arquivo |

---

## Documentos vivos

### Fiscal & regras de negócio

| Documento | Descrição |
|---|---|
| [conversao_unidades.md](conversao_unidades.md) | Regras de conversão de unidades (`q_conv`, `q_conv_fisica`), fatores e casos especiais |
| [agregacao_produtos_canonico.md](agregacao_produtos_canonico.md) | Regras canônicas de agrupamento de produtos (`id_agrupado`) |
| [mov_estoque.md](mov_estoque.md) | Movimentação de estoque — eventos, saldos, C170/NF-e |
| [abordagem_qconv_fisica.md](abordagem_qconv_fisica.md) | Detalhamento da abordagem para `q_conv_fisica` |
| [tabelas/c176_xml.md](tabelas/c176_xml.md) | Schema e regras do Registro C176 (PMU) |
| [tabelas/movimentacao_estoque.md](tabelas/movimentacao_estoque.md) | Schema da tabela de movimentação de estoque |
| [tabelas/fatores_conversao.md](tabelas/fatores_conversao.md) | Schema dos fatores de conversão |

> **Gaps de documentação:**
> - `TODO`: `docs/auditoria_conversao_agregacao_estoque.md` — documento de auditoria
>   consolidado de conversão, agregação e estoque ainda não existe. Criá-lo em P1-04/P2.
> - `TODO`: `docs/plano_melhorias_backend_frontend_arquitetura.md` — plano de melhorias
>   consolidado ainda não existe como arquivo canônico (conteúdo disperso em `docs/PLAN.md`
>   e `docs/archive/`). Consolidar em P2.
> - `TODO`: Regras C176/PMU/inventário unificadas em um único doc fiscal.

### Catálogo de tabelas Parquet

| Documento | Descrição |
|---|---|
| [tabelas/README.md](tabelas/README.md) | Índice do catálogo de tabelas |
| [tabelas/produtos_final.md](tabelas/produtos_final.md) | Schema de `produtos_final_{cnpj}.parquet` |
| [tabelas/descricao_produtos.md](tabelas/descricao_produtos.md) | Schema de `descricao_produtos` |
| [tabelas/fontes_produtos.md](tabelas/fontes_produtos.md) | Schema de fontes de produtos |
| [tabelas/calculos_mensais.md](tabelas/calculos_mensais.md) | Schema dos cálculos mensais |
| [tabelas/calculos_anuais.md](tabelas/calculos_anuais.md) | Schema dos cálculos anuais |
| [tabelas/c170_xml.md](tabelas/c170_xml.md) | Schema do C170 |
| [tabelas/itens.md](tabelas/itens.md) | Schema da tabela de itens |
| [tabelas/item_unidades.md](tabelas/item_unidades.md) | Schema de item × unidades |
| [tabelas/tb_documentos.md](tabelas/tb_documentos.md) | Schema da tabela de documentos |

### Metodologia e análise

| Documento | Descrição |
|---|---|
| [metodologia_mds_plan.md](metodologia_mds_plan.md) | Plano da Metodologia MDS |
| [analise_metodologia_mds_runtime_2026-04-20.md](analise_metodologia_mds_runtime_2026-04-20.md) | Análise de runtime da MDS (2026-04-20) |
| [tabela_mensal.md](tabela_mensal.md) | Estrutura da tabela mensal |
| [tabela_anual.md](tabela_anual.md) | Estrutura da tabela anual |
| [tabela_periodo.md](tabela_periodo.md) | Estrutura da tabela por período |

### Arquitetura & plano

| Documento | Descrição |
|---|---|
| [PLAN.md](PLAN.md) | Plano de execução P0–P5 (fonte de verdade do roadmap) |
| [plano_q.md](plano_q.md) | Plano de melhorias da qualidade dos dados |
| [ADR-001](adr/0001-futuro-backend-fastapi.md) | Decisão sobre o futuro do backend FastAPI |

> **Gap de documentação:**
> - `TODO`: `docs/plano_melhorias_backend_frontend_arquitetura.md` — consolidar a partir
>   de `PLAN.md` + `docs/archive/Plano de Melhorias - Arquitetura.md`.

### Operações & runbooks

| Documento | Descrição |
|---|---|
| [scripts_usage.md](scripts_usage.md) | Como usar os scripts em `scripts/` (generate_parquet_references, generate_output_samples, etc.) |
| [codex_usage.md](codex_usage.md) | Como usar o Codex com este repositório |
| [operational/snapshots_mapa_manual.md](operational/snapshots_mapa_manual.md) | Runbook de snapshots do mapa manual |
| [branch_cleanup.md](branch_cleanup.md) | Procedimento de limpeza de branches |
| [PR_followups.md](PR_followups.md) | Follow-ups e débitos técnicos de PRs anteriores |

> **Gap de documentação:**
> - `TODO`: `docs/runbook_sync_repo.md` — runbook de sincronização do repositório ainda
>   não existe como arquivo canônico. Criar em P2.

### Referências normativas

| Documento / Arquivo | Descrição |
|---|---|
| [referencias/fatores_conversao_unidades.md](referencias/fatores_conversao_unidades.md) | Tabela de fatores de conversão de unidades |
| `referencias/Guia Prático EFD - Versão 3.2.1.pdf` | Guia prático EFD (SPED) |
| `referencias/MOC_CTe_VisaoGeral_v4.00.pdf` | Manual de Orientação ao Contribuinte — CT-e |
| `referencias/Manual de Orientação ao Contribuinte - MOC - versão 7.0 - NF-e e NFC-e.pdf` | MOC NF-e e NFC-e v7.0 |
| `referencias/Manual_CTe_v2_0.pdf` | Manual CT-e v2.0 |
| `referencias/moc7-anexo-i-leiaute-e-rv (2).pdf` | MOC v7 Anexo I — Leiaute e Regras de Validação |

### Análises e diagnósticos

| Documento | Descrição |
|---|---|
| [analise_audit_pyside.md](analise_audit_pyside.md) | Análise geral do projeto |
| [agente_audit_pyside.md](agente_audit_pyside.md) | Descrição do agente audit_pyside |
| [agente_audit_pyside_pyside_only.md](agente_audit_pyside_pyside_only.md) | Configuração do agente PySide-only |

---

## Como contribuir

### Nomenclatura de branches

| Tipo | Padrão | Exemplo |
|---|---|---|
| Feature | `feat/<modulo>-<objetivo>` | `feat/estoque-saldo-inicial` |
| Fix | `fix/<modulo>-<problema>` | `fix/conversao-unidade-nula` |
| Docs | `docs/<tema>` | `docs/adr-backend` |
| Chore / infra | `chore/<escopo>` | `chore/p1-consolidacao-docs` |
| Refactor | `refactor/<modulo>-<escopo>` | `refactor/main-window-decompose` |

### AGENTS por escopo

Antes de contribuir em uma área, leia o AGENTS.md correspondente:

- **Pipeline / transformação** → [`src/transformacao/AGENTS.md`](../src/transformacao/AGENTS.md)
- **GUI PySide6** → [`src/interface_grafica/AGENTS.md`](../src/interface_grafica/AGENTS.md)
- **Qualquer outra área** → [`AGENTS.md`](../AGENTS.md) (raiz)

## Transição

| Origem anterior | Situação em P1-01 | Destino canônico atual |
|---|---|---|
| `.agent.md` | Removido | [`AGENTS.md`](../AGENTS.md) |
| `docs/AGENTS.md` | Removido | [`AGENTS.md`](../AGENTS.md) |
| `tests/AGENTS.md` | Removido | [`AGENTS.md`](../AGENTS.md) |
| Escopo de pipeline | Mantido como escopo canônico | [`src/transformacao/AGENTS.md`](../src/transformacao/AGENTS.md) |
| Escopo de GUI PySide6 | Mantido como escopo canônico | [`src/interface_grafica/AGENTS.md`](../src/interface_grafica/AGENTS.md) |
| Backend stub | Removido em P2 (ADR-001 Opção B) | [`AGENTS.md`](../AGENTS.md) + [ADR-001](adr/0001-futuro-backend-fastapi.md) |

### Regras de PR

- Nunca commitar direto na `main`.
- PRs devem ter escopo único e ser revisáveis em uma sessão.
- Não misturar refatoração ampla com correção de regra fiscal.
- Descrição da PR: **Objetivo / Contexto / Reaproveitamento / Arquitetura / Implementação / Validação / Riscos / MVP**.
- Mudanças em schema Parquet, chaves de join, conversão ou estoque requerem seção explícita de **Riscos e Rollback**.
- ADRs para decisões arquiteturais significativas: `docs/adr/NNNN-kebab-case.md`.

---

## Status atual (P0–P5)

> Fonte de verdade: [`docs/PLAN.md`](PLAN.md). Esta seção apenas espelha o estado de alto nível.

| Fase | Descrição | Status |
|---|---|---|
| P0 | Estabilização da base (limpeza, compat, CI básico) | Em andamento |
| P0-04 | Limpeza de artefatos obsoletos (copy dirs, tmp, patch files) | ✅ Concluído (PR atual) |
| P1 | Consolidação de docs e AGENTS | Em andamento (PR atual) |
| P1-01 | Consolidação de AGENTS.md (11 → 4) | ✅ Concluído (PR atual) |
| P1-02 | Índice mestre `docs/README.md` | ✅ Concluído (PR atual) |
| P1-03 | ADR-001 draft (futuro do backend) | ✅ Concluído (PR atual) |
| P1-04/05/06 | pyproject+uv, ruff+mypy+pre-commit, CI | Concluído (PR atual) |
| P2 | Remoção do backend FastAPI (ADR-001 Opção B) | Em andamento (PR atual) |
| P3 | Decomposição de `main_window.py` | Planejado |
| P4 | Multi-tenant / autenticação | Não iniciado |
| P5 | Performance e escalabilidade | Não iniciado |

---

## Gaps de documentação (TODO)

- `docs/auditoria_conversao_agregacao_estoque.md` — doc consolidado de auditoria de conversão, agregação e estoque.
- `docs/plano_melhorias_backend_frontend_arquitetura.md` — plano de melhorias consolidado de arquitetura.
- `docs/runbook_sync_repo.md` — runbook de sincronização do repositório.
- ADRs adicionais para decisões de P3 (decomposição GUI) e P4 (autenticação).
