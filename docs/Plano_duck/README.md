# Plano Mestre DuckDB + GUI Paginada

Este diretorio contem a documentacao tecnica e o planejamento para a introducao do DuckDB como backend de consulta para arquivos Parquet grandes, a implementacao de paginacao na GUI PySide6 e a disponibilizacao de ferramentas via protocolo MCP.

## Documentos

| Arquivo | Conteudo |
|---|---|
| [00_PLANO_MESTRE.md](00_PLANO_MESTRE.md) | Visao geral, objetivos e regras inviolaveis |
| [01_decisoes_pendentes.md](01_decisoes_pendentes.md) | Registro de decisoes arquiteturais (D1–D7) |
| [02_arquitetura_alvo_polars_duckdb.md](02_arquitetura_alvo_polars_duckdb.md) | Desenho da arquitetura dual Polars/DuckDB |
| [03_roadmap_fases_e_prs.md](03_roadmap_fases_e_prs.md) | Sequenciamento de implementacao (E0–E8) |
| [04_governanca_qualidade_riscos.md](04_governanca_qualidade_riscos.md) | Gate de qualidade, riscos e rollback |
| [05_kpis_metas_baseline.md](05_kpis_metas_baseline.md) | KPIs, metas e baseline |
| [06_plano_acao_imediato.md](06_plano_acao_imediato.md) | Checklists de acao imediata (semana 1) |

## Contexto

Datasets fiscais acima de 2GB causam instabilidade ao serem carregados integralmente via Polars na GUI. A solucao adota o DuckDB para consultas eficientes com projecao e paginacao, mantendo o Polars no nucleo de processamento fiscal.
