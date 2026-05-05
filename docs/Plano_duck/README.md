# Plano Mestre DuckDB + GUI Paginada

Este diretório contém a documentação técnica e o planejamento para a introdução do DuckDB como backend de consulta para arquivos Parquet grandes, a implementação de paginação na GUI PySide6 e a disponibilização de ferramentas via protocolo MCP.

## Documentos

| Arquivo | Conteúdo |
|---|---|
| [00_PLANO_MESTRE.md](00_PLANO_MESTRE.md) | Visão geral, objetivos e regras invioláveis |
| [01_decisoes_pendentes.md](01_decisoes_pendentes.md) | Registro de decisões arquiteturais (D1–D7) |
| [02_arquitetura_alvo.md](02_arquitetura_alvo.md) | Desenho da arquitetura dual Polars/DuckDB |
| [03_roadmap_fases_e_prs.md](03_roadmap_fases_e_prs.md) | Sequenciamento de implementação (E0–E8) |

## Contexto

Datasets fiscais acima de 2GB causam instabilidade ao serem carregados integralmente via Polars na GUI. A solução adota o DuckDB para consultas eficientes com projeção e paginação, mantendo o Polars no núcleo de processamento fiscal.
