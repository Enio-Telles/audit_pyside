# PLAN.md — Redirecionamento

> **Este documento foi arquivado.**
>
> O plano vigente e o **Plano Mestre DuckDB + GUI Paginada**, que define a
> arquitetura de query dual (Polars / DuckDB), as fases de entrega (E0–E8)
> e os PRs associados.
>
> **Consulte: [docs/Plano_duck/00_PLANO_MESTRE.md](Plano_duck/00_PLANO_MESTRE.md)**

## Por que este arquivo foi arquivado

O conteúdo original deste arquivo descrevia um plano de reorganização das
consultas SQL para a árvore `sql/` e eliminação de dependências em caminhos
absolutos externos. Esse escopo foi válido até abril/2026, quando o ADR-001
removeu o backend FastAPI do repositório.

A partir de então, o foco passou para a estabilidade da GUI PySide6 com
arquivos Parquet grandes (> 512 MB), o que gerou o plano atual em
`docs/Plano_duck/`.

## Índice do plano vigente

| Arquivo | Conteúdo |
|---|---|
| [00_PLANO_MESTRE.md](Plano_duck/00_PLANO_MESTRE.md) | Objetivo, contexto e visão geral (Fonte Canônica) |
| [01_decisoes_pendentes.md](Plano_duck/01_decisoes_pendentes.md) | Decisões Operacionais D1–D11 |
| [02_arquitetura_alvo_polars_duckdb.md](Plano_duck/02_arquitetura_alvo_polars_duckdb.md) | Arquitetura dual Polars + DuckDB |
| [03_roadmap_fases_e_prs.md](Plano_duck/03_roadmap_fases_e_prs.md) | Roadmap E0–E8 e tabela de PRs |
| [04_governanca_qualidade_riscos.md](Plano_duck/04_governanca_qualidade_riscos.md) | Invariantes, riscos e fluxo de PR |
| [05_kpis_metas_baseline.md](Plano_duck/05_kpis_metas_baseline.md) | KPIs, metas e baseline |
| [README.md](Plano_duck/README.md) | Índice do Plano DuckDB |