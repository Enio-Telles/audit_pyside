# PLAN.md — Redirecionamento

> **Este documento foi arquivado.**
>
> O plano vigente e o **Plano Mestre DuckDB + GUI Paginada**, que define a
> arquitetura de query dual (Polars / DuckDB), as fases de entrega (E0–E9)
> e os PRs associados.
>
> **Consulte: [docs/Plano_duck/00_PLANO_MESTRE.md](Plano_duck/00_PLANO_MESTRE.md)**

## Por que este arquivo foi arquivado

O conteudo original deste arquivo descrevia um plano de reorganizacao das
consultas SQL para a arvore `sql/` e eliminacao de dependencias em caminhos
absolutos externos. Esse escopo foi valido ate abril/2026, quando o ADR-001
removeu o backend FastAPI do repositorio.

A partir de entao, o foco passou para a estabilidade da GUI PySide6 com
arquivos Parquet grandes (> 512 MB), o que gerou o plano atual em
`docs/Plano_duck/`.

## Indice do plano vigente

| Arquivo | Conteudo |
|---|---|
| [00_PLANO_MESTRE.md](Plano_duck/00_PLANO_MESTRE.md) | Objetivo, contexto e visao geral |
| [01_decisoes_pendentes.md](Plano_duck/01_decisoes_pendentes.md) | Decisoes D1–D7 (owner, benchmark, policy) |
| [02_arquitetura_alvo_polars_duckdb.md](Plano_duck/02_arquitetura_alvo_polars_duckdb.md) | Arquitetura dual Polars + DuckDB |
| [03_roadmap_fases_e_prs.md](Plano_duck/03_roadmap_fases_e_prs.md) | Roadmap E0–E9 e tabela de PRs |
| [04_governanca_qualidade_riscos.md](Plano_duck/04_governanca_qualidade_riscos.md) | Gate de qualidade, riscos e rollback |
| [05_kpis_metas_baseline.md](Plano_duck/05_kpis_metas_baseline.md) | KPIs, metas e baseline |
| [06_plano_acao_imediato.md](Plano_duck/06_plano_acao_imediato.md) | Checklists de acao imediata (semana 1) |
| [README.md](Plano_duck/README.md) | Indice e caminho critico |