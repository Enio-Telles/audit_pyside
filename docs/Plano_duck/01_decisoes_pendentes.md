# Decisões Operacionais e de Governança

As decisões abaixo documentam os acordos operativos firmados após a conclusão da fase E5 (benchmark/release-hardening) para os itens pendentes #209–#215.

## D1 — Owner formal de performance (Resolvido)
**Decisão:** Enio is the formal Performance Owner.
**Rationale e Impacto:** Estabelece a responsabilidade final pelas aprovações de performance e garante que relatórios de benchmarking possuam escrutínio humano, mesmo se delegados a agentes no dia a dia.
- Benchmark execution may be delegated to AI agents.
- Human go/no-go remains with Enio.
- PRs labeled `performance` must include KPI evidence or explicit justification of non-applicability.
- `docs/baseline_performance.md` is the canonical performance baseline document.

## D2 — Owner formal de release (Resolvido)
**Decisão:** Enio is the formal Release Owner.
**Rationale e Impacto:** Garante controle de qualidade sobre as versões enviadas aos usuários e protege o ciclo de vida do software com etapas de aprovação claras.
- Tags, changelog, version bumps and release notes require human approval.
- Releases require green CI, applicable smoke tests and rollback notes.
- Versioning should follow SemVer-style `vMAJOR.MINOR.PATCH`.

## D3 — Hardware-target for benchmarks (Resolvido)
**Decisão:** Initial official benchmark target is: Windows, Intel i5 (4 physical cores), 16 GB RAM.
**Rationale e Impacto:** Fixa uma base realista comum para aferição de ganhos/perdas de performance antes da aprovação de melhorias.
- Every benchmark report must record: CPU, RAM, OS, storage type if known, Python version, Polars version, DuckDB version, dataset/fixture used, command used.
- Results from other machines are allowed, but must be marked as comparative, not official baseline.

## D4 — Fixture policy (Resolvido)
**Decisão:** Hybrid fixture policy.
**Rationale e Impacto:** Protege dados sensíveis, aderindo à LGPD, mantendo viabilidade de testes representativos.
- CI uses synthetic fixtures.
- Manual/local benchmarks may use real data only if pseudonymized.
- No sensitive real data may be committed.
- Pseudonymization must preserve schema, relevant cardinality, joins where required, and the five fiscal invariants (`id_agrupado`, `id_agregado`, `__qtd_decl_final_audit__`, `q_conv`, `q_conv_fisica`).
- Large real/pseudonymized datasets stay outside the repository.

## D5 — Review SLA (Resolvido)
**Decisão:** SLAs estabelecidos por categoria de revisão.
**Rationale e Impacto:** Organiza expectativas de tempo e previne gargalos no desenvolvimento e correção de erros.
- Critical fiscal correction: 1 business day
- Small PR without fiscal logic: 2–3 business days
- Performance/refactor PR: up to 5 business days
- Docs/housekeeping: up to 7 business days
- Draft PRs are outside SLA
- Chained PRs must be reviewed in dependency order

## D6 — MCP DuckDB roadmap (Resolvido)
**Decisão:** MCP DuckDB is deferred to E6/future after the core desktop workflow is stable.
**Rationale e Impacto:** Evita distração do foco crítico (estabilidade desktop) antes que as ferramentas core estejam maduras.
- Does not block E0–E5.
- Future MCP server should be separate from the desktop app.
- It must be read-only by default.
- It should target agents/Claude Desktop style consumers.
- It must not expose arbitrary `execute_sql`.
- Minimum future tools may include: `list_datasets`, `get_schema`, `count_rows`, `query_page`, `aggregate_by`.

## D7 — Windows code signing policy (Resolvido)
**Decisão:** Internal technical distribution may continue unsigned for now.
**Rationale e Impacto:** Reduz overhead imediato de publicação e certificação até que o aplicativo alcance estabilidade para distribuição pública automática.
- Before auto-update or recurring distribution to non-technical users, require Authenticode OV certificate.
- EV certificate and MSIX/Store distribution are deferred until there is a real distribution need.
- Auto-update remains blocked until signing policy is satisfied.
