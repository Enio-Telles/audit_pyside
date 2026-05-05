# D1–D7: Decisões Pendentes — Plano Duck

Este documento registra as decisões arquiteturais e operacionais críticas para o sucesso do **Plano DuckDB + GUI Paginada**.

| ID | Decisão | Status | Responsável |
|---|---|---|---|
| D1 | Owner do Benchmark (Performance Owner) | Pendente | — |
| D2 | Ambiente de Benchmark (Hardware de referência) | Pendente | — |
| D3 | Baseline de Aceitação (SLA de UI) | Pendente | — |
| **D4** | **Política de Fixtures e Anonimização** | **Resolvida** | Jules |
| D5 | Estratégia de Rollback (Mecanismo de chave) | Pendente | — |
| D6 | Política de Versionamento de Esquema Parquet | Pendente | — |
| D7 | Governança de Metadados (Lineage) | Pendente | — |

---

## D4: Política de Fixtures — Anonimização de dados reais para benchmarks

### Contexto
Os benchmarks de performance precisam de arquivos Parquet grandes (256 MB, 1 GB, 2 GB) para validar a escalabilidade do DuckDB e a fluidez da GUI paginada. Dados reais sem anonimização não podem entrar no repositório, mas fixtures 100% sintéticas podem não representar a distribuição real e mascarar regressões.

### Decisão
Adotar uma política **Híbrida**:

1.  **Fixtures Sintéticas para CI**: O repositório conterá apenas geradores de dados 100% sintéticos (via scripts em `bench/` ou `tests/`) para garantir que o pipeline de CI rode sem riscos de vazamento de dados.
2.  **Dados Reais Anonimizados para Benchmarks Manuais**: Para testes de performance locais e validação de "pé no chão", serão utilizados dados reais submetidos a um processo de anonimização (scrambling de CNPJ, nomes de produtos, NF-e e embaralhamento de quantidades/valores).

### Invariantes de Dados
Mesmo em dados anonimizados para benchmark, as seguintes colunas devem manter integridade referencial para não quebrar a lógica fiscal:
- `id_agrupado`
- `id_agregado`
- `__qtd_decl_final_audit__`
- `q_conv`
- `q_conv_fisica`

### Ações Decorrentes
- [ ] Criar script `scripts/anonymize_benchmark_data.py` (Opcional, conforme demanda).
- [x] Documentar esta decisão em `docs/Plano_duck/01_decisoes_pendentes.md`.
- [ ] Garantir que `.gitignore` barre qualquer Parquet em `benchmarks/data/` que não siga o padrão `.synthetic.parquet`.
