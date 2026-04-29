# AGENTS.md — src/transformacao

Estas instruções valem para toda a árvore `src/transformacao/`.
Para regras transversais (chaves invariantes, anti-padrões gerais, formato de resposta),
veja `AGENTS.md` na raiz.

---

## Papel desta área

Aqui vive a **regra principal** de transformação, harmonização, agrupamento, conversão e cálculo.
Implemente a regra no módulo temático correto; não espalhe lógica em wrappers ou pontos laterais.

---

## Camadas do pipeline (sequência obrigatória)

```
raw → base → curated → marts / views
```

| Camada | Responsabilidade |
|---|---|
| **raw** | Captura dados na forma mais próxima da origem (Oracle, CSV, Parquet). Sem transformação de negócio. |
| **base** | Normaliza tipos e nomes, remove duplicatas, define chaves consistentes. |
| **curated** | Agrega e harmoniza para análise; mantém integridade das chaves invariantes. |
| **marts / views** | Expõe datasets prontos para GUI, relatórios ou API. |

Nunca pule etapas. Não escreva direto no `curated` sem passar pelo `base`.
Crie módulos separados por etapa; evite scripts monolíticos que misturam extração e agregação.

---

## Chaves invariantes nesta camada

| Chave | Observação |
|---|---|
| `id_agrupado` | Formato canônico: `id_agrupado_auto_<sha1[:12]>` (ex.: `id_agrupado_auto_3f2a9c1b8e4d`). Gerado em `03_descricao_produtos.py` via SHA-1 da descrição normalizada. Não redefina manualmente. |
| `id_agregado` | Alias de apresentação; use `utilitarios.compat.ensure_id_aliases` para garantir compatibilidade com datasets legados. |
| `__qtd_decl_final_audit__` | Valor de auditoria; não alterar o saldo físico. |
| `q_conv` | Quantidade convertida para unidade de referência (saídas de `calculos_periodo_pkg`). |
| `q_conv_fisica` | Quantidade convertida na perspectiva física (movimentação de estoque). |

---

## Regras específicas de pipeline

### Polars e LazyFrame

- Use sempre `.group_by()` — `.groupby()` é **proibido** em Polars 1.x e levanta erro de runtime.
- Prefira `pl.scan_parquet` / `pl.scan_csv` (LazyFrame) para operações encadeadas.
- Chame `.collect()` apenas nos **pontos de checkpoint** definidos (fim de etapa, materialização).
- Não use pandas para grandes volumes; use Polars.

### SQL e extração

- Consultas de extração residem em `sql/`; **nunca** escreva SQL inline em scripts Python.
- Chame queries via `utilitarios.sql_catalog` e `utilitarios.ler_sql`.
- Oracle é apenas fonte de extração inicial; execute toda lógica analítica no Polars.

### Cache-first

- Prefira ler Parquet existente antes de reextrair do Oracle.
- Verifique `existe_parquet(cnpj, dataset)` antes de disparar extração.

### Lineage e logging

- No início de cada pipeline, registre: CNPJ, período (ano-mês), dataset de origem, filtros aplicados.
- No final, registre: nome do dataset gerado, schema (colunas + tipos) e data/hora de geração.
- Mantenha manifesto de datasets (localização, origem, schema, periodicidade).

### Compatibilidade legada

- Use `utilitarios.compat.ensure_id_aliases(df)` ao consumir datasets que podem conter
  nomes de coluna anteriores à padronização (ex.: `id_group` → `id_agrupado`).
  Essa função está em `src/utilitarios/compat.py` e aceita `DataFrame` ou `LazyFrame`.

### Schema Parquet

- Não altere schema sem avaliar todos os consumidores, migração e necessidade de reprocessamento.
- Padronize colunas de data, valores monetários e quantidade de forma explícita
  (`pl.Date`, `pl.Float64`, `pl.Decimal` — não deixe como `pl.Utf8` por omissão).

---

## Mudanças sensíveis nesta área

Dê atenção extra a:
- Agrupamento de produtos (`id_agrupado`, `id_agregado`)
- Conversão de unidades (`q_conv`, `q_conv_fisica`, fatores de conversão)
- Movimentação de estoque (saldos iniciais/finais, eventos C170/NF-e)
- Cálculos mensais e anuais
- Deduplicação e joins críticos
- Materialização Parquet (schema, path, nomes de arquivo)

Para qualquer uma dessas mudanças: declare **Riscos** e **Rollback** na PR.

---

## Anti-padrões específicos

- Misturar extração e agregação no mesmo script sem separar em etapas.
- Usar `.groupby()` (Polars 1.x proíbe — use `.group_by()`).
- Usar pandas para volumes que Polars trataria melhor.
- Criar Parquet sem registrar schema ou origem.
- Aplicar lógica fiscal (impostos, conformidade) sem documentação ou validação.
- Ignorar `ensure_id_aliases` ao consumir datasets legados (quebra joins silenciosamente).
- Chamar `.collect()` prematuramente em LazyFrames longos.
- Escrever SQL ad hoc como string Python em vez de usar o catálogo `sql/`.

---

## Validação esperada

Quando aplicável:
- Testes unitários para regra crítica (cada módulo temático).
- Testes de integração para encadeamento de etapas.
- Validação de schema antes e depois da mudança.
- Validação de cálculo (totais `base` ≡ `raw`; métricas `curated` representam os originais).
- Validação de reprocessamento idempotente (rodar duas vezes produz o mesmo resultado).

---

## Formato de resposta

Use o formato padrão definido em `AGENTS.md` da raiz:
**Objetivo / Contexto / Reaproveitamento / Arquitetura / Implementação / Validação / Riscos / MVP**
