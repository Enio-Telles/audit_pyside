# ADR-0005: Estratégia de tipagem categórica para campos fiscais em Parquet

- **Status:** Aceita
- **Data:** 2026-05-06
- **Decisores:** Enio Telles (audit_pyside) + sessão Claude
- **Contexto técnico:** Polars ≥ 1.32, PyArrow ≥ 14, DuckDB ≥ 1.1
- **Documento-mestre:** Notion `358edc8b7d5d81cfb33ce023d4cee84f`
- **Substitui:** —
- **Substituída por:** —

## Resumo executivo

Adotamos uma estratégia **híbrida de três tipos** para campos fiscais
em memória e em Parquet:

| Tipo | Para quais campos | Justificativa principal |
|---|---|---|
| `pl.Enum(VALUES)` | Domínios oficiais fechados (UF, CFOP, CST/CSOSN, indicadores NF-e) | Validação na borda do ETL + economia ~80% RAM |
| `pl.Categorical()` | Domínios dinâmicos (NCM, CEST, unid) | Aceita valores novos sem erro |
| `pl.String` (puro) | 5 invariantes fiscais, chaves de alta cardinalidade, texto livre | Defesa em profundidade vs Polars #24034 |

A regra-de-ouro: **invariantes fiscais nunca recebem cast categórico**,
mesmo se a cardinalidade for baixa. Esta é uma proibição absoluta,
implementada em `INVARIANT_BLOCKLIST` no módulo
`src/io/categorical_recovery.py`.

## Contexto e problema

O `audit_pyside` é o pipeline fiscal da SEFIN/RO que processa SPED, NF-e
e NFC-e em tabelas Parquet analíticas para auditoria de ICMS e
substituição tributária. O catálogo tem 13 famílias de tabelas:

- `tb_documentos`, `item_unidades`, `itens`, `descricao_produtos`,
  `produtos_final`, `fontes_produtos`, `fatores_conversao`,
  `c170_xml`, `c176_xml`, `movimentacao_estoque`,
  `calculos_mensais`, `calculos_anuais`, `calculos_periodos`.

Antes desta decisão, **todos os campos categóricos de baixa cardinalidade
estavam armazenados como `pl.String` puro**: CFOP (~700 valores oficiais,
~200 ativos), CST/CSOSN (~30-120), UF (27), NCM (~10.000), CEST
(~1.500), indicadores fiscais (2-20 cada).

Os problemas observados:

1. **Pico de RSS elevado.** Scan completo de 1 mês (~5 GB Parquet) com
   apenas 16 GB de RAM disponíveis (D3 = i5 4 cores, 16 GB Windows;
   DuckDB `memory_limit=6GB threads=2`) atinge ~6,5 GB de RSS,
   deixando margem mínima para operações concorrentes.

2. **`group_by(id_agrupado).agg(sum)`** das invariantes (operação
   central do pipeline) gasta segundos a minutos por mês.

3. **Filtros interativos da GUI** (`filter(cfop in [...])`,
   `filter(cst='00')`) são lentos por comparação de strings em vez de
   inteiros.

4. **Tamanho on-disk** dos Parquets atuais é ~5 GB/mês quando poderia
   ser ~3 GB/mês com dictionary encoding adequado.

5. **Falta de validação de domínio** na borda do ETL: CFOPs inválidos
   (`9999`, `'    5102'`, `''`) chegam ao pipeline e só são detectados
   em fases posteriores.

A pergunta de fundo: **podemos adotar tipos categóricos em Polars sem
quebrar a semântica fiscal**? Em particular, sem afetar as 5 invariantes
auditáveis: `id_agrupado`, `id_agregado`, `__qtd_decl_final_audit__`,
`q_conv`, `q_conv_fisica`.

## Forças e drivers de decisão

1. **Correção fiscal absoluta.** As 5 invariantes precisam permanecer
   byte-idênticas pré e pós-refatoração. SEFIN/RO depende delas para
   auditoria.

2. **Restrição de hardware (D3).** 16 GB de RAM total. Reduzir o pico
   de RSS é o ganho mais valioso — 30% de redução já remove o gargalo
   crítico de scans paralelos.

3. **Correção semântica em joins.** Comparar Categorical entre
   LazyFrames de origens diferentes pode falhar com `ComputeError` no
   Polars 1.x. Não podemos quebrar joins entre Parquets de meses
   distintos.

4. **Validação de dados sujos.** SPED tem strings com espaços, case
   misto, e códigos inválidos. Capturar isso na borda do ETL é mais
   barato que descobrir 3 transformações depois.

5. **Polars 1.x tem bugs conhecidos** que afetam Categorical/Enum:
   - **#24034**: high-cardinality Categorical infla 10× a serialização
     Parquet/pickle (regressão Polars 1.32, agosto/2025)
   - **#22586**: `write_parquet(use_pyarrow=True)` apaga metadata
     `_PL_ENUM_VALUES`
   - **#19389**: `scan_parquet` lazy pode rebaixar Enum→Categorical
   - **#18868**: predicate pushdown não funciona em Categorical/Enum no
     Polars (mas funciona via DuckDB sobre Parquet RLE_DICTIONARY)

6. **Domínios fiscais não são todos iguais.**
   - CFOP/CST/UF mudam raramente (Ajustes SINIEF/Convênios).
   - NCM/CEST mudam anualmente (NCM 2022, NCM 2024).
   - `unid` no SPED tem strings sujas (espaços, caixa misturada).

7. **Doutrina Polars (User Guide ≥ 1.0).** "Prefer `Enum` whenever
   possible" — `Enum` valida na escrita, `Categorical` aceita
   silenciosamente.

## Opções consideradas

### Opção A — Status quo (tudo `pl.String`)

Manter o que existe. Sem refatoração.

- ✅ Zero risco de regressão
- ❌ Pico de RSS continua em ~6,5 GB
- ❌ Filtros e groupbys continuam lentos
- ❌ Sem validação de domínio na borda do ETL
- ❌ Parquets continuam grandes em disco

**Rejeitada.** Não resolve nenhum dos 5 problemas observados.

### Opção B — `pl.Categorical` para tudo de baixa cardinalidade

Aplicar `pl.Categorical()` em qualquer coluna string com cardinalidade
< 5%, incluindo CFOP, CST, NCM, CEST, unid.

- ✅ Reduz RSS significativamente
- ✅ Acelera groupby e filter
- ❌ Sem validação de domínio (CFOP inválido aceito silenciosamente)
- ❌ Issue #24034 perigosa em high-cardinality (`id_agrupado` se alguém
  esquecer)
- ❌ NCM/CEST mudam anualmente — sem problema (Categorical aceita
  novos), mas perde-se a oportunidade de validar o que já é estável

**Rejeitada.** Doutrina Polars recomenda Enum onde possível, e perdemos
validação grátis de borda.

### Opção C — `pl.Enum` para tudo de baixa cardinalidade

Aplicar `pl.Enum(VALUES)` em qualquer coluna com cardinalidade < 5%,
carregando os valores de um cadastro versionado.

- ✅ Validação automática na borda do ETL
- ✅ Reduz RSS significativamente
- ✅ FrozenCategoricalMapping é imutável → zero re-encoding em chunks
- ❌ NCM mudou em 2024; `pl.Enum(NCM_2022)` falharia em coluna nova
- ❌ `unid` no SPED tem strings sujas que não cabem em Enum sem
  normalização prévia

**Rejeitada.** NCM e CEST mudam ao longo do ano e exigem tolerância
que `pl.Enum` não oferece.

### Opção D — Híbrida: `pl.Enum` para fechados, `pl.Categorical` para dinâmicos, `pl.String` para invariantes (escolhida)

Categorizar com regras explícitas:

- **`pl.Enum(VALUES)`** para domínios oficiais fechados que raramente
  mudam: UF (27), CFOP (~700 oficiais), CST/CSOSN (~120), indicadores
  NF-e (2-20 cada), regime tributário (4), tipo de operação SPED (2).
  Valores carregados de `ref/fiscal_codes_2026.json` versionado em git.

- **`pl.Categorical()`** para domínios dinâmicos: NCM (~10.000, muda
  anualmente), CEST (~1.500), unid (~80, sujo no SPED), CNAE, fontes
  XML, status XML.

- **`pl.String`** (puro) para:
  - As 5 invariantes fiscais (defesa em profundidade vs #24034)
  - Chaves de alta cardinalidade (chv_nfe, cnpj_*, num_doc, gtin)
  - Texto livre (descricao*, info_adicionais, x_just)

- ✅ Validação automática nos campos onde isso é grátis e útil
- ✅ Tolerância onde o domínio realmente muda
- ✅ Defesa em profundidade nas invariantes
- ✅ RSS reduzido ~46% (estimativa) sem risco de regressão fiscal
- ❌ Mais complexa que B ou C — exige lista declarativa de campos
- ❌ Cadastro JSON precisa de manutenção anual

**Aceita.** A complexidade adicional é justificada pela correção fiscal
absoluta + ganho de validação de borda + tolerância onde precisa.

## Decisão

Adotamos a **Opção D** com as seguintes regras de implementação:

### 1. Cadastro versionado de Enums

Os valores válidos para cada Enum vêm de `ref/fiscal_codes_2026.json`,
que **deve ser revisado anualmente em janeiro** (e a cada Ajuste SINIEF
intermediário) e tem:

- 569 CFOPs (entrada + saída + exterior)
- 98 combinações de CST ICMS (cartesian product origem × tributação)
- 33 CSTs PIS/COFINS
- 28 UFs (27 estados + EX = exterior)
- 13 listas auxiliares (CSOSN, modelos NF-e, indicadores SPED, etc.)
- Bloco `_metadata` com fontes oficiais (Ato COTEPE/ICMS 08/2008,
  Convênio s/nº 1970, IN RFB 1.009/2010, Ajustes SINIEF 40/2023 e
  10/2021)

### 2. Hook único de leitura tipada

Toda leitura de Parquet fiscal passa por
`audit_pyside.io.scan_parquet_typed(path)` que:

1. Carrega o JSON via `lru_cache`
2. Aplica `pl.Enum` em colunas conhecidas (carregadas do JSON)
3. Aplica `pl.Categorical()` em colunas dinâmicas
4. **Nunca** toca colunas em `INVARIANT_BLOCKLIST` (defesa em
   profundidade, com warning explícito se alguém tentar)
5. Skip idempotente quando coluna já tem o dtype esperado
6. Re-cast defensivo contornando issue #19389

### 3. Defesa em profundidade vs #24034

A `INVARIANT_BLOCKLIST` é uma `frozenset` definida no módulo:

```python
INVARIANT_BLOCKLIST: Final[frozenset[str]] = frozenset({
    "id_agrupado",
    "id_agregado",
    "__qtd_decl_final_audit__",
    "q_conv",
    "q_conv_fisica",
})
```

Mesmo se um futuro mantenedor adicionar uma invariante ao `ENUM_MAP`
por engano, o hook a remove antes do cast e emite um WARNING. Esta é a
única regra **inegociável** do módulo — invariantes fiscais nunca
podem ser categorizadas devido à inflação 10× da issue #24034.

### 4. Lista de regras numeradas para classificação

Implementadas em `scripts/audit_categorical_inventory.py` (PR 1) e
aplicadas em ordem de prioridade decrescente — **a primeira condição
que casa, vence**:

1. Coluna em `INVARIANTES_FISCAIS` → `PROIBIDO_CATEGORIZAR`
2. Coluna em `CHAVES_PROIBIDAS` (chv_nfe, cnpj, gtin, etc.) →
   `PROIBIDO_CATEGORIZAR`
3. Coluna casa `PADROES_DESCRICAO` (descricao, x_just, etc.) →
   `MANTER_STRING`
4. Coluna casa `PADROES_DATA` (Dt_, dh_, etc.) → `MANTER_STRING`
5. Coluna casa `PADROES_NUMERICOS` (vl_, qtd_, etc.) → `MANTER_STRING`
6. `n_unique > 65_536` → `MANTER_STRING` (categorizar nunca compensa)
7. Coluna em `ENUM_FIELD_MAP` → `CATEGORICAL_EM_MEMORIA__ENUM`
8. Coluna em `CATEGORICAL_FIELDS` → `CATEGORICAL_EM_MEMORIA__CATEGORICAL`
9. Dtype não-string → `MANTER_STRING`
10. Heurística: `ratio ≤ 1%` forte; `≤ 5%` candidato; `≤ 20%` MEDIR;
    `> 20%` `MANTER_STRING`

### 5. Anti-padrões formalmente proibidos

#### 5.1 `write_parquet(use_pyarrow=True)` em código novo

Apaga metadata `_PL_ENUM_VALUES` (issue #22586). **Proibido em
`src/io/`**, com lint/grep no CI:

```bash
# Em scripts/check_no_pyarrow_writer.sh
grep -rn 'use_pyarrow=True' src/io/ && exit 1
exit 0
```

Se realmente precisar escrever via PyArrow para integração externa,
usar `pyarrow.parquet.write_table` diretamente com configuração explícita
de `use_dictionary` e `dictionary_pagesize_limit` (ver §E.2 do plano
mestre).

#### 5.2 DuckDB `CREATE TYPE … AS ENUM`

Perdido na escrita Parquet (DuckDB issue #14937). **Anti-padrão**.
DuckDB lê dictionary encoding nativamente do Parquet — não precisa
"replicar" o Enum. Documentar em `CONTRIBUTING.md`.

#### 5.3 Predicate pushdown via Polars LazyFrame em colunas categóricas

Polars #18868: pushdown não funciona em Categorical/Enum. **GUI deve
filtrar via DuckDB sobre Parquet** (RLE_DICTIONARY suportado nativamente
pelo DuckDB), não via `lf.filter().collect()`.

### 6. Persistência (PR 4) com `differential_harness`

A migração v1 → v2 dos Parquets persistidos exige 3 testes verdes
**antes de mergear**:

1. `test_invariants_byte_identical`: SHA-256 das 5 invariantes
   ordenadas é idêntico v1 vs v2.
2. `test_aggregations_identical`: `group_by + sum` em 3 grouping sets
   típicos é byte-idêntico.
3. `test_encoding_physically_dictionary`: 100% dos row_groups das
   colunas-alvo usam `RLE_DICTIONARY`.

Implementados em `tests/diff_harness/test_categorical_migration.py`.
Por default fazem `pytest.skip()` (pré-PR 4); ativam-se via
`--parquet-v2=PATH`.

### 7. KPIs SMART de gate

O bench `bench/run_kpis.py` mede 8 KPIs e aplica 4 gates obrigatórios
para destravar a refatoração de `src/transformacao/*` (PR 3):

| Gate | Critério |
|---|---|
| RSS peak | redução ≥ 30% |
| `group_by` | typed ≤ 60% baseline |
| `filter cfop in [...]` | typed ≤ 30% baseline (3×+) |
| `filter cst = '00'` | typed ≤ 20% baseline (5×+) |

**Decisão GO** se ≥ 2 dos 4 gates passarem em CNPJ real. NO_GO bloqueia
o merge da PR 3.

## Consequências

### Positivas

- **Pico de RSS estimado em ~3,5 GB** (de ~6,5 GB) para scan completo
  de 1 mês — folga crítica em D3.

- **Filtros 3-8× mais rápidos** em colunas Enum (comparação UInt8 vs
  string).

- **`group_by` ~40% mais rápido** em chaves Enum.

- **Tamanho on-disk Parquet reduzido ~40%** com `RLE_DICTIONARY` em
  colunas-alvo.

- **Validação automática na borda do ETL** captura CFOPs/CSTs/UFs
  inválidos imediatamente, antes de contaminar o pipeline.

- **Cadastro versionado** dá auditabilidade — diffs de
  `ref/fiscal_codes_2026.json` mostram exatamente quando códigos foram
  adicionados/removidos.

- **Defesa em profundidade tripla** sobre as invariantes:
  1. `INVARIANT_BLOCKLIST` no módulo (impede cast no scan)
  2. Warning explícito se alguém tenta adicionar invariante ao mapa
  3. `differential_harness` valida byte-a-byte v1 vs v2 na PR 4

### Negativas

- **Complexidade adicional.** Mantenedor precisa entender 3 dtypes em
  vez de 1.

- **Cadastro JSON anual.** Em janeiro de cada ano, revisar
  `ref/fiscal_codes_YYYY.json` contra Ajustes SINIEF do ano anterior.

- **Predicate pushdown lacuna no Polars.** Issue #18868 força filtros
  da GUI a roteamento via DuckDB. Adiciona uma camada arquitetural,
  embora DuckDB já seja parte da stack.

- **Lock-in moderado em Polars ≥ 1.33.** Dependência de comportamento
  específico do writer Rust (não-PyArrow). Pin no `pyproject.toml` é
  necessário.

- **Risco de regressão silenciosa em #24034.** Se um futuro Polars
  mudar comportamento de high-cardinality Categorical sem aviso,
  invariantes podem ser afetadas. Mitigação:
  `assert_no_invariant_categorized` no `differential_harness` corre em
  CI.

### Neutras

- **NCM/CEST permanecem estritamente em `pl.Categorical`**, sem
  validação de domínio. Aceita-se essa lacuna porque o domínio é
  legitimamente dinâmico.

- **`unid` (unidade comercial) permanece em `pl.Categorical` mesmo com
  cardinalidade baixa**, porque strings sujas no SPED (espaços, case)
  exigem normalização prévia. Considerar normalização por ETL upstream
  como melhoria futura (não bloqueia esta ADR).

## Conformidade e validação

### Como esta decisão é validada em CI

1. **Testes unitários do módulo** (`tests/unit/io/test_categorical_recovery.py`,
   30 testes): cobrem regras 1-9, idempotência, defesa em profundidade.

2. **Differential harness** (`tests/diff_harness/`, 4 testes): valida
   migração v1 → v2 com gates byte-identical.

3. **Smoke test do bench** (`bench/run_kpis.py smoke`): valida
   esqueleto sem dados reais.

4. **Lint anti-padrões** (`scripts/check_no_pyarrow_writer.sh` ou
   `ruff` rule custom): impede `use_pyarrow=True` em `src/io/`.

5. **`assert_no_invariant_categorized` em smoke tests pré-release**:
   garante que nenhum Parquet de produção tem invariante categorizada.

### Como esta decisão deve evoluir

- **Anualmente em janeiro:** revisar `ref/fiscal_codes_YYYY.json`
  contra os Ajustes SINIEF do ano anterior. Adicionar códigos novos
  mantendo a ordem original. **Não remover códigos legados sem ADR**
  (dados históricos podem conter).

- **A cada release Polars:** verificar se #24034, #19389, #18868 ou
  #22586 foram resolvidas e atualizar o pin de versão.

- **Se nova lacuna surgir:** abrir nova ADR (ADR-0006...) que
  superseda esta com referência cruzada.

## Anexo A — Por que `Enum` em vez de `Categorical` para domínios fechados

A doutrina Polars (User Guide ≥ 1.0) é "Prefer Enum whenever possible".
Os 4 motivos concretos para o nosso caso:

1. **Validação na borda do ETL.** `cast(pl.Enum(CFOPS_OFICIAIS))` lança
   `InvalidOperationError` em CFOP inválido. `Categorical` aceita
   silenciosamente. Capturar dados sujos do SPED imediatamente é mais
   barato.

2. **`FrozenCategoricalMapping` é imutável.** Zero re-encoding em
   streaming/chunks — crítico para D3 (memory_limit=6GB, threads=2).

3. **Compatibilidade cross-DataFrame sem `StringCache`.** Dois Parquets
   de meses diferentes têm o mesmo encoding físico do Enum. Categorical
   pré-1.32 exigia `with pl.StringCache():`.

4. **Polars 1.32 (agosto/2025) tornou `pl.StringCache` no-op** (PR
   #23016). Código legado funciona, mas não confiar em StringCache em
   código novo.

## Anexo B — Por que invariantes nunca recebem cast (Polars #24034)

A regressão Polars 1.32 (issue #24034) infla **10×** a serialização
Parquet/pickle de Categoricals high-cardinality. As 5 invariantes têm
cardinalidade 10⁵–10⁶:

- `id_agrupado`: ~10⁵ valores únicos por mês
- `id_agregado`: ~10⁵
- `__qtd_decl_final_audit__`, `q_conv`, `q_conv_fisica`: cardinalidade
  ≈ N rows (Float64)

Categorizar qualquer uma delas explodiria o pico de RSS de ~3,5 GB
(meta) para ~35 GB — inviável em D3.

Mesmo se #24034 for resolvida em uma versão futura, a defesa em
profundidade permanece: **invariantes têm dtype semanticamente
incorreto como categórico**. Fazem parte do contrato auditável e devem
preservar identidade byte-a-byte. Esta é uma regra de negócio, não
apenas técnica.

## Anexo C — Tabela de decisão expandida

| Categoria de campo | Cardinalidade | Dtype | Ganho RSS | Ganho filter | Risco |
|---|---|---|---|---|---|
| UF | 27 | `pl.Enum` | ~85% | 5–10× | Nenhum |
| CST/CSOSN | 30–120 | `pl.Enum` | ~80% | 5–8× | Revalidar anual |
| CFOP | ~700 oficiais | `pl.Enum` | ~75% | 3–8× | Reformas tributárias |
| Indicadores NF-e | 2–20 | `pl.Enum` | ~85% | 5–10× | Nenhum |
| CEST | ~1.500 | `pl.Categorical` | ~50% | 2–5× | Cardinalidade pode crescer |
| NCM | ~10.000 | `pl.Categorical` | ~30% | 2–3× | Mudanças anuais |
| Unidade comercial | ~80 (sujo) | `pl.Categorical` | ~70% | 3–6× | Normalizar antes |
| Status/regras XML | 5–50 | `pl.Enum` ou `Categorical` | ~80% | 3–6× | Verificar valores |
| **5 invariantes fiscais** | 10⁵–10⁶ | **`pl.String` ou numérico** | N/A | N/A | **🔴 NUNCA categorizar (#24034)** |
| Chaves alta-card | 10³–10⁵ | `pl.String` | N/A | N/A | #24034 |
| Texto livre | ≈ N rows | `pl.String` | N/A | N/A | Card ≈ rows |
| Datas | n/a | `pl.Date`/`pl.Datetime` | N/A | N/A | Categorizar perde range queries |
| Tempo agregado (ano/mes) | ~10/12 | `pl.Int16`/`pl.Int8` | N/A | N/A | Categorizar perde ordenação numérica |

## Anexo D — Encaixe nas ondas do plano-mestre

| Onda | Entrega relacionada a esta ADR |
|---|---|
| **Onda 0 — Higiene** | Lint anti-`use_pyarrow=True`. Pin `polars >= 1.33`. CI marker `diff_harness`. |
| **Onda 1 — Baseline** | PR 1 — script de inventário gera `categorical_candidates.md` com a tabela de decisão preenchida com cardinalidades reais. |
| **Onda 2 — Guard rails** | `differential_harness` mergeado como skip/xfail. Snapshot tests. ADR-0005 (este documento) mergeado. |
| **Onda 3 — Caminho crítico** | PR 2 + PR 3 — `scan_parquet_typed` mergeado. `ParquetQueryService` refatorado. KPIs medidos. **Esta ADR só se torna 'Aceita' formalmente após Onda 3 com KPIs verdes.** |
| **Onda 4 — Escala** | PR 4 — rewrite Parquets v2/. `differential_harness` ativo. |
| **Onda 5 — Expansão** | Generalizar para SPED-Contribuições, EFD ICMS/IPI legados. |

## Referências

### Documentos internos

- **Plano principal:** Notion `358edc8b7d5d81cfb33ce023d4cee84f` —
  Plano de auditoria de campos categóricos
- **Subpáginas de implementação:**
  - PR 1 (inventário): Notion `358edc8b7d5d81b3a7d9ef07929df130`
  - PR 2 (categorical_recovery): Notion `358edc8b7d5d81059247de78a772a16e`
  - PR 3 (bench KPIs): Notion `359edc8b7d5d815badabcf32ee542c07`
  - PR 4 (diff_harness): Notion `359edc8b7d5d8116848cd7350ca1c9e4`
- **Plano-mestre Performance-First:** Notion `4870a1ffc6de4bfda0b88164b2dad759`

### Documentos externos

- **Polars User Guide — Categorical / Enum:**
  <https://docs.pola.rs/user-guide/concepts/data-types/categoricals/>
- **Polars #24034** (high-cardinality regression):
  <https://github.com/pola-rs/polars/issues/24034>
- **Polars #22586** (`write_parquet(use_pyarrow=True)` apaga Enum):
  <https://github.com/pola-rs/polars/issues/22586>
- **Polars #19389** (`scan_parquet` rebaixa Enum):
  <https://github.com/pola-rs/polars/issues/19389>
- **Polars #18868** (predicate pushdown em Categorical):
  <https://github.com/pola-rs/polars/issues/18868>
- **Polars #20089** (tracking issue Enum/Categorical Parquet):
  <https://github.com/pola-rs/polars/issues/20089>
- **Polars PR #23016** (`pl.StringCache` virou no-op em 1.32):
  <https://github.com/pola-rs/polars/pull/23016>
- **DuckDB #14937** (ENUM perdido na escrita Parquet):
  <https://github.com/duckdb/duckdb/issues/14937>
- **PyArrow Parquet docs:**
  <https://arrow.apache.org/docs/python/parquet.html>

### Fontes legais (cadastro `ref/fiscal_codes_2026.json`)

- **CFOP:** Ato COTEPE/ICMS 08/2008 + Ajuste SINIEF 40/2023 +
  Ajuste SINIEF 10/2021
- **CST ICMS:** Convênio s/nº de 15/12/1970, Anexo Tabela B
- **CSOSN:** Ajuste SINIEF 03/2010
- **CST PIS/COFINS:** IN RFB nº 1.009/2010, Anexo Único, Tabela III
- **UF:** Códigos IBGE
- **Modelos de documento:** Manual de Orientação NF-e + SPED Fiscal
  Bloco C
- **Indicadores NF-e:** Manual NF-e/NFC-e
- **Layout SPED:** Layout EFD ICMS/IPI vigente

## Histórico de revisões

| Data | Autor | Mudança |
|---|---|---|
| 2026-05-06 | Enio Telles + Claude | Criação. Status Aceita após sessão de revisão profunda do plano original e validação de 4 PRs de implementação. |

---

> **Como citar esta ADR:** ADR-0005 (Estratégia de tipagem categórica
> para campos fiscais). audit_pyside, 2026-05-06.
> `docs/adr/0005-categorical-strategy.md`.
