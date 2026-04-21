# Análise Profunda — `audit_pyside` (repositório Enio-Telles)
**Data:** 17/04/2026  
**Eixos:** Agregação de Produtos · Conversão de Unidades · Movimentação/Estoque

---

## 1. Resumo Executivo

O projeto está em estado de **maturidade intermediária**: a arquitetura geral é sólida, os fluxos principais estão corretos e a cobertura de testes de saldo sequencial é boa. Contudo foram identificados **dois bugs latentes críticos**, **quatro divergências de alta severidade** entre método e implementação, e **gaps significativos de teste** nos domínios mais complexos (agrupamento manual, reconciliação de overrides em reprocessamento).

**Veredicto final:** `método consistente mas parcialmente aplicado`

A documentação é internamente contraditória em um ponto importante (`q_conv` em ESTOQUE FINAL), e a implementação diverge da documentação nesse mesmo ponto. O código atual é funcionalmente correto, mas a documentação precisa ser reconciliada.

---

## 1.1 Atualizacao de Runtime - 20/04/2026

O roteamento de `produtos_final` foi simplificado apos a auditoria original.

Cadeia vigente:

```text
orquestrador_pipeline.py
  -> "transformacao.produtos_final_v2:gerar_produtos_final"
    -> src/transformacao/produtos_final_v2.py                  (proxy canonico)
      -> src/transformacao/rastreabilidade_produtos/produtos_final_v2.py
        -> src/transformacao/rastreabilidade_produtos/_produtos_final_impl.py
```

Entry points legados agora apenas delegam para a mesma implementacao:

- `src/transformacao/produtos_final.py`
- `src/transformacao/04_produtos_final.py`
- `src/transformacao/rastreabilidade_produtos/04_produtos_final.py`

Com isso, o risco descrito abaixo sobre `importlib` e dependencia do nome fisico `04_produtos_final.py` nao e mais o caminho principal de runtime.

## 2. Cadeia de Roteamento Confirmada

Antes de qualquer diagnóstico, é essencial entender a cadeia real de execução — que tem até **4 níveis de indireção**:

```
orquestrador_pipeline.py
  → "transformacao.produtos_final_v2:gerar_produtos_final"       (proxy de 2 linhas)
    → rastreabilidade_produtos/produtos_final_v2.py              (loader via importlib)
      → rastreabilidade_produtos/04_produtos_final.py            (código real ✓)

  → "transformacao.movimentacao_estoque:gerar_movimentacao_estoque"  (proxy)
    → movimentacao_estoque_pkg/movimentacao_estoque.py               (código real ✓)

  → "transformacao.fatores_conversao:calcular_fatores_conversao" (proxy)
    → rastreabilidade_produtos/fatores_conversao.py              (código real ✓)

  → "transformacao.fontes_produtos:gerar_fontes_produtos"        (proxy)
    → rastreabilidade_produtos/fontes_produtos.py                (código real ✓)
```

**Risco:** `04_produtos_final.py` é carregado por nome de arquivo via `importlib.util.spec_from_file_location`. Qualquer renomeação desse arquivo quebra silenciosamente toda a cadeia de produtos sem erro de importação antecipado.

---

## 3. Diagnóstico da Agregação de Produtos

### 3.1 Agrupamento automático por `descricao_normalizada`

**Método (doc):** "produtos com a mesma `descricao_normalizada` são agrupados automaticamente no mesmo `id_agrupado`."

**Código (`_aplicar_agrupamento_manual` sem arquivo manual):**
```python
# Fallback: 1 id_agrupado para cada id_descricao
return df_descricoes.with_columns(
    pl.col("id_descricao").alias("id_agrupado")
)
```

**Aparentemente divergente, mas funcionalmente equivalente:** `id_descricao` é gerado em `03_descricao_produtos.py` como:
```python
.sort(["descricao_normalizada", "descricao"], nulls_last=True)
.with_columns(pl.format("id_descricao_{}", pl.col("seq")).alias("id_descricao"))
```
após um `group_by("descricao_normalizada")`. Portanto `id_descricao` é 1:1 com `descricao_normalizada`. **Implementação semanticamente correta**, mas a equivalência não é óbvia pelo código.

### 3.2 Mapeamento manual tem precedência

**Método (doc):** "O pipeline prioriza o mapeamento manual antes da sistemática automática."

**Código (`_aplicar_agrupamento_manual`):**
```python
caminho_manual = pasta_analises / f"mapa_agrupamento_manual_{cnpj}.parquet"
if not caminho_manual.exists():
    return df_descricoes.with_columns(pl.col("id_descricao").alias("id_agrupado"))
df_manual = pl.read_parquet(caminho_manual).select(["id_descricao", "id_agrupado"])
return df_descricoes.join(df_manual, on="id_descricao", how="left").with_columns(
    pl.coalesce([pl.col("id_agrupado"), pl.col("id_descricao")]).alias("id_agrupado")
)
```

**Status:** ✅ Implementado corretamente. Manual tem precedência; itens sem entrada no mapa manual recebem fallback automático via `coalesce`.

**Gap:** Não há testes para o cenário com `mapa_agrupamento_manual` presente.

### 3.3 Tabela ponte — `codigo_fonte` populado condicionalmente

**Método (doc):** "A tabela ponte relaciona cada `codigo_fonte` ao respectivo `id_agrupado`... `codigo_fonte` é a chave de vínculo com a fonte bruta."

**Código (`_construir_tabela_ponte`):**
```python
if "lista_codigo_fonte" not in df_descricoes.columns:
    return (
        df_descricoes.select([
            pl.col("id_descricao").alias("chave_produto"),
            "id_agrupado",
            pl.lit(None, dtype=pl.Utf8).alias("codigo_fonte"),  # ← NULO!
            "descricao_normalizada",
        ]).unique()
    )
```

**Risco:** Se `descricao_produtos` não contiver `lista_codigo_fonte`, toda a tabela ponte terá `codigo_fonte = None`. Em `fontes_produtos.py`, o join primário por `codigo_fonte` falha silenciosamente e o sistema cai inteiramente no fallback por `descricao_normalizada`.

**Verificação:** `lista_codigo_fonte` é populada em `03_descricao_produtos.py` (linha 167), então quando o pipeline completo roda, a coluna existe. O risco é real em reprocessamentos parciais ou entradas sem `codigo_fonte` na extração SQL.

### 3.4 Fio de ouro — `id_linha_origem`

**Método (doc):** `linha original → id_linha_origem → codigo_fonte → id_agrupado → tabelas analíticas`

**Código (`fontes_produtos.py/_preservar_colunas_rastreabilidade`):**
```python
if "id_linha_origem" in df_src.columns:
    exprs.append(pl.col("id_linha_origem").cast(pl.Utf8, strict=False))
# Se não existir: simplesmente não inclui
```

**Risco alto:** O primeiro elo do fio de ouro (`id_linha_origem`) **só existe se a extração SQL o gerar**. Não há verificação ou aviso quando ausente. O pipeline não falha, mas a rastreabilidade fica incompleta sem que o usuário perceba.

### 3.5 Log de merge/unmerge manual

**Método (doc):** "Cada agrupamento manual deve registrar em log: grupo de destino, grupos de origem, itens envolvidos, versão do agrupamento, snapshot necessário para reversão."

**Código:** Não encontrado nenhum módulo de log de merge/unmerge na análise. A API está documentada (`POST /aggregation/merge`, `POST /aggregation/unmerge`) mas os módulos do backend que implementariam o log de auditoria não foram verificados no escopo desta análise.

---

## 4. Diagnóstico da Conversão de Unidades

### 4.1 Vínculo item_unidades → id_agrupado

**Método (doc):** "A base `item_unidades` é normalizada e ligada a `produtos_final` por `descricao_normalizada`."

**Código (`fatores_conversao.py/_carregar_vinculo_produto_canonico`):**
```python
# Prioridade 1: map_produto_agrupado (tabela ponte)
df_map_raw = pl.scan_parquet(arq_pont)
    .select(["descricao_normalizada", "id_agrupado"])
    ...

# Prioridade 2: produtos_final por descricao_normalizada
df_final_base, _ = _construir_vinculo_unico_por_descricao(df_final, ...)
```

**Status:** Vínculo por `descricao_normalizada` em ambos os casos. O `codigo_fonte` **não é usado diretamente no vínculo de fatores**. Isso é uma fragilidade: dois produtos com descrições idênticas de fornecedores diferentes gerarão ambiguidade que o sistema resolve com o filtro `filter(n_unique == 1)` — descartando ambos silenciosamente.

**Confirmado pelo código:**
```python
df_grouped.filter(pl.col("__qtd_ids__") == 1)  # descarta ambíguos
```

Se a ambiguidade existir, o produto fica sem fator de conversão e recebe `fator = 1.0` (fallback), sem aviso explícito ao auditor.

### 4.2 Preservação de overrides manuais em reprocessamento

**Método (doc):** "Reprocessamentos devem tentar preservar essas escolhas em vez de descartá-las."

**Código (`_reconciliar_fatores_existentes_com_agrupamento_atual`):** Implementação sofisticada e correta:

1. Identifica overrides manuais (`fator_manual=True` ou `unid_ref_manual=True`)
2. Tenta remapear para o agrupamento canônico atual por `descricao_normalizada`
3. Loga remapeados vs descartados em `log_reconciliacao_overrides_fatores_<cnpj>.parquet`
4. Preserva órfãos (manuais sem par no novo cálculo)

**Status:** ✅ Implementação correta e robusta.

**Gap de teste:** Não há testes para `_reconciliar_fatores_existentes_com_agrupamento_atual` nem para preservação de órfãos.

### 4.3 Classificação `fator_origem`

**Método (doc):** Coluna `fator_origem` com valores `manual`, `fallback`, `preco`.

**Código:** ✅ Implementado exatamente conforme documentado.

### 4.4 Unidade de referência

**Método:** `unid_ref = unid_ref_override OR unid_ref_manual OR unid_ref_auto`

**Código:**
```python
pl.coalesce([
    pl.col("unid_ref_override"),   # override explícito do parquet existente
    pl.col("unid_ref_manual"),     # de unid_ref_sugerida em produtos_final
    pl.col("unid_ref_auto")        # maior qtd_mov_total
]).alias("unid_ref")
```

**Status:** ✅ Correto. Nomenclatura `unid_ref_manual` é reutilizada para dois conceitos distintos (override e sugestão), o que gera alguma confusão de leitura mas comportamento correto.

---

## 5. Diagnóstico do Estoque

### 5.1 `q_conv` em ESTOQUE FINAL — **DIVERGÊNCIA CRÍTICA**

Esta é a divergência mais importante encontrada. O documento é **internamente contraditório**:

**Seção "Semântica das quantidades" (doc linha ~205):**
> Em linhas de `3 - ESTOQUE FINAL`:
> - `q_conv` **pode permanecer preenchido** para auditoria row-level;
> - `q_conv_fisica = 0`;
> - `__q_conv_sinal__ = 0`;

**Seção "Estoque final auditado" (doc linha ~214):**
> `3 - ESTOQUE FINAL` não altera o saldo físico:
> - **`q_conv` permanece `0`** (não impacta saldo);

**O código implementa a primeira versão (q_conv preenchido):**
```python
.when(pl.col("Tipo_operacao").str.starts_with("3 - ESTOQUE FINAL"))
.then(q_conv_valido_expr)   # ← PREENCHIDO, não zero
.alias("q_conv"),
```

**Análise:** O código está **funcionalmente correto** para o objetivo pretendido: `q_conv` preenchido permite auditoria row-level do inventário, enquanto `__q_conv_sinal__ = 0` garante que o saldo sequencial não é afetado. A seção "Estoque final auditado" da doc está desatualizada.

**Risco prático:** Um auditor que leia apenas a seção "Estoque final auditado" esperará `q_conv=0` e ficará confuso ao ver valores. Camadas downstream que consumam `q_conv` sem saber dessa semântica podem cometer erros de dupla contagem.

### 5.2 `__q_conv_sinal__` e saldo sequencial

**Status:** ✅ Correto. `__q_conv_sinal__ = 0` para ESTOQUE FINAL, conforme documentado. O núcleo Numba trata `tipo_int == 2` com `pass` (não altera saldo). Devoluções retornam quantidade sem alterar custo médio.

### 5.3 Entradas desacobertadas

**Status:** ✅ Correto. Quando saída faria saldo ficar negativo:
```python
entr_desac = -saldo_qtd  # captura o excesso
saldo_qtd = 0.0
saldo_valor = 0.0
custo_medio = 0.0
```

### 5.4 `_salvar_log_vinculo_produto` — **BUG LATENTE CRÍTICO**

**Código (`movimentacao_estoque.py`, linhas 215 e 228):**
```python
_salvar_log_vinculo_produto(
    {**resumo, "resultado": {"qtd_descricoes_vinculadas": 0}},
    pasta_analises,
    cnpj,
)
```

**Problema:** A função `_salvar_log_vinculo_produto` **não está definida** em `movimentacao_estoque_pkg/movimentacao_estoque.py`. Não há `from X import _salvar_log_vinculo_produto` no arquivo. Isso resulta em `NameError` quando o path de "nenhuma base disponível" for atingido.

**Verificado:** grep em todo o arquivo não encontra `def _salvar_log_vinculo_produto`. A função existe em `rastreabilidade_produtos/fatores_conversao.py` mas não é importada aqui.

### 5.5 Filtro por fonte

**Status:** ✅ Correto. `c170 → ENTRADA`, `nfe/nfce → SAIDAS`, `bloco_h → qualquer tipo`. Testado.

### 5.6 Geração de eventos ESTOQUE INICIAL/FINAL

**Status:** ✅ Correto. Anos sem inventário 31/12 recebem ESTOQUE FINAL gerado (qtd=0). Cada ESTOQUE FINAL gera um ESTOQUE INICIAL no dia seguinte. Fonte dos eventos gerados = "gerado". Testado.

### 5.7 `periodo_inventario`

**Status:** ✅ Correto. Calculado por `cum_sum()` dos `0 - ESTOQUE INICIAL` por `id_agrupado`. Dois cálculos independentes: `_calcular_saldo_estoque_anual` por (id_agrupado, ano) e `_calcular_saldo_estoque_periodo` por (id_agrupado, periodo_inventario).

---

## 6. Tabela de Inconsistências Método × Implementação

| # | Arquivo | Severidade | O que o método diz | O que o código faz | Risco prático |
|---|---------|------------|--------------------|--------------------|---------------|
| I1 | `movimentacao_estoque.py` L215/228 | **CRÍTICA** | — | Chama `_salvar_log_vinculo_produto` que não existe | `NameError` em runtime no path de falha do vínculo |
| I2 | `mov_estoque.md` vs `movimentacao_estoque.py` | **ALTA** | `q_conv = 0` em ESTOQUE FINAL (seção "Estoque final auditado") | `q_conv = q_conv_valido_expr` (preenchido) | Confusão de auditor; dupla contagem em downstream ingênuo |
| I3 | `_construir_tabela_ponte` | **ALTA** | `codigo_fonte` é chave primária da ponte | `codigo_fonte = None` quando `lista_codigo_fonte` ausente | Vínculo cai para heurística de `descricao_normalizada` sem aviso |
| I4 | `fatores_conversao.py` | **ALTA** | Vínculo por `codigo_fonte` (doc agregação) | Vínculo por `descricao_normalizada` exclusivamente | Descrições idênticas de fornecedores distintos são descartadas silenciosamente |
| I5 | `fontes_produtos.py` L153 | **ALTA** | Tabela ponte é a base efetiva | `.unique(subset=["codigo_fonte"])` mantém apenas primeiro match | Colisão silenciosa quando um `codigo_fonte` mapeia para múltiplos `id_agrupado` |
| I6 | `fontes_produtos.py` | **ALTA** | `id_linha_origem` é elo do fio de ouro | Só preservado se já existir na fonte | Fio de ouro quebrado no primeiro elo sem aviso |
| I7 | `produtos_final_v2.py` | **MÉDIA** | Módulo estável | Carrega `04_produtos_final.py` por nome de arquivo via `importlib` | Renomeação do arquivo quebra silenciosamente toda a cadeia |
| I8 | `_aplicar_agrupamento_manual` | **BAIXA** | Agrupamento automático por `descricao_normalizada` | `id_agrupado = id_descricao` (equivalente, mas não óbvio) | Confusão de leitura; sem impacto funcional |

---

## 7. Melhorias Específicas Propostas

### 7.1 Melhorias de Método

**M1.** Reconciliar a seção "Estoque final auditado" do `mov_estoque.md`: remover a afirmação `q_conv permanece 0` e substituir por:
> "Em linhas de ESTOQUE FINAL, `q_conv` permanece preenchido para auditoria row-level; `q_conv_fisica = 0` e `__q_conv_sinal__ = 0` garantem que o saldo sequencial não seja afetado."

**M2.** Documentar explicitamente em `conversao_unidades.md` que o vínculo de fatores usa `descricao_normalizada` (não `codigo_fonte`) e o comportamento quando há ambiguidade (descarte silencioso → adicionar log explícito).

**M3.** Adicionar à documentação de rastreabilidade a condição de obrigatoriedade de `id_linha_origem` na extração SQL, com exemplo de query.

### 7.2 Melhorias de Implementação

**P1 (CRÍTICA):** Definir ou importar `_salvar_log_vinculo_produto` em `movimentacao_estoque.py`.

**P2 (ALTA):** Em `_construir_tabela_ponte`, adicionar aviso quando `lista_codigo_fonte` está ausente e a ponte ficará sem `codigo_fonte`.

**P3 (ALTA):** Em `fatores_conversao.py`, quando descrição é ambígua (descartada), emitir aviso explícito com lista dos produtos afetados em vez de silenciar.

**P4 (ALTA):** Em `fontes_produtos.py`, substituir `.unique(subset=["codigo_fonte"])` por detecção e log de colisões:
```python
colisoes = df_mapa_codigo.group_by("codigo_fonte").agg(
    pl.col("id_agrupado_codigo").n_unique().alias("n_ids")
).filter(pl.col("n_ids") > 1)
if not colisoes.is_empty():
    salvar_log(colisoes, ...)  # não silenciar
```

**P5 (MÉDIA):** Em `produtos_final_v2.py`, usar importação explícita por pacote em vez de `importlib` por nome de arquivo:
```python
# Substituir importlib por:
from transformacao.rastreabilidade_produtos._04_produtos_final import (
    produtos_agrupados, gerar_produtos_final
)
```
Renomear o arquivo para `_04_produtos_final.py` ou `produtos_final_impl.py`.

**P6 (MÉDIA):** Em `_aplicar_agrupamento_manual`, documentar explicitamente no código que `id_descricao` é 1:1 com `descricao_normalizada`.

**P7 (BAIXA):** Renomear variável interna `unid_ref_manual` que aparece com dois significados (override explícito vs sugestão) para evitar confusão de leitura.

### 7.3 Melhorias de Observabilidade/Auditoria

**O1:** Padronizar arquivos de log de auditoria com schema fixo e prefixo `audit_`:
```
audit_vinculo_descricao_ambigua_<cnpj>.parquet
audit_sem_id_agrupado_<fonte>_<cnpj>.parquet
audit_codigo_fonte_colisao_<cnpj>.parquet
audit_reconciliacao_overrides_<cnpj>.parquet
```

**O2:** Gerar um arquivo `audit_rastreabilidade_summary_<cnpj>.json` por execução do pipeline contendo:
- % de linhas com `id_linha_origem` preenchido por fonte
- % de linhas com `id_agrupado` resolvido por `codigo_fonte` vs `descricao_normalizada`
- qtd de ambiguidades de descrição descartadas nos fatores

---

## 8. Lista Priorizada de Correções

| Prioridade | Ação | Arquivo | Esforço |
|------------|------|---------|---------|
| 🔴 1 | Definir/importar `_salvar_log_vinculo_produto` | `movimentacao_estoque_pkg/movimentacao_estoque.py` | 5 min |
| 🔴 2 | Reconciliar seção "Estoque final auditado" no doc | `docs/mov_estoque.md` | 10 min |
| 🟠 3 | Logar ambiguidades de `descricao_normalizada` em fatores | `rastreabilidade_produtos/fatores_conversao.py` | 30 min |
| 🟠 4 | Logar colisões de `codigo_fonte` em fontes_produtos | `rastreabilidade_produtos/fontes_produtos.py` | 30 min |
| 🟠 5 | Aviso quando `lista_codigo_fonte` ausente na tabela ponte | `rastreabilidade_produtos/04_produtos_final.py` | 15 min |
| 🟡 6 | Substituir `importlib` por importação direta em `produtos_final_v2.py` | `rastreabilidade_produtos/produtos_final_v2.py` | 20 min |
| 🟡 7 | Testes: agrupamento manual com `mapa_agrupamento_manual` | `tests/` | 45 min |
| 🟡 8 | Testes: reconciliação de overrides em reprocessamento | `tests/` | 45 min |
| 🟡 9 | Testes: preservação de `id_linha_origem` nas fontes | `tests/` | 30 min |
| 🟢 10 | `audit_rastreabilidade_summary.json` por pipeline run | múltiplos | 1h |

---

## 9. Sugestão de Patches por Arquivo

### 9.1 `movimentacao_estoque_pkg/movimentacao_estoque.py` — BUG CRÍTICO

```python
# ADICIONAR ao bloco de imports, após os imports existentes:
import json

# ADICIONAR função antes de _carregar_vinculo_produto_canonico:
def _salvar_log_vinculo_produto(resumo: dict, pasta_analises: Path, cnpj: str) -> None:
    """Persiste resumo do vínculo de produto para rastreabilidade."""
    caminho = pasta_analises / f"log_vinculo_produto_estoque_{cnpj}.json"
    try:
        with open(caminho, "w", encoding="utf-8") as f:
            json.dump(resumo, f, ensure_ascii=False, indent=2)
    except Exception as exc:
        rprint(f"[yellow]Aviso: falha ao salvar log de vínculo: {exc}[/yellow]")
```

### 9.2 `rastreabilidade_produtos/04_produtos_final.py` — aviso ponte sem `codigo_fonte`

```python
def _construir_tabela_ponte(df_descricoes: pl.DataFrame) -> pl.DataFrame:
    if "lista_codigo_fonte" not in df_descricoes.columns:
        rprint(
            "[yellow]Aviso: lista_codigo_fonte ausente em descricao_produtos. "
            "A tabela ponte terá codigo_fonte=None em todas as linhas. "
            "O vínculo de fontes usará apenas descricao_normalizada (fallback).[/yellow]"
        )
        return (
            df_descricoes.select([
                pl.col("id_descricao").alias("chave_produto"),
                "id_agrupado",
                pl.lit(None, dtype=pl.Utf8).alias("codigo_fonte"),
                "descricao_normalizada",
            ]).unique()
        )
    # ... restante igual
```

### 9.3 `rastreabilidade_produtos/fatores_conversao.py` — log de ambiguidades

```python
# Em _construir_vinculo_unico_por_descricao, após df_grouped:
df_ambiguos = df_grouped.filter(pl.col("__qtd_ids__") > 1)
if df_ambiguos.height > 0:
    rprint(
        f"[yellow]Aviso ({origem}): {df_ambiguos.height} descrições mapeiam para "
        f"múltiplos id_agrupado e serão descartadas do vínculo de fatores.[/yellow]"
    )
    # Salvar log para auditoria:
    salvar_para_parquet(
        df_ambiguos,
        pasta_analises,  # passar pasta como parâmetro adicional
        f"audit_descricao_ambigua_fatores_{cnpj}.parquet",
    )
```

### 9.4 `docs/mov_estoque.md` — reconciliar q_conv

**Substituir** a seção "Estoque final auditado" (linhas ~212–221):

```markdown
## Estoque final auditado

`3 - ESTOQUE FINAL` não altera o saldo físico:

- `q_conv` **permanece preenchido** com a quantidade convertida declarada no inventário (para auditoria row-level);
- `q_conv_fisica = 0` (não representa movimento físico);
- `__q_conv_sinal__ = 0` (não altera o saldo sequencial);
- a quantidade declarada também fica em `__qtd_decl_final_audit__` (para agregação anual);
- `saldo_estoque_anual` não muda;
- `custo_medio_anual` não muda;
- `entr_desac_anual` permanece `0`.

Essa linha existe para auditoria de inventário. Camadas downstream que consumam `q_conv`
devem verificar `Tipo_operacao` antes de somar quantidades para evitar dupla contagem.
```

### 9.5 `rastreabilidade_produtos/produtos_final_v2.py` — substituir importlib

```python
"""Loader para 04_produtos_final.py via importação direta."""
# ANTES (frágil):
# _spec = util.spec_from_file_location("produtos_final_v2_impl", STUB_PATH)

# DEPOIS (robusto):
# Renomear 04_produtos_final.py para produtos_final_impl.py e importar:
from transformacao.rastreabilidade_produtos.produtos_final_impl import (  # noqa
    produtos_agrupados,
    gerar_produtos_final,
)
__all__ = ["produtos_agrupados", "gerar_produtos_final"]
```

---

## 10. Gaps de Testes e Testes a Criar

### 10.1 Gaps Identificados

| Gap | Módulo real | Risco |
|-----|-------------|-------|
| Agrupamento manual com `mapa_agrupamento_manual_<cnpj>.parquet` | `04_produtos_final.py` | Manual ignorado silenciosamente se arquivo tiver schema errado |
| Reconciliação de overrides em reprocessamento | `fatores_conversao.py / _reconciliar_...` | Overrides manuais perdidos após reprocessamento |
| Preservação de órfãos (manuais sem par no novo cálculo) | `fatores_conversao.py` | Overrides descartados sem evidência |
| Tabela ponte com `codigo_fonte = None` | `04_produtos_final.py` | Vínculo cai para heurística sem aviso |
| `_salvar_log_vinculo_produto` (após fix) | `movimentacao_estoque.py` | Falha silenciosa de log |
| `id_linha_origem` ausente na fonte | `fontes_produtos.py` | Fio de ouro quebrado sem aviso |
| `filtrar_movimentacoes_por_fonte` com `bloco_h` e tipos mistos | `movimentacao_estoque.py` | Filtro pass-through para `bloco_h` correto? |

### 10.2 Testes Prioritários a Criar

```python
# tests/test_agrupamento_manual.py

def test_mapeamento_manual_tem_precedencia_sobre_automatico():
    """Quando mapa_manual existe, o id_agrupado manual deve prevalecer."""
    # Criar df_descricoes com id_descricao diferente de id_agrupado manual
    # Criar arquivo mapa_agrupamento_manual temporário
    # Rodar _aplicar_agrupamento_manual
    # Assert: id_agrupado == valor do mapa manual

def test_fallback_quando_id_descricao_nao_esta_no_mapa_manual():
    """Itens sem entrada no mapa manual devem receber id_agrupado = id_descricao."""

def test_tabela_ponte_sem_lista_codigo_fonte_gera_codigo_fonte_nulo():
    """Quando lista_codigo_fonte ausente, codigo_fonte deve ser None e emitir aviso."""
```

```python
# tests/test_fatores_reconciliacao.py

def test_override_manual_preservado_em_reprocessamento():
    """Após reprocessamento, fator_manual=True deve ser mantido."""

def test_orfao_manual_preservado_quando_produto_some_do_novo_calculo():
    """Override de produto que saiu do novo pipeline deve ser mantido como órfão."""

def test_descricao_ambigua_nao_vincula_e_gera_log():
    """Descrição que mapeia para 2 id_agrupado diferentes deve ser descartada e logada."""
```

```python
# tests/test_rastreabilidade_fio_de_ouro.py

def test_id_linha_origem_preservado_quando_presente_na_fonte():
    """Fonte com id_linha_origem deve tê-lo nas saídas _agr."""

def test_pipeline_nao_falha_quando_id_linha_origem_ausente():
    """Fonte sem id_linha_origem deve processar normalmente (campo fica nulo)."""

def test_codigo_fonte_colisao_logada():
    """Quando codigo_fonte mapeia para 2 id_agrupado, colisão deve ser logada."""
```

---

## 11. Conclusão Final

```
VEREDICTO: método consistente mas parcialmente aplicado
```

**O que está correto:**
- Saldo sequencial com Numba: ✅ robusto e testado
- Devoluções sem alterar custo médio: ✅ correto
- ESTOQUE FINAL não altera saldo (via `__q_conv_sinal__=0`): ✅ correto
- Mapeamento manual com precedência: ✅ correto
- Preservação de overrides em reprocessamento: ✅ implementado
- Filtro de fonte por direção: ✅ correto e testado
- Geração de eventos sintéticos: ✅ correto e testado

**O que está divergente ou incompleto:**
- `_salvar_log_vinculo_produto` ausente em `movimentacao_estoque.py`: **BUG crítico**
- Documentação de `q_conv` em ESTOQUE FINAL: **contraditória internamente**
- Vínculo de fatores por `descricao_normalizada` com descarte silencioso de ambíguos: **risco alto**
- `codigo_fonte` na tabela ponte depende de condição nem sempre garantida: **risco alto**
- `id_linha_origem` como elo do fio de ouro: **não garantido pela implementação**
- Cadeia de importação via `importlib` por nome de arquivo: **frágil**
- Zero testes para agrupamento manual, reconciliação de overrides, ambiguidades de descrição

**Prioridade imediata (antes de qualquer auditoria fiscal):**
1. Corrigir `_salvar_log_vinculo_produto` (5 minutos, evita `NameError` em produção)
2. Reconciliar a seção "Estoque final auditado" no doc (evita confusão do auditor sobre `q_conv`)
3. Adicionar log de ambiguidades em `fatores_conversao.py` (torna o fallback visível)
