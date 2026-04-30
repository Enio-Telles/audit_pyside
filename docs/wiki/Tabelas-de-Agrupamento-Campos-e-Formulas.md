# Tabelas de Agrupamento — Campos, Relações e Fórmulas

Esta página documenta `produtos_final`, `fontes_produtos` e `fatores_conversao`.

Essas camadas conectam descrições normalizadas a produtos consolidados, propagam o agrupamento para fontes operacionais e calculam fatores de conversão entre unidades.

---

## 1. `produtos_final_<cnpj>.parquet`

### Papel

Tabela canônica de produto por descrição principal normalizada. Consolida o grupo automático, os ajustes manuais e os atributos fiscais/cadastrais do produto.

### Relações

| Origem | Destino | Campos | Como interpretar |
|---|---|---|---|
| `descricao_produtos` | `produtos_final` | `id_descricao`, `descricao_normalizada` | A descrição normalizada vira uma linha de produto detalhado. |
| `item_unidades` | `produtos_final` | `descricao_normalizada`, unidades, NCM/CEST | Apoia a unidade sugerida e atributos fiscais. |
| `produtos_final` | `fontes_produtos` | `id_agrupado`, `descr_padrao`, `ncm/cest/co_sefin` | Propaga atributos consolidados para fontes brutas. |
| `produtos_final` | `fatores_conversao` | `id_agrupado`, `unid_ref_sugerida` | Define produto e unidade de referência sugerida. |
| `produtos_final` | `movimentacao_estoque` | `id_agrupado`, atributos fiscais | Dá identidade fiscal ao movimento de estoque. |

### Campos

| Campo | Tipo | Explicação | Relação/Fórmula |
|---|---|---|---|
| `id_descricao` | `str` | Chave da descrição principal normalizada. | Elo entre `descricao_produtos` e `produtos_final`. |
| `id_agrupado` | `str` | Chave vigente do produto agrupado. | Chave principal para fontes, fatores, estoque e análises. |
| `id_agrupado_base` | `str` | Chave automática do grupo-base. | Deriva da igualdade de `descricao_normalizada`. |
| `descricao_normalizada` | `str` | Descrição principal limpa. | Base do agrupamento automático. |
| `descricao` | `str` | Descrição principal de origem da camada detalhada. | Apoio humano para revisão. |
| `descricao_final` | `str` | Descrição final consolidada da linha. | Usada para apresentação detalhada. |
| `descr_padrao` | `str` | Descrição padrão eleita para o grupo vigente. | Propagada para fontes, estoque e relatórios. |
| `ncm_final` | `str` | NCM consolidado da linha. | Apoia eleição ou propagação do NCM padrão. |
| `cest_final` | `str` | CEST consolidado da linha. | Apoia classificação fiscal e ST. |
| `gtin_final` | `str` | GTIN consolidado da linha. | Apoia identificação cadastral do produto. |
| `co_sefin_final` | `str` | Código SEFIN consolidado da linha. | Apoia classificação SITAFE. |
| `co_sefin_padrao` | `str` | Código SEFIN padrão do grupo. | Pode originar `co_sefin_agr`. |
| `unid_ref_sugerida` | `str` | Unidade de referência sugerida. | Consumida por `fatores_conversao`. |
| `criterio_agrupamento` | `str` | Critério efetivo do agrupamento. | Explica se agrupou por regra automática/manual. |
| `origem_agrupamento` | `str` | Origem do agrupamento vigente. | Auditoria do agrupamento. |

### Fórmulas / regras

Grupo-base automático:

```text
id_agrupado_base = hash(descricao_normalizada)
```

Regra vigente com ajuste manual:

```text
se existe override manual para id_descricao ou descricao_normalizada:
    id_agrupado = id_agrupado_manual
senão:
    id_agrupado = id_agrupado_base
```

Eleição de descrição padrão:

```text
descr_padrao = descrição mais representativa do grupo vigente
```

A representatividade pode considerar frequência, qualidade cadastral e revisão manual. Não trate a frequência como verdade fiscal isolada.

---

## 2. `fontes_produtos` / arquivos `*_agr_<cnpj>.parquet`

### Papel

Não é uma tabela única. É uma camada que enriquece fontes operacionais com `id_agrupado` e atributos padronizados.

Exemplos de saídas:

```text
c170_agr_<cnpj>.parquet
bloco_h_agr_<cnpj>.parquet
nfe_agr_<cnpj>.parquet
nfce_agr_<cnpj>.parquet
```

### Relações

| Origem | Destino | Campos | Como interpretar |
|---|---|---|---|
| `map_produto_agrupado` | `fontes_produtos` | `codigo_fonte`, `descricao_normalizada`, `id_agrupado` | Mapa de ligação entre fonte e produto. |
| `produtos_final` | `fontes_produtos` | `id_agrupado`, atributos padrão | Injeta descrição, NCM, CEST, SEFIN e unidade sugerida. |
| `fontes_produtos` | `c170_xml` / `c176_xml` | `codigo_fonte`, `id_agrupado` | Enriquecimento fiscal posterior. |
| `fontes_produtos` | `movimentacao_estoque` | `id_agrupado`, atributos fiscais | Apoia a montagem do estoque por produto. |

### Campos

| Campo | Tipo | Explicação | Relação/Fórmula |
|---|---|---|---|
| `id_agrupado` | `str` | Produto agrupado atribuído à linha da fonte. | Vem do mapa/produto final. |
| `codigo_fonte` | `str` | Chave operacional da fonte. | Vínculo preferencial com `map_produto_agrupado`. |
| `descricao_normalizada` | `str` | Descrição limpa usada como fallback. | Usada quando `codigo_fonte` não resolve. |
| `descr_padrao` | `str` | Descrição padrão do grupo. | Vem de `produtos_final`. |
| `ncm_padrao` | `str` | NCM padrão do grupo. | Propagado para estoque e relatórios. |
| `cest_padrao` | `str` | CEST padrão do grupo. | Propagado para estoque e análise ST. |
| `co_sefin_agr` | `str` | Código SEFIN agrupado. | Chave de cruzamento com SITAFE. |
| `unid_ref_sugerida` | `str` | Unidade de referência sugerida. | Ajuda no cálculo de fatores. |

### Regras de vínculo

```text
1. tentar match por codigo_fonte
2. se não houver match, tentar por descricao_normalizada
3. se houver ambiguidade, registrar auditoria
4. se continuar sem id_agrupado, gerar log de faltantes e remover da saída enriquecida
```

### Relação com rastreabilidade

```text
fonte bruta.codigo_fonte
  -> map_produto_agrupado.codigo_fonte
  -> map_produto_agrupado.id_agrupado
  -> produtos_final.id_agrupado
```

---

## 3. `fatores_conversao_<cnpj>.parquet`

### Papel

Define como converter as unidades usadas nas fontes para uma unidade de referência comum por produto agrupado.

### Relações

| Origem | Destino | Campos | Como interpretar |
|---|---|---|---|
| `item_unidades` | `fatores_conversao` | `id_item_unid`, `descricao_normalizada`, `unid`, compras/vendas | Base para preços médios por unidade. |
| `produtos_final` | `fatores_conversao` | `id_agrupado`, `unid_ref_sugerida`, `descr_padrao` | Define produto e unidade sugerida. |
| `fatores_conversao` | `c170_xml` / `c176_xml` | `id_agrupado`, `unid`, `fator` | Converte quantidades e valores unitários. |
| `fatores_conversao` | `movimentacao_estoque` | `id_agrupado`, `unid_ref`, `fator` | Padroniza o saldo físico. |

### Campos

| Campo | Tipo | Explicação | Relação/Fórmula |
|---|---|---|---|
| `id_agrupado` | `str` | Produto agrupado. | Chave principal do fator. |
| `id_produtos` | `str` | Alias histórico preenchido com `id_agrupado`. | Compatibilidade legada. |
| `descr_padrao` | `str` | Descrição padrão do produto. | Vem de `produtos_final`. |
| `unid` | `str` | Unidade original da linha. | Ex.: UN, CX, KG. |
| `unid_ref` | `str` | Unidade de referência efetiva. | Unidade para a qual a quantidade será convertida. |
| `unid_ref_override` | `str` | Unidade de referência manual. | Tem prioridade quando preenchida. |
| `fator` | `float` | Fator efetivo de conversão. | `qtd_convertida = qtd_original * fator`. |
| `fator_override` | `float` | Fator manual preservado. | Tem prioridade sobre cálculo automático. |
| `fator_manual` | `bool` | Indicador de fator manual. | Verdadeiro quando `fator` vem de override. |
| `unid_ref_manual` | `bool` | Indicador de unidade manual. | Verdadeiro quando `unid_ref` vem de override. |
| `preco_medio` | `float` | Preço médio usado como base. | Derivado de compra ou venda. |
| `origem_preco` | `str` | Origem do preço: `COMPRA`, `VENDA`, `SEM_PRECO`. | Auditoria da confiabilidade do fator. |
| `fator_origem` | `str` | Origem do fator: `manual`, `preco`, `fallback_sem_preco`, etc. | Auditoria do cálculo. |

### Fórmulas

Preço médio de compra:

```text
preco_medio_compra = compras / qtd_compras
```

Preço médio de venda:

```text
preco_medio_venda = vendas / qtd_vendas
```

Escolha do preço:

```text
se existe preco_medio_compra:
    preco_medio = preco_medio_compra
senão se existe preco_medio_venda:
    preco_medio = preco_medio_venda
senão:
    origem_preco = SEM_PRECO
```

Escolha da unidade de referência:

```text
se existe unid_ref_override:
    unid_ref = unid_ref_override
senão se existe unid_ref_sugerida:
    unid_ref = unid_ref_sugerida
senão:
    unid_ref = unidade com maior movimentação agregada
```

Fator automático por preço:

```text
fator = preco_medio_da_unidade / preco_medio_da_unid_ref
```

Prioridade do fator manual:

```text
se existe fator_override:
    fator = fator_override
    fator_manual = true
senão:
    fator = fator_calculado
    fator_manual = false
```

Conversão posterior:

```text
qtd_padronizada = quantidade_original * fator
valor_unitario_padronizado = valor_unitario_original / fator
```

### Regras de preservação manual

Overrides manuais devem sobreviver a reprocessamentos:

```text
novo_fator = reconciliar(fator_antigo_manual, agrupamento_atual)
```

Se o agrupamento mudou, o processo precisa registrar reconciliação para auditoria.

---

## Relação resumida desta camada

```text
descricao_produtos.descricao_normalizada
  -> produtos_final.id_descricao / id_agrupado
  -> fontes_produtos.id_agrupado
  -> fatores_conversao.id_agrupado + unid + fator
```

## Campos críticos

| Campo | Motivo |
|---|---|
| `id_agrupado` | Chave mestra do produto. |
| `id_agrupado_base` | Preserva o agrupamento automático original. |
| `id_descricao` | Apoia ajustes manuais pontuais. |
| `codigo_fonte` | Melhor ligação entre fonte e produto. |
| `unid`, `unid_ref`, `fator` | Base das conversões físicas. |
| `fator_override`, `unid_ref_override` | Ajustes manuais que não podem ser perdidos. |
| `co_sefin_agr` / `co_sefin_padrao` | Base para classificação fiscal SITAFE. |
