# Tabelas de Agrupamento — Campos, Relações e Fórmulas

Esta página documenta `produtos_final`, `fontes_produtos` e `fatores_conversao`.

Essas camadas conectam descrições normalizadas a produtos consolidados, propagam o agrupamento para fontes operacionais e calculam fatores de conversão entre unidades.

## Metodologia canônica de identificação

A metodologia vigente de identificação de produtos é determinística e auditável. O agrupamento automático **não** usa similaridade textual livre/fuzzy como regra canônica.

Fluxo base:

```text
descrição bruta
  -> descricao_normalizada
  -> id_descricao
  -> id_agrupado_base
  -> id_agrupado
```

Regra central:

```text
descrições normalizadas iguais   -> mesmo id_agrupado_base
descrições normalizadas diferentes -> id_agrupado_base diferente
```

Ajustes humanos entram depois, por override manual, preservando a chave automática original em `id_agrupado_base`.

O `id_agrupado` é a identidade vigente do produto fiscal consolidado. Ele é propagado para fontes, fatores de conversão, movimentação de estoque e tabelas analíticas.

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

### Campos principais

| Campo | Tipo | Explicação | Relação/Fórmula |
|---|---|---|---|
| `id_descricao` | `str` | Chave da descrição principal normalizada. | Elo entre `descricao_produtos` e `produtos_final`. |
| `id_agrupado` | `str` | Chave vigente do produto agrupado. | Chave principal para fontes, fatores, estoque e análises. |
| `id_agrupado_base` | `str` | Chave automática do grupo-base. | Deriva de SHA1 da `descricao_normalizada`. |
| `descricao_normalizada` | `str` | Descrição principal limpa. | Base do agrupamento automático determinístico. |
| `descr_padrao` | `str` | Descrição padrão do grupo vigente. | Propagada para fontes, estoque e relatórios. |
| `ncm_final` | `str` | NCM consolidado da linha. | Apoia eleição ou propagação do NCM padrão. |
| `cest_final` | `str` | CEST consolidado da linha. | Apoia classificação fiscal e ST. |
| `gtin_final` | `str` | GTIN consolidado da linha. | Apoia identificação cadastral do produto. |
| `co_sefin_final` | `str` | Código SEFIN consolidado da linha. | Apoia classificação SITAFE. |
| `unid_ref_sugerida` | `str` | Unidade de referência sugerida. | Consumida por `fatores_conversao`. |
| `criterio_agrupamento` | `str` | Critério efetivo do agrupamento. | Explica se agrupou por regra automática/manual. |
| `origem_agrupamento` | `str` | Origem do agrupamento vigente. | Auditoria do agrupamento. |

### Fórmulas / regras

Grupo-base automático:

```text
id_agrupado_base = "id_agrupado_auto_" + SHA1(descricao_normalizada)[:12]
```

Regra vigente com ajuste manual:

```text
se existe override manual para id_descricao:
    id_agrupado = id_agrupado_manual
senão se existe override manual para descricao_normalizada:
    id_agrupado = id_agrupado_manual
senão:
    id_agrupado = id_agrupado_base
```

Interpretação:

```text
id_agrupado_base = agrupamento automático original
id_agrupado      = agrupamento vigente, após override manual quando houver
```

Eleição de descrição padrão:

```text
descr_padrao = descrição padrão selecionada para o grupo vigente
```

No código atual, a escolha de `descr_padrao` é simples e deriva da primeira descrição disponível no grupo, enquanto NCM, CEST, GTIN e SEFIN usam moda. Se o projeto decidir que `descr_padrao` deve considerar frequência, qualidade cadastral ou revisão manual, isso deve ser implementado e testado em PR própria.

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

### Regras de vínculo

```text
1. tentar match por codigo_fonte
2. se não houver match, tentar por descricao_normalizada
3. se houver ambiguidade, registrar auditoria
4. se continuar sem id_agrupado, gerar log de faltantes e remover da saída enriquecida
```

A `descricao_normalizada` é fallback controlado. Ela não substitui o `codigo_fonte` como chave preferencial.

### Relação com rastreabilidade

```text
fonte bruta.codigo_fonte
  -> map_produto_agrupado.codigo_fonte
  -> map_produto_agrupado.id_agrupado
  -> produtos_final.id_agrupado
```

Campos principais:

| Campo | Explicação |
|---|---|
| `id_agrupado` | Produto agrupado atribuído à linha da fonte. |
| `codigo_fonte` | Chave operacional preferencial da fonte. |
| `descricao_normalizada` | Descrição limpa usada como fallback controlado. |
| `descr_padrao` | Descrição padrão do grupo. |
| `ncm_padrao` | NCM padrão do grupo. |
| `cest_padrao` | CEST padrão do grupo. |
| `co_sefin_agr` | Código SEFIN agrupado. |
| `unid_ref_sugerida` | Unidade de referência sugerida. |

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

Campos principais:

| Campo | Explicação |
|---|---|
| `id_agrupado` | Produto agrupado. |
| `id_produtos` | Alias histórico preenchido com `id_agrupado`. |
| `descr_padrao` | Descrição padrão do produto. |
| `unid` | Unidade original da linha. |
| `unid_ref` | Unidade de referência efetiva. |
| `unid_ref_override` | Unidade de referência manual. |
| `fator` | Fator efetivo de conversão. |
| `fator_override` | Fator manual preservado. |
| `fator_manual` | Indica se o fator veio de override manual. |
| `unid_ref_manual` | Indica se a unidade de referência veio de override manual. |
| `preco_medio` | Preço médio usado como base. |
| `origem_preco` | Origem do preço: `COMPRA`, `VENDA`, `SEM_PRECO`. |
| `fator_origem` | Origem do fator: `manual`, `preco`, `fallback_sem_preco`, etc. |

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

## Frase canônica

```text
A metodologia do audit_pyside identifica produtos por normalização determinística da descrição, cria uma chave automática estável, permite correção humana controlada via override, propaga a identidade às fontes por codigo_fonte com fallback auditado por descrição, e padroniza quantidades com fatores de conversão preservando ajustes manuais.
```
