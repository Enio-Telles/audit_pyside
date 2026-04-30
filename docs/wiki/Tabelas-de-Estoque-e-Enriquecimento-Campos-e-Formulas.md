# Tabelas de Estoque e Enriquecimento — Campos, Relações e Fórmulas

Esta página documenta `c170_xml`, `c176_xml` e `movimentacao_estoque`.

Essas tabelas aplicam o agrupamento e os fatores de conversão aos registros fiscais, formando a movimentação cronológica de estoque.

---

## 1. `c170_xml_<cnpj>.parquet`

### Papel

Enriquece registros C170 do SPED com dados de XML, produto agrupado e fator de conversão.

### Relações

| Origem | Destino | Campos | Como interpretar |
|---|---|---|---|
| `fontes_produtos` | `c170_xml` | `codigo_fonte`, `id_agrupado` | Injeta produto agrupado na linha C170. |
| `fatores_conversao` | `c170_xml` | `id_agrupado`, `unid`, `fator`, `unid_ref` | Injeta conversão de unidade. |
| XML/NFe | `c170_xml` | `chave_acesso`, `num_doc`, `num_item` | Complementa informações do SPED com XML. |
| `c170_xml` | `movimentacao_estoque` | `id_agrupado`, `qtd_conv`, valores e datas | Alimenta movimentos fiscais. |

### Campos

| Campo | Tipo | Explicação | Relação/Fórmula |
|---|---|---|---|
| `id_linha_origem` | `str` | Chave de rastreabilidade da linha C170. | Permite voltar à linha original. |
| `id_agrupado` | `str` | Produto agrupado. | Vem de `fontes_produtos`. |
| `chave_acesso` | `str` | Chave da NF-e. | Liga SPED e XML. |
| `num_doc` | `str` | Número do documento. | Rastreabilidade documental. |
| `num_item` | `int` | Número do item. | Identifica a linha no documento. |
| `descricao` | `str` | Descrição do produto. | Apoio operacional e auditoria. |
| `ncm` | `str` | NCM do item. | Classificação fiscal. |
| `qtd` | `float` | Quantidade original. | Base para `qtd_conv`. |
| `qtd_conv` | `float` | Quantidade convertida. | `qtd * fator`. |
| `valor_unitario` | `float` | Valor unitário original. | Base para `valor_unitario_conv`. |
| `valor_unitario_conv` | `float` | Valor unitário convertido. | `valor_unitario / fator`. |
| `unid` | `str` | Unidade original. | Chave para buscar fator. |
| `unid_ref` | `str` | Unidade de referência. | Vem de `fatores_conversao`. |
| `fator` | `float` | Fator aplicado. | Vem de `fatores_conversao`. |
| `fonte` | `str` | Origem do registro. | Normalmente `c170`. |

### Fórmulas

```text
qtd_conv = qtd * fator
```

```text
valor_unitario_conv = valor_unitario / fator
```

Neutralização:

```text
se mov_rep = true ou excluir_estoque = true ou status_xml inválido ou base = 0:
    qtd_conv = 0
```

---

## 2. `c176_xml_<cnpj>.parquet`

### Papel

Enriquece registros C176 do SPED com XMLs, agrupamento e fatores. Complementa o C170 em informações fiscais adicionais.

### Relações

| Origem | Destino | Campos | Como interpretar |
|---|---|---|---|
| `fontes_produtos` | `c176_xml` | `codigo_fonte`, `id_agrupado` | Injeta produto agrupado. |
| `fatores_conversao` | `c176_xml` | `id_agrupado`, `unid`, `fator`, `unid_ref` | Injeta conversão de unidade. |
| `c176_xml` | `movimentacao_estoque` | `id_agrupado`, `qtd_conv`, valores | Complementa movimentos de estoque. |

### Campos

| Campo | Tipo | Explicação | Relação/Fórmula |
|---|---|---|---|
| `id_linha_origem` | `str` | Chave da linha original. | Rastreabilidade. |
| `id_agrupado` | `str` | Produto agrupado. | Liga ao produto consolidado. |
| `num_doc` | `str` | Número do documento fiscal. | Rastreabilidade documental. |
| `num_item` | `int` | Número do item. | Identifica item no documento. |
| `descricao` | `str` | Descrição do produto. | Apoio operacional. |
| `qtd` | `float` | Quantidade original. | Base para conversão. |
| `qtd_conv` | `float` | Quantidade convertida. | `qtd * fator`. |
| `valor_unitario` | `float` | Valor unitário original. | Base para conversão de valor. |
| `valor_unitario_conv` | `float` | Valor unitário convertido. | `valor_unitario / fator`. |
| `unid` | `str` | Unidade original. | Chave para fator. |
| `unid_ref` | `str` | Unidade de referência. | Vem de fatores. |
| `fator` | `float` | Fator aplicado. | Vem de fatores. |
| `fonte` | `str` | Origem do registro. | Preserva origem. |

### Fórmulas

```text
qtd_conv = qtd * fator
valor_unitario_conv = valor_unitario / fator
```

A neutralização segue a mesma lógica do C170.

---

## 3. `mov_estoque_<cnpj>.parquet`

### Papel

Tabela central de auditoria operacional. Ordena cronologicamente entradas, saídas, estoque inicial e estoque final por produto agrupado, calculando saldo físico, entradas desacobertadas e custo médio.

### Relações

| Origem | Destino | Campos | Como interpretar |
|---|---|---|---|
| `c170_xml` / `c176_xml` | `movimentacao_estoque` | `id_agrupado`, `qtd_conv`, valores, datas | Gera movimentos fiscais. |
| `fatores_conversao` | `movimentacao_estoque` | `unid_ref`, `fator` | Garante unidade comum. |
| `produtos_final` / `fontes_produtos` | `movimentacao_estoque` | `descr_padrao`, `ncm_padrao`, `cest_padrao`, `co_sefin_agr` | Enriquecimento fiscal/cadastral. |
| `movimentacao_estoque` | `calculos_mensais` | `id_agrupado`, data, saldos, `q_conv` | Agrega por mês. |
| `movimentacao_estoque` | `calculos_anuais` | `id_agrupado`, ano, saldo, auditoria | Agrega por ano. |

### Campos

| Campo | Tipo | Explicação | Relação/Fórmula |
|---|---|---|---|
| `id_agrupado` | `str` | Produto agrupado. | Chave de cálculo e agrupamento. |
| `fonte` | `str` | Origem da linha: `c170`, `nfe`, `nfce`, `bloco_h`, `gerado`. | Ajuda a auditar a fonte do movimento. |
| `Tipo_operacao` | `str` | Tipo de movimento. | Controla sinal e efeito no saldo. |
| `Dt_e_s` | `date` | Data de entrada/saída. | Preferida para ordenação e agregação mensal. |
| `Dt_doc` | `date` | Data do documento. | Fallback temporal. |
| `q_conv` | `float` | Quantidade convertida observada. | `abs(Qtd) * abs(fator)`. |
| `q_conv_fisica` | `float` | Quantidade que altera saldo físico. | Zero em estoque final auditado. |
| `preco_item` | `float` | Valor total da linha. | Alimenta saldo financeiro e médias. |
| `Vl_item` | `float` | Valor unitário da linha. | Fallback para cálculo de preços. |
| `saldo_estoque_anual` | `float` | Saldo físico acumulado. | Calculado linha a linha no ano. |
| `entr_desac_anual` | `float` | Entrada desacobertada detectada. | Surge quando saída excede saldo. |
| `custo_medio_anual` | `float` | Custo médio móvel anual. | `saldo_financeiro / saldo_estoque_anual`. |
| `__qtd_decl_final_audit__` | `float` | Quantidade declarada em estoque final. | Usada para auditoria, não para movimentar saldo. |
| `ncm_padrao` | `str` | NCM padrão do produto. | Vem do agrupamento. |
| `cest_padrao` | `str` | CEST padrão do produto. | Vem do agrupamento. |
| `unid_ref` | `str` | Unidade de referência. | Vem de fatores. |
| `fator` | `float` | Fator aplicado. | Vem de fatores. |
| `co_sefin_final` | `str` | Código SEFIN final. | Apoio fiscal. |
| `co_sefin_agr` | `str` | Código SEFIN agrupado. | Chave para SITAFE. |
| `it_pc_interna` | `float` | Alíquota interna. | Usada em ICMS. |
| `it_in_st` | `str` | Indicador de ST. | Afeta cálculo de ICMS. |
| `it_pc_mva` | `float` | MVA original. | Usada em MVA efetivo/ajustado. |
| `it_in_mva_ajustado` | `str` | Indica MVA ajustada. | Controla fórmula de MVA. |
| `it_pc_reducao` | `float` | Redução de base de cálculo. | Apoio fiscal. |
| `it_in_reducao_credito` | `str` | Indicador de redução de crédito. | Apoio fiscal. |

### Tipos de operação

| Tipo | Efeito no saldo | Explicação |
|---|---|---|
| `0 - ESTOQUE INICIAL` | Soma | Inicia ou compõe o saldo físico anual. |
| `1 - ENTRADA` | Soma | Aumenta estoque e pode recalcular custo médio. |
| `2 - SAIDAS` | Subtrai | Reduz estoque; se faltar saldo, gera entrada desacobertada. |
| `3 - ESTOQUE FINAL` | Não altera saldo | Apenas audita o estoque declarado. |

### Fórmulas

Quantidade convertida:

```text
q_conv = abs(Qtd) * abs(fator)
```

Quantidade física:

```text
se Tipo_operacao = '3 - ESTOQUE FINAL':
    q_conv_fisica = 0
senão:
    q_conv_fisica = q_conv
```

Estoque final declarado:

```text
__qtd_decl_final_audit__ = q_conv quando Tipo_operacao = '3 - ESTOQUE FINAL'
```

Saldo físico:

```text
saldo_estoque_anual_novo = saldo_estoque_anual_anterior + entradas - saidas
```

Saída sem saldo:

```text
saldo_projetado = saldo_anterior - qtd_saida

se saldo_projetado < 0:
    entr_desac_anual = abs(saldo_projetado)
    saldo_estoque_anual = 0
senão:
    entr_desac_anual = 0
    saldo_estoque_anual = saldo_projetado
```

Custo médio:

```text
custo_medio_anual = saldo_financeiro / saldo_estoque_anual
```

Entrada válida:

```text
saldo_financeiro_novo = saldo_financeiro_anterior + preco_item_entrada
saldo_estoque_novo = saldo_estoque_anterior + q_conv_fisica_entrada
custo_medio = saldo_financeiro_novo / saldo_estoque_novo
```

Saída válida:

```text
baixa_financeira = q_conv_fisica_saida * custo_medio_vigente
saldo_financeiro_novo = saldo_financeiro_anterior - baixa_financeira
```

Estoque final:

```text
não altera saldo_estoque_anual
não altera saldo_financeiro
não recalcula custo_medio_anual
```

### Neutralizações

```text
se mov_rep = true:
    q_conv = 0
    q_conv_fisica = 0
```

```text
se excluir_estoque = true:
    q_conv = 0
    q_conv_fisica = 0
```

```text
se status XML não for autorizado:
    q_conv = 0
    q_conv_fisica = 0
```

---

## Relação resumida desta camada

```text
fatores_conversao.fator
  -> c170_xml.qtd_conv / c176_xml.qtd_conv
  -> movimentacao_estoque.q_conv
  -> movimentacao_estoque.saldo_estoque_anual
  -> calculos_mensais / calculos_anuais
```

## Campos críticos

| Campo | Motivo |
|---|---|
| `id_linha_origem` | Volta à linha fiscal original. |
| `id_agrupado` | Produto usado no estoque e nas análises. |
| `qtd_conv` / `q_conv` | Quantidade convertida observada. |
| `q_conv_fisica` | Única quantidade que deve alterar o saldo físico. |
| `__qtd_decl_final_audit__` | Preserva o estoque final declarado sem alterar o saldo. |
| `saldo_estoque_anual` | Base de fechamento e divergência. |
| `entr_desac_anual` | Indica saída sem cobertura de entrada. |
| `custo_medio_anual` | Base financeira para estoque e auditoria. |
| `co_sefin_agr`, `it_in_st`, `it_pc_mva`, `it_pc_interna` | Campos fiscais para ST, MVA e ICMS. |
