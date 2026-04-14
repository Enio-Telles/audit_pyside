# Movimentação de Estoque

Este documento consolida as regras operacionais da `mov_estoque_<cnpj>.parquet`, gerada pelo módulo `src/transformacao/movimentacao_estoque_pkg/movimentacao_estoque.py`.

## Papel da tabela

A `mov_estoque` é a camada cronológica e auditável do fluxo de mercadorias. Ela consolida C170, NFe, NFCe, inventário do Bloco H e linhas sintéticas geradas pelo processo anual.

É nessa tabela que ficam materializados:

- o custo médio móvel em `custo_medio_anual`.

## Identificação Fiscal (SITAFE)

Para que os saldos de estoque possam ser auditados sob a ótica da carga tributária, cada produto é vinculado a um `co_sefin` (código de classificação interna da SEFIN) utilizando as tabelas do SITAFE localizadas em `dados/referencias/CO_SEFIN/`.

A identificação ocorre na etapa `item_unidades` via `pl.coalesce`, seguindo esta precedência de match:
1. **CEST + NCM**: Prioridade máxima via `sitafe_cest_ncm.parquet`.
2. **Somente CEST**: Fallback para match por CEST via `sitafe_cest.parquet`.
3. **Somente NCM**: Fallback final para match por NCM via `sitafe_ncm.parquet`.

Este vínculo é essencial para as colunas enriquecidas como `it_pc_interna`, `it_in_st` e `it_pc_mva`, permitindo que o cálculo de ressarcimento e auditoria de estoque reflita a classificação fiscal correta.

## Campos da Tabela

### Identificação do Produto


| Campo            | Tipo    | Descrição                                                     |
|------------------|---------|---------------------------------------------------------------|
| `id_agrupado`    | `str`   | Chave mestra de agrupamento do produto (ex:`PROD_MSTR_00001`) |
| `descr_padrao`   | `str`   | Descrição padrão normalizada do agrupamento                   |
| `ncm_padrao`     | `str`   | NCM padrão do agrupamento                                     |
| `cest_padrao`    | `str`   | CEST padrão do agrupamento                                    |
| `unid_ref`       | `str`   | Unidade de referência sugerida para o agrupamento             |
| `fator`          | `float` | Fator de conversão para`unid_ref`                             |
| `co_sefin_agr`   | `str`   | Co-sefin agregado do agrupamento                              |
| `co_sefin_final` | `str`   | Co-sefin final resolvido pela`co_sefin_class`                 |

### Dados do Item


| Campo         | Tipo  | Descrição                           |
|---------------|-------|-------------------------------------|
| `Cod_item`    | `str` | Código do item na fonte original    |
| `Cod_barra`   | `str` | Código de barras (GTIN/CEAN)        |
| `Descr_item`  | `str` | Descrição do item na fonte original |
| `Descr_compl` | `str` | Descrição complementar              |
| `Cfop`        | `str` | CFOP da operação                    |

### Operação


| Campo           | Tipo   | Descrição                                                                             |
|-----------------|--------|---------------------------------------------------------------------------------------|
| `Tipo_operacao` | `str`  | Tipo canônico:`0 - ESTOQUE INICIAL`, `1 - ENTRADA`, `2 - SAIDAS`, `3 - ESTOQUE FINAL` |
| `fonte`         | `str`  | Origem:`c170`, `nfe`, `nfce`, `bloco_h`, `gerado`                                     |
| `Dt_doc`        | `date` | Data de emissão do documento                                                          |
| `Dt_e_s`        | `date` | Data de entrada/saída                                                                 |
| `nsu`           | `int`  | Número sequencial único (SPED)                                                        |

### Chaves Fiscais e Validação


| Campo           | Tipo  | Descrição                                                                    |
|-----------------|-------|------------------------------------------------------------------------------|
| `Chv_nfe`       | `str` | Chave de acesso da NF-e (44 dígitos)                                         |
| `Num_item`      | `int` | Número do item na NF-e                                                       |
| `finnfe`        | `str` | Finalidade da NF-e (`1`=Normal, `2`=Complementar, `3`=Ajuste, `4`=Devolução) |
| `infprot_cstat` | `str` | Status do protocolo (`100`=Autorizada, `150`=Autorizada fora de prazo)       |
| `co_uf_emit`    | `str` | UF do emitente                                                               |
| `co_uf_dest`    | `str` | UF do destinatário                                                           |

### Quantidades e Valores


| Campo        | Tipo    | Descrição                                                     |
|--------------|---------|---------------------------------------------------------------|
| `Qtd`        | `float` | Quantidade bruta na unidade original                          |
| `Vl_item`    | `float` | Valor total do item na nota                                   |
| `Unid`       | `str`   | Unidade de medida original                                    |
| `q_conv`     | `float` | Quantidade convertida para`unid_ref`: `abs(Qtd) * abs(fator)` |
| `preco_unit` | `float` | Preço unitário calculado:`preco_item / q_conv`                |

### Campos Calculados de Auditoria


| Campo                      | Tipo    | Descrição                                                        |
|----------------------------|---------|------------------------------------------------------------------|
| `__qtd_decl_final_audit__` | `float` | Quantidade declarada em estoque final (auditoria anual)          |
| `__q_conv_sinal__`         | `float` | Quantidade sinalizada para cálculo sequencial (+entrada, -saida) |
| `ordem_operacoes`          | `int`   | Ordem cronológica das operações (1-based)                        |

### Saldos Anuais (sufixo `_anual`)


| Campo                 | Tipo    | Descrição                                             |
|-----------------------|---------|-------------------------------------------------------|
| `saldo_estoque_anual` | `float` | Saldo físico sequencial acumulado no ano civil        |
| `entr_desac_anual`    | `float` | Entradas desacobertadas (saídas sem saldo suficiente) |
| `custo_medio_anual`   | `float` | Custo médio móvel vigente                             |

### Saldos por Período de Inventário (sufixo `_periodo`)


| Campo                   | Tipo    | Descrição                              |
|-------------------------|---------|----------------------------------------|
| `saldo_estoque_periodo` | `float` | Saldo físico por período de inventário |
| `entr_desac_periodo`    | `float` | Entradas desacobertadas por período    |
| `custo_medio_periodo`   | `float` | Custo médio móvel por período          |

### Campos de Controle de Movimentação


| Campo             | Tipo   | Descrição                                                                   |
|-------------------|--------|-----------------------------------------------------------------------------|
| `mov_rep`         | `bool` | Movimentação repetida (mesma`Chv_nfe` + `Num_item` aparece mais de uma vez) |
| `excluir_estoque` | `bool` | Flag para excluir do estoque (base zero, etc.)                              |
| `dev_simples`     | `bool` | Indica devolução por Simples Nacional                                       |
| `dev_venda`       | `bool` | Indica devolução de venda                                                   |
| `dev_compra`      | `bool` | Indica devolução de compra                                                  |
| `dev_ent_simples` | `bool` | Indica devolução de entrada por Simples                                     |

### Campos Fiscais Enriquecidos (`co_sefin_class`)


| Campo                   | Tipo    | Descrição                              |
|-------------------------|---------|----------------------------------------|
| `it_pc_interna`         | `float` | Percentual de carga tributária interna |
| `it_in_st`              | `bool`  | Indica se é sujeito a ST               |
| `it_pc_mva`             | `float` | Percentual MVA                         |
| `it_in_mva_ajustado`    | `bool`  | MVA ajustado                           |
| `it_pc_reducao`         | `float` | Percentual de redução de base          |
| `it_in_reducao_credito` | `bool`  | Indica redução de crédito              |

## Origem das linhas

Valores usuais de `fonte`:

- `c170`: entradas vindas do SPED;
- `nfe`: saídas vindas de XML de NFe;
- `nfce`: saídas vindas de NFCe;
- `bloco_h`: inventário real declarado;
- `gerado`: linhas sintéticas criadas pelo pipeline.

Regras de direção:

- `c170` participa como `1 - ENTRADA`;
- `nfe` e `nfce` participam como `2 - SAIDAS`;
- inventário do Bloco H entra como `3 - ESTOQUE FINAL`, preservando `fonte = bloco_h`;
- estoques sintéticos de abertura e fechamento usam `fonte = gerado`.

## Ordenação e reinício anual

O cálculo é sequencial por `id_agrupado` e ano civil. Dentro de cada ano, a ordem lógica é:

1. `0 - ESTOQUE INICIAL`
2. `1 - ENTRADA`
3. `2 - SAIDAS`
4. `3 - ESTOQUE FINAL`

Ao mudar o ano, reiniciam:

- saldo físico;
- saldo financeiro;
- custo médio;
- contador de entradas desacobertadas.

O campo `periodo_inventario` permite resetar o cálculo a cada período de inventário (não apenas ano civil), sendo incrementado a cada `0 - ESTOQUE INICIAL`.

## Quantidade convertida

`q_conv` representa a quantidade da linha convertida para `unid_ref`:

```text
q_conv = abs(Qtd) * abs(fator)
```

Neutralizações relevantes:

- `mov_rep = true`;
- `excluir_estoque = true`;
- `infprot_cstat` diferente de `100` ou `150`;
- base da linha igual a zero.

**Nota sobre estoque inicial e final:**

- Estoque inicial (`0 - ESTOQUE INICIAL`) captura `q_conv` em **qualquer data**;
- Estoque final (`3 - ESTOQUE FINAL`) captura `__qtd_decl_final_audit__` em **qualquer data**;
- A restrição anterior de 01/01 e 31/12 foi removida para permitir auditoria anual completa.

Quando a linha é neutralizada, `q_conv = 0` e ela também deixa de compor médias de preço nas camadas mensal e anual.

## Estoque final auditado

`3 - ESTOQUE FINAL` não altera o saldo físico:

- `q_conv` permanece `0` (não impacta saldo);
- a quantidade declarada fica em `__qtd_decl_final_audit__` (para auditoria na tabela anual);
- `saldo_estoque_anual` não muda;
- `custo_medio_anual` não muda;
- `entr_desac_anual` permanece `0`.

Essa linha existe para auditoria de inventário, não para recomposição física do saldo.

**Importante:** A quantidade em `__qtd_decl_final_audit__` é capturada para **qualquer linha** com `Tipo_operacao` iniciando com "3 - ESTOQUE FINAL", independente da data. Isso permite que a tabela anual some corretamente todos os estoques finais do ano para auditoria.

## Saldo e entradas desacobertadas

Regras principais:

- entradas e estoque inicial somam no saldo;
- saídas baixam o saldo;
- estoque final apenas audita.

Quando uma saída faria o saldo ficar negativo:

```text
entr_desac_anual = abs(saldo_negativo)
saldo_estoque_anual = 0
```

Portanto, `entr_desac_anual` nasce apenas de saída sem saldo suficiente. Estoque final não cria esse campo.

## Custo médio anual

O custo médio usa saldo financeiro acumulado.

Entradas válidas:

- somam quantidade;
- somam `preco_item` no saldo financeiro;
- recalculam `custo_medio_anual`.

Saídas válidas:

- baixam pelo custo médio vigente;
- não usam o valor da própria linha para formar nova média.

Estoque final:

- não altera saldo financeiro;
- não recalcula custo médio.

## Devoluções

Devoluções são detectadas por:

- `finnfe = "4"` (NF-e de devolução);
- `dev_simples = true`;
- `dev_venda = true`;
- `dev_compra = true`;
- `dev_ent_simples = true`.

Devoluções retornam quantidade sem alterar o custo médio vigente.

## Campos críticos de auditoria

Além dos campos de saldo, a `mov_estoque` preserva colunas mandatórias para cruzamentos posteriores:

- `id_agrupado`
- `ncm_padrao`
- `cest_padrao`
- `unid_ref`
- `fator`
- `co_sefin_final`
- `co_sefin_agr`
- `it_pc_interna`
- `it_in_st`
- `it_pc_mva`
- `it_in_mva_ajustado`
- `it_pc_reducao`
- `it_in_reducao_credito`

## Saída gerada

Arquivo persistido:

```text
dados/CNPJ/<cnpj>/analises/produtos/mov_estoque_<cnpj>.parquet
```

## Período de Inventário

O campo `periodo_inventario` é incrementado a cada `0 - ESTOQUE INICIAL` dentro de um `id_agrupado`. Isso permite:

1. Resetar o cálculo de saldo a cada período de inventário (não apenas ano civil);
2. Gerar campos com sufixo `_periodo` (`saldo_estoque_periodo`, `entr_desac_periodo`, `custo_medio_periodo`);
3. Auditoria independente por período fiscal customizado.

O cálculo sequencial é executado em dois passos:

1. `_calcular_saldo_estoque_anual`: por `id_agrupado` + `__ano_saldo__`;
2. `_calcular_saldo_estoque_periodo`: por `id_agrupado` + `periodo_inventario`.
