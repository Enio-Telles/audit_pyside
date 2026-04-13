# Movimentação de Estoque

Este documento consolida as regras operacionais da `mov_estoque_<cnpj>.parquet`, gerada pelo módulo `src/transformacao/movimentacao_estoque.py` e pela implementação em `src/transformacao/movimentacao_estoque_pkg/`.

## Papel da tabela

A `mov_estoque` é a camada cronológica e auditável do fluxo de mercadorias. Ela consolida C170, NFe, NFCe, inventário do Bloco H e linhas sintéticas geradas pelo processo anual.

É nessa tabela que ficam materializados:

- a origem da linha em `fonte`;
- a quantidade convertida em `q_conv`;
- o saldo físico anual em `saldo_estoque_anual`;
- a omissão de entrada por saldo negativo em `entr_desac_anual`;
- o custo médio móvel em `custo_medio_anual`.

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
