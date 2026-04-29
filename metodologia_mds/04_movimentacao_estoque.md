# Movimentacao de estoque: contrato vigente do mov_estoque
<a id="mds-04-movimentacao-estoque"></a>

Este documento descreve a camada `mov_estoque_<cnpj>.parquet` do jeito que ela existe hoje no pipeline.

## Papel da camada

`mov_estoque` e o ponto de consolidacao entre:

- fontes fiscais enriquecidas;
- fatores de conversao;
- agregacao de produtos;
- eventos sinteticos de estoque;
- calculo sequencial de saldos;
- comparacao entre saldo calculado e inventario declarado.

## Colunas centrais do runtime

As colunas mais importantes hoje sao:

- `id_agrupado`
- `Tipo_operacao`
- `q_conv`
- `q_conv_fisica`
- `__q_conv_sinal__`
- `__qtd_decl_final_audit__`
- `periodo_inventario`
- `saldo_estoque_anual`
- `saldo_estoque_periodo`
- `delta_decl_final_anual`
- `delta_decl_final_periodo`
- `origem_evento_estoque`
- `evento_sintetico`
- `ordem_operacoes`

O service MDS tambem acrescenta:

- `quantidade_convertida`
- `quantidade_fisica`
- `quantidade_fisica_sinalizada`
- `estoque_final_declarado`

Essas colunas conceituais convivem com o contrato legado; elas nao substituem os nomes historicos consumidos pelas abas finais.

## Operacoes reconhecidas

O pipeline trabalha com quatro familias canonicas:

- `0 - ESTOQUE INICIAL`
- `1 - ENTRADA`
- `2 - SAIDAS`
- `3 - ESTOQUE FINAL`

Tambem existem variantes sinteticas, por exemplo:

- `0 - ESTOQUE INICIAL gerado`
- `3 - ESTOQUE FINAL gerado`

Logo, qualquer documentacao correta deve considerar prefixo para estoque inicial e estoque final, e nao igualdade textual absoluta.

## Regras de quantidade

### `q_conv`

Quantidade observada da linha apos fator e validacoes.

### `q_conv_fisica`

Quantidade usada como movimento fisico. Estoque final fica zerado aqui.

### `__q_conv_sinal__`

Quantidade com sinal para saldo:

- positivo em estoque inicial e entrada
- negativo em saida
- zero em estoque final

### `__qtd_decl_final_audit__`

Quantidade declarada em inventario, preservada para auditoria.

## Neutralizacao

No runtime atual, neutralizacao relevante ocorre antes do saldo final:

- `mov_rep`
- `excluir_estoque`
- protocolo de autorizacao invalido

Quando uma linha cai nessas regras, o valor efetivo consumido pelo saldo vira zero.

## Eventos sinteticos

`gerar_eventos_estoque()` pode criar linhas sinteticas para manter coerencia do fluxo temporal, especialmente quando falta estoque final de 31/12 ou quando e necessario derivar estoque inicial do periodo seguinte.

Essas linhas sao rastreadas por:

- `evento_sintetico`
- `origem_evento_estoque`

## Periodo de inventario

`periodo_inventario` e incrementado quando o fluxo encontra linhas que comecam com `0 - ESTOQUE INICIAL`.

Na pratica isso significa que o periodo e ancorado pelo estoque inicial do ciclo, inclusive quando esse estoque inicial e derivado de um inventario final anterior.

## Saldo e divergencia

Depois de materializar quantidades e ordem operacional, o pipeline calcula:

- `saldo_estoque_anual`
- `saldo_estoque_periodo`
- `delta_decl_final_anual`
- `delta_decl_final_periodo`

Os deltas comparam saldo sistemico com `__qtd_decl_final_audit__` apenas em linhas de estoque final.

## Regra pratica

Ao ler `mov_estoque`, interprete assim:

1. `q_conv` = o que a linha observou
2. `q_conv_fisica` = o que move estoque
3. `__q_conv_sinal__` = o que entra no saldo
4. `__qtd_decl_final_audit__` = o que o inventario declarou
5. `evento_sintetico` e `origem_evento_estoque` = de onde veio o evento
