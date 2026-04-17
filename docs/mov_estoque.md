# Movimentação de Estoque

Este documento consolida as regras operacionais da `mov_estoque_<cnpj>.parquet`.

## Papel da tabela

A `mov_estoque` é a camada cronológica e auditável do fluxo de mercadorias. Ela consolida C170, NFe, NFCe, inventário do Bloco H e linhas sintéticas geradas pelo processo anual.

## Campos de auditoria de evento

Além dos campos já existentes, a tabela passa a preservar:

- `origem_evento_estoque`
- `evento_sintetico`

### Semântica

- `origem_evento_estoque = registro`: linha original de movimentação;
- `inventario_bloco_h`: estoque final vindo do inventário declarado;
- `estoque_final_gerado`: estoque final sintético gerado pelo pipeline;
- `estoque_inicial_derivado`: estoque inicial derivado de inventário real;
- `estoque_inicial_gerado`: estoque inicial sintético por ausência de abertura explícita.

`evento_sintetico` diferencia linhas geradas pelo pipeline das linhas físicas/originais.

## Quantidades convertidas

Há três papéis distintos:

- `q_conv`: quantidade convertida observada na linha;
- `q_conv_fisica`: quantidade convertida que representa movimento físico;
- `__q_conv_sinal__`: quantidade física sinalizada usada no cálculo sequencial.

## Estoque final auditado

Linhas de `3 - ESTOQUE FINAL`:

- não alteram saldo físico;
- não alteram custo médio;
- preservam `__qtd_decl_final_audit__` para auditoria;
- podem ter `q_conv` preenchido para inspeção row-level.

## Saldos e custo médio

As regras permanecem:

- entradas e estoque inicial somam no saldo;
- saídas baixam o saldo;
- estoque final apenas audita;
- devoluções retornam quantidade sem alterar o custo médio vigente.

## Observabilidade

Com o novo metadado de evento, relatórios e auditorias conseguem separar claramente:

- inventário real;
- evento sintético gerado;
- abertura derivada;
- movimentação original.
