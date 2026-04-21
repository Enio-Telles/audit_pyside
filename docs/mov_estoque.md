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
