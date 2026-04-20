# Abordagem de quantidades: contrato conceitual e colunas reais
<a id="mds-01-abordagem-quantidades"></a>

Este documento consolida a semantica de quantidades da metodologia MDS sem confundir conceito com nome materializado no runtime.

## Objetivo

Separar com clareza:

- quantidade observada na linha fiscal;
- quantidade que representa movimento fisico;
- quantidade sinalizada para saldo;
- quantidade declarada em inventario para auditoria.

## Mapa entre conceito e runtime

| Conceito | Coluna real hoje |
| --- | --- |
| `quantidade_convertida` | `q_conv` |
| `quantidade_fisica` | `q_conv_fisica` |
| `quantidade_fisica_sinalizada` | `__q_conv_sinal__` |
| `estoque_final_declarado` | `__qtd_decl_final_audit__` |
| `tipo_operacao` no service | `Tipo_operacao` no pipeline |

O `MovimentacaoService` tambem materializa as colunas conceituais (`quantidade_*` e `estoque_final_declarado`) para fins de clareza e compatibilidade, mas as camadas finais continuam consumindo majoritariamente os nomes legados.

## Regras canonicas hoje

### 1. Quantidade observada

`q_conv` representa a quantidade observada na linha apos:

- normalizacao de unidade;
- aplicacao de fator;
- neutralizacao por `mov_rep`, `excluir_estoque` ou protocolo invalido, quando cabivel.

Em linhas de estoque final, `q_conv` preserva a quantidade declarada no inventario para auditoria.

### 2. Quantidade fisica

`q_conv_fisica` representa apenas movimento fisico.

Regra:

- `0 - ESTOQUE INICIAL`: entra no fisico
- `1 - ENTRADA`: entra no fisico
- `2 - SAIDAS`: entra no fisico
- `3 - ESTOQUE FINAL`: fica sempre zero no fisico

Ou seja: inventario declarado nao movimenta saldo cronologico.

### 3. Quantidade sinalizada

`__q_conv_sinal__` e a quantidade usada no calculo sequencial do saldo:

- estoque inicial e entrada: positivo
- saida: negativo
- estoque final: zero

### 4. Quantidade declarada para auditoria

`__qtd_decl_final_audit__` preserva a quantidade informada no inventario.

Ela e usada para:

- confronto entre saldo calculado e saldo declarado;
- calculos de divergencia em `aba_periodos` e `aba_anual`;
- trilha auditavel do que foi declarado no Bloco H.

## Operacoes reconhecidas

O runtime trabalha hoje com `Tipo_operacao` e reconhece principalmente:

- `0 - ESTOQUE INICIAL`
- `1 - ENTRADA`
- `2 - SAIDAS`
- `3 - ESTOQUE FINAL`

Tambem existem variantes geradas pelo pipeline, como:

- `0 - ESTOQUE INICIAL gerado`
- `3 - ESTOQUE FINAL gerado`

Por isso, as regras de estoque inicial e estoque final devem ser descritas por prefixo (`starts_with`) e nao por igualdade estrita em toda a documentacao.

## Neutralizacao e devolucao

### Neutralizacao

Linhas marcadas por duplicidade (`mov_rep`) ou exclusao (`excluir_estoque`) nao devem seguir como movimento valido. No runtime atual isso e resolvido antes da etapa final de saldo, zerando o valor efetivo consumido em `q_conv`, `q_conv_fisica` e `__q_conv_sinal__`.

### Devolucao

Devolucao nao muda sinal por si so. O sinal continua vindo de `Tipo_operacao`.

As flags de devolucao (`dev_simples`, `dev_venda`, `dev_compra`, `dev_ent_simples`, alem de `finnfe = 4` quando aplicavel) servem principalmente para:

- marcar `__is_devolucao__`;
- excluir linhas das medias;
- evitar que linhas observacionais contaminem `pme` e `pms`.

## Compatibilidade com Parquet legado

Se um parquet antigo nao tiver `q_conv_fisica`, as camadas mensal e anual fazem fallback a partir de `q_conv`, zerando estoque final.

Portanto:

- `q_conv` continua sendo o contrato legado mais importante;
- `q_conv_fisica` e `__q_conv_sinal__` formalizam a semantica correta sem quebrar consumidores existentes.

## Regra pratica

Quando houver duvida, a leitura correta e:

1. `q_conv` = o que a linha observou ou declarou
2. `q_conv_fisica` = o que realmente move estoque
3. `__q_conv_sinal__` = o que entra no saldo
4. `__qtd_decl_final_audit__` = o que o inventario declarou
