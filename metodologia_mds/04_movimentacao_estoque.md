# Movimentação de estoque

Este documento consolida as regras operacionais e nomenclaturas da `movimentacao_estoque_<cnpj>.parquet`. A camada de movimentação é o coração do pipeline, pois integra as linhas de NF‑e/NFC‑e, C170 e inventário (Bloco H), aplicando as convenções de quantidade e agrupamento.

## Papel da tabela

A `movimentacao_estoque` é uma visão cronológica e auditável de todos os eventos que impactam o estoque de um produto agregado. Ela é construída a partir das buscas SQL e incorpora ajustes derivados (por exemplo, abertura de estoque inicial, linhas sintéticas geradas anualmente e periodicamente).

## Campos principais

| Campo                         | Descrição |
|------------------------------|-----------|
| `id_linha_origem`            | Chave física da linha no documento original (C170, NF‑e, NFC‑e ou Bloco H). |
| `id_produto_origem`          | Chave formada por CNPJ do emitente (ou participante) e código do item; extraída diretamente nas consultas SQL. |
| `id_produto_agrupado`        | Identificador do produto agregado resultante do mapeamento. |
| `tipo_operacao`              | Classificação da linha: `0 – ESTOQUE INICIAL`, `1 – ENTRADA`, `2 – SAIDA`, `3 – ESTOQUE FINAL`, entre outros. |
| `quantidade_convertida`      | Quantidade convertida para a unidade de referência (`unidade_referencia`) usando o fator de conversão. |
| `quantidade_fisica`          | Quantidade de movimento físico (0 em inventários). |
| `quantidade_fisica_sinalizada` | Quantidade física com sinal, usada no cálculo sequencial do saldo. |
| `estoque_final_declarado`     | Quantidade declarada no inventário (Bloco H) preservada para auditoria. |
| `origem_evento_estoque`       | Indica a origem da linha: `registro` (linha original), `inventario_bloco_h`, `estoque_final_gerado`, `estoque_inicial_derivado` ou `estoque_inicial_gerado`. |
| `evento_sintetico`            | Flag booleana que diferencia linhas geradas pelo pipeline (`True`) das linhas físicas dos documentos (`False`). |

## Semântica das operações

1. **Estoque inicial (0 – ESTOQUE INICIAL)**: representa a abertura de saldo no início de um período ou ano. Contribui positivamente para o saldo físico (`quantidade_fisica_sinalizada > 0`).
2. **Entradas (1 – ENTRADA)**: movimentações de compra ou devoluções que aumentam o estoque. Também incrementam o saldo físico.
3. **Saídas (2 – SAIDA)**: vendas, baixas de estoque ou devoluções de entrada. Reduzem o saldo físico (`quantidade_fisica_sinalizada < 0`).
4. **Estoque final (3 – ESTOQUE FINAL)**: inventário declarado no Bloco H. Não altera o saldo físico; `quantidade_fisica = 0` e `quantidade_fisica_sinalizada = 0`. A quantidade declarada é armazenada em `estoque_final_declarado` para auditoria.
5. **Eventos sintéticos**: linhas geradas pelo pipeline para ajustar estoque inicial ou final quando não há inventário explícito. Ex.: `estoque_final_gerado` e `estoque_inicial_gerado`.

## Regras de cálculo

1. **Salto cronológico**: o saldo do produto agregado é calculado sequencialmente somando `quantidade_fisica_sinalizada` em ordem cronológica. Inventários apenas informam a quantidade declarada e não alteram o saldo.
2. **Custo médio**: ao entrar mercadoria com preço unitário, atualiza‑se o custo médio por método de média ponderada. Inventários não alteram o custo médio.
3. **Devoluções**: devoluções de compra ou de venda devem ser classificadas corretamente (`tipo_operacao` apropriado) e inverter o sinal da `quantidade_fisica_sinalizada` em relação à operação original.

## Controle de unidades

Cada linha utiliza a unidade de medida original e o fator de conversão calculado na etapa de **conversão de unidades**. A `quantidade_convertida` sempre deve corresponder à unidade de referência do produto agregado. O campo `quantidade_fisica` resulta da aplicação de regras semânticas (ver [Abordagem de quantidades](01_abordagem_quantidades.md)).

## Integridade e auditoria

* **Campos legados preservados**: as quantidades originais extraídas das consultas SQL (`qtd`, `prod_qcom`, `prod_qtrib`) devem ser preservadas para auditabilidade, mesmo que não sejam usadas no cálculo.
* **Logs de inconsistências**: linhas com `id_produto_origem` sem mapeamento ou com unidade incompatível devem ser exportadas para auditoria. O pipeline não deve gerar agrupamentos ou saldos silenciosos.
* **Separação de inventário**: sempre que uma linha de inventário (`3 – ESTOQUE FINAL`) estiver presente, a quantidade deve ser copiada para `estoque_final_declarado`. Camadas posteriores (tabelas periódicas e anuais) usam esse valor para comparar com o saldo calculado.

Ao seguir essas nomenclaturas e regras, a tabela de movimentação oferece transparência sobre a origem e a natureza de cada movimento de estoque e estabelece a base para as tabelas de período, mensal e anual.