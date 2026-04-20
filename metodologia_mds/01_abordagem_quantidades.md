# Abordagem de quantidades: convertidas e físicas
<a id="mds-01-abordagem-quantidades"></a>

Este documento apresenta a nova nomenclatura e semântica para as quantidades derivadas das buscas SQL (Bloco H, C170, NF‑e e NFC‑e) e consumidas ao longo do pipeline de auditoria.

## Objetivo

Preservar as quantidades declaradas nas linhas originais (inventário, NF‑e/NFC‑e, C170) para auditoria e, ao mesmo tempo, definir claramente a quantidade que representa movimento físico real. Separar as duas visões é fundamental para evitar distorções no cálculo de saldos, médias de preço e divergências de estoque.

## Conceitos e nomenclatura

| Campo                          | Descrição                                                                                                                                                                      |
|-------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `quantidade_convertida`       | Quantidade da linha convertida para a unidade de referência. Corresponde à quantidade extraída do documento (por ex.: `qtd` do C170 ou `prod_qcom` da NF‑e) multiplicada pelo fator de conversão. Este campo permanece preenchido em linhas de inventário para fins de conferência. |
| `quantidade_fisica`           | Quantidade convertida que efetivamente representa movimento físico. Para linhas de inventário (`3 – ESTOQUE FINAL`) este campo é **sempre zero**, pois o inventário não é uma entrada nem uma saída no fluxo cronológico. |
| `quantidade_fisica_sinalizada` | Quantidade física com sinal (+ para entradas, − para saídas) utilizada no cálculo sequencial do saldo. É derivada de `quantidade_fisica` aplicando o sinal da operação (`tipo_operacao`). |
| `estoque_final_declarado`      | Quantidade declarada no inventário (Bloco H) que deve ser preservada para auditoria anual/periódica. Não interfere na movimentação física nem no saldo cronológico. |

### Observações importantes

* **Inventário (3 – ESTOQUE FINAL)**: apesar de conter `quantidade_convertida` (extraída das colunas `qtd`/`vl_item` dos registros H010), essa linha serve exclusivamente para auditar o estoque declarado. Portanto, `quantidade_fisica = 0`, `quantidade_fisica_sinalizada = 0` e a quantidade declarada é armazenada em `estoque_final_declarado`.
* **Neutralizações e devoluções**: quando uma linha estiver marcada para neutralização ou representar devolução simples, a quantidade física deve refletir a devolução com sinal apropriado. A derivação de `quantidade_fisica` deve sempre consultar o campo `tipo_operacao`.
* **Compatibilidade retroativa**: Parquets antigos podem não conter a coluna `quantidade_fisica`. Em tais casos, ao ler os dados, derive `quantidade_fisica` aplicando as regras acima, garantindo que inventários não alterem o saldo físico.

### Derivação recomendada em código

O snippet abaixo ilustra como derivar `quantidade_fisica` quando a coluna não existir ou quando for necessário recalcular o campo:

```python
pl.when(pl.col("tipo_operacao").str.starts_with("3 - ESTOQUE FINAL"))
  .then(pl.lit(0.0))
  .otherwise(pl.col("quantidade_convertida").cast(pl.Float64).fill_null(0.0))
  .alias("quantidade_fisica")
```

## Impacto nas camadas

* **movimentacao_estoque**: passa a materializar explicitamente `quantidade_convertida`, `quantidade_fisica`, `quantidade_fisica_sinalizada` e `estoque_final_declarado`. As linhas de inventário permanecem para auditoria, mas não alteram o saldo.
* **cálculos mensais e anuais**: todas as agregações de entradas/saídas e médias de preço devem utilizar `quantidade_fisica` e não `quantidade_convertida`. A quantidade convertida continua disponível apenas para auditoria de valores unitários.
* **tabelas analíticas**: devem distinguir claramente entre a quantidade usada para cálculo de estoque (`quantidade_fisica`) e a quantidade declarada em inventário (`estoque_final_declarado`).

## Compatibilidade e testes

Esta abordagem foi elaborada para que a leitura de Parquets legados seja retrocompatível. Testes unitários devem assegurar que:

1. **Inventário não altera saldo**: somar `quantidade_fisica_sinalizada` ao longo da movimentação deve resultar no mesmo saldo independentemente da presença ou ausência do inventário.
2. **Quantidades negativas ou devoluções**: devoluções registradas (CFOP de devolução) devem gerar `quantidade_fisica` com sinal invertido, impactando o saldo e o custo médio de forma adequada.
3. **Campos de auditoria preservados**: `quantidade_convertida` e `estoque_final_declarado` permanecem intocados para conferência.

Ao adotar estas nomenclaturas e regras, o pipeline passa a refletir de forma fiel a movimentação física de mercadorias, sem perder a rastreabilidade e a auditoria das quantidades declaradas nas origens SQL.