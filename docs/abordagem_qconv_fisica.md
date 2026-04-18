# Abordagem: q_conv_fisica

Este documento descreve a semântica introduzida para separar a quantidade observada na linha (`q_conv`) da quantidade que representa movimento físico real (`q_conv_fisica`).

Motivação:

- Em auditoria é desejável preservar a quantidade declarada em inventários (`3 - ESTOQUE FINAL`) para fins de conferência, sem que essas linhas afetem o saldo sequencial.
- Ao mesmo tempo, precisamos de uma métrica consistente para agregações físicas (entradas/saídas) e para cálculo de médias de preço que represente apenas movimentos físicos.

Definições:

- `q_conv`: quantidade convertida observada na linha. Em `3 - ESTOQUE FINAL` pode permanecer preenchida para auditoria.
- `q_conv_fisica`: quantidade convertida que representa movimento físico. Em `3 - ESTOQUE FINAL` é sempre zero.
- `__q_conv_sinal__`: quantidade sinalizada (+entrada, -saída) usada no cálculo sequencial do saldo.

Regras de derivação:

- `q_conv = abs(Qtd) * abs(fator)` quando não houver neutralização.
- `q_conv_fisica = 0` para linhas com `Tipo_operacao` começando com `3 - ESTOQUE FINAL`.
- Caso `q_conv_fisica` esteja ausente em Parquets antigos, será derivada em leitura:

```python
pl.when(pl.col("Tipo_operacao").str.starts_with("3 - ESTOQUE FINAL"))
  .then(pl.lit(0.0))
  .otherwise(pl.col("q_conv").cast(pl.Float64).fill_null(0.0))
  .alias("q_conv_fisica")
```

Impacto nas camadas:

- `mov_estoque`: agora materializa `q_conv_fisica` como coluna explícita.
- `calculos_mensais` e `calculos_anuais`: passam a usar `q_conv_fisica` para agregações físicas e médias (`pme`, `pms`).
- Tabelas derivadas mantêm `q_conv` para auditoria e `q_conv_fisica` para cálculos.

Compatibilidade:

- Leitura de Parquets antigos sem `q_conv_fisica` é suportada via fallback.
- Não há mudança no cálculo de `__qtd_decl_final_audit__`.

Observações:

- Tests foram adicionados para garantir a compatibilidade e a separação semântica entre `q_conv` e `q_conv_fisica`.
