# Correção: Cálculo de Estoque Inicial e Final na Tabela Anual

## Problema Identificado

Ao analisar o cálculo do `estoque_inicial` e `estoque_final` na tabela anual (`aba_anual_<cnpj>.parquet`) para o `id_agrupado_23` em 2021, foi identificado que:

1. **Estoque inicial** (`Tipo_operacao = 0 - ESTOQUE INICIAL`) só era capturado quando a data era **01/01**
2. **Estoque final** (`Tipo_operacao = 3 - ESTOQUE FINAL`) só era capturado quando a data era **31/12**

Isso causava perda de dados de inventário declarados em outras datas do ano, resultando em:
- `estoque_inicial` = 0 quando o inventário inicial ocorria em data diferente de 01/01
- `estoque_final` = 0 quando o inventário final ocorria em data diferente de 31/12
- `saidas_calculadas` incorretas (fórmula: `estoque_inicial + entradas + entradas_desacob - estoque_final`)

## Causa Raiz

No arquivo `src/transformacao/movimentacao_estoque_pkg/movimentacao_estoque.py`, as expressões de cálculo tinham restrições de data:

```python
# Código ANTES da correção (linhas 497-517):

# __qtd_decl_final_audit__ só capturava em 31/12
pl.when(
    pl.col("Tipo_operacao").cast(pl.Utf8, strict=False).str.starts_with("3 - ESTOQUE FINAL")
    & (pl.col("__data_ref_calc__").dt.month() == 12)
    & (pl.col("__data_ref_calc__").dt.day() == 31)
)
.then(q_conv_valido_expr)
.otherwise(pl.lit(0.0))
.alias("__qtd_decl_final_audit__"),

# q_conv de estoque inicial só capturava em 01/01
pl.when(
    pl.col("Tipo_operacao").cast(pl.Utf8, strict=False).str.starts_with("0 - ESTOQUE INICIAL")
    & (pl.col("__data_ref_calc__").dt.month() == 1)
    & (pl.col("__data_ref_calc__").dt.day() == 1)
)
.then(q_conv_valido_expr)
# ...
```

## Solução Aplicada

Foram removidas as restrições de data para permitir auditoria anual completa:

```python
# Código APÓS a correção:

# __qtd_decl_final_audit__: captura TODO estoque final do ano
pl.when(
    pl.col("Tipo_operacao").cast(pl.Utf8, strict=False).str.starts_with("3 - ESTOQUE FINAL")
)
.then(q_conv_valido_expr)
.otherwise(pl.lit(0.0))
.alias("__qtd_decl_final_audit__"),

# q_conv: estoque inicial capturado em qualquer data
pl.when(
    pl.col("Tipo_operacao").cast(pl.Utf8, strict=False).str.starts_with("0 - ESTOQUE INICIAL")
)
.then(q_conv_valido_expr)
# ...
```

## Arquivos Modificados

### 1. `src/transformacao/movimentacao_estoque_pkg/movimentacao_estoque.py`
- **Linhas 496-503**: Removida restrição de 31/12 para `__qtd_decl_final_audit__`
- **Linhas 504-527**: Removida restrição de 01/01 para `q_conv` e `__q_conv_sinal__` de estoque inicial

### 2. `docs/mov_estoque.md`
- Atualizada seção "Quantidade convertida" para refletir nova regra
- Adicionada nota sobre captura de estoque em qualquer data
- Atualizada seção "Estoque final auditado" com esclarecimentos

### 3. `docs/tabela_anual.md`
- Atualizada seção "Quantitativos anuais" com notas sobre as datas
- Adicionada nota sobre remoção das restrições de 01/01 e 31/12

### 4. `tests/test_estoque_inicial_final_anual.py` (novo)
- Criado teste unitário completo validando a correção
- Testa estoque inicial/final em datas arbitrárias do ano
- Valida fórmulas de cálculo da tabela anual

## Validação

Os testes confirmam que agora:

✅ **Estoque inicial** é capturado em **qualquer data** do ano
✅ **Estoque final** é capturado em **qualquer data** do ano  
✅ A soma na tabela anual reflete corretamente todos os estoques do ano
✅ As fórmulas de `saidas_calculadas`, `saidas_desacob` e `estoque_final_desacob` funcionam corretamente

## Impacto na Auditoria

Esta correção garante que:

1. **Inventários de meio de ano** sejam capturados para auditoria
2. **Estoques declarados em datas arbitrárias** (ex: inventário rotativo) sejam considerados
3. A **tabela anual** tenha visão completa de todos os estoques do período
4. A **rastreabilidade** seja preservada conforme o conceito de Golden Thread

## Nota sobre `q_conv` vs `__qtd_decl_final_audit__`

É importante notar que:

- **`q_conv` de ESTOQUE FINAL continua sendo 0** (por design, não impacta saldo físico)
- **`__qtd_decl_final_audit__` de ESTOQUE FINAL agora é capturado** (para auditoria na tabela anual)
- **`q_conv` de ESTOQUE INICIAL agora é capturado** (impacta saldo inicial)

Isso preserva a regra de negócio onde:
- Estoque inicial/final **não altera** o saldo físico calculado sequencialmente
- Estoque final **apenas audita** a quantidade declarada no inventário

## Como Reprocessar

Para aplicar a correção aos dados existentes:

```bash
# Reprocessar movimentacao_estoque (gera mov_estoque com nova regra)
python src/transformacao/movimentacao_estoque_pkg/movimentacao_estoque.py <CNPJ>

# Reprocessar calculos_anuais (gera aba_anual com correção)
python src/transformacao/calculos_anuais_pkg/calculos_anuais.py <CNPJ>
```

Ou via interface gráfica:
1. Abrir aplicação com `python app.py`
2. Selecionar CNPJ
3. Executar pipeline a partir de `movimentacao_estoque`
4. Executar `calculos_anuais`
