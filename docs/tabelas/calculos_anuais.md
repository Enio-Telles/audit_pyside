# calculos_anuais

## Visão Geral

Tabela que resume a auditoria anual por produto, confrontando estoque inicial, entradas, saídas, estoque final declarado e saldo final calculado, com reflexos de ICMS sobre saídas e estoque desacobertados.

## Função de Geração

```python
def gerar_calculos_anuais(cnpj: str, pasta_cnpj: Path | None = None) -> bool
```

Módulo: `src/transformacao/calculos_anuais.py` (wrapper)  
Implementação: `src/transformacao/calculos_anuais_pkg/calculos_anuais.py`

## Dependências

- **Depende de**: `movimentacao_estoque`
- **É dependência de**: nenhuma (tabela de saída analítica)

## Fontes de Entrada

- `mov_estoque_<cnpj>.parquet`
- Classificações SEFIN (`sitafe_produto_sefin_aux.parquet`)

## Objetivo

Consolidar a auditoria anual por produto, identificando:

- Divergências entre estoque declarado e calculado
- Saídas desacobertadas (sem entrada correspondente)
- Estoque final desacobertado (omissão de entrada)
- ICMS devido sobre divergências
- PME e PMS anuais

## Principais Colunas

| Coluna | Tipo | Descrição |
|--------|------|-----------|
| `id_agregado` | str | Chave do produto agrupado (renomeado de `id_agrupado`) |
| `ano` | int | Ano civil |
| `descr_padrao` | str | Descrição padrão do produto |
| `unid_ref` | str | Unidade de referência |
| `estoque_inicial` | float | Soma de `q_conv` do estoque inicial |
| `entradas` | float | Soma de `q_conv` das entradas |
| `saidas` | float | Soma de `q_conv` das saídas |
| `estoque_final` | float | Soma de `__qtd_decl_final_audit__` do estoque final |
| `entradas_desacob` | float | Soma anual de `entr_desac_anual` |
| `saldo_final` | float | Último `saldo_estoque_anual` do ano |
| `saidas_calculadas` | float | `estoque_inicial + entradas + entradas_desacob - estoque_final` |
| `saidas_desacob` | float | `max(estoque_final - saldo_final, 0)` |
| `estoque_final_desacob` | float | `max(saldo_final - estoque_final, 0)` |
| `pme` | float | Preço médio de entrada anual |
| `pms` | float | Preço médio de saída anual |
| `ST` | str | Histórico de períodos de ST do ano |
| `aliq_interna` | float | Alíquota interna (%) |
| `ICMS_saidas_desac` | float | ICMS sobre saídas desacobertadas |
| `ICMS_estoque_desac` | float | ICMS sobre estoque desacobertado |

## Regras de Processamento

### Quantitativos Anuais

**Agregações físicas:**
```
estoque_inicial = soma(q_conv) das linhas "0 - ESTOQUE INICIAL" (qualquer data)
entradas = soma(q_conv) das linhas "1 - ENTRADA"
saidas = soma(q_conv) das linhas "2 - SAIDAS"
estoque_final = soma(__qtd_decl_final_audit__) das linhas "3 - ESTOQUE FINAL" (qualquer data)
entradas_desacob = soma anual de entr_desac_anual
saldo_final = último saldo_estoque_anual do ano
```

**Nota:** Restrições anteriores de data (01/01 para estoque inicial, 31/12 para estoque final) foram removidas. Agora todo estoque inicial/final do ano é capturado para auditoria.

### Fórmulas Principais

```
saidas_calculadas = estoque_inicial + entradas + entradas_desacob - estoque_final
saidas_desacob = max(estoque_final - saldo_final, 0)
estoque_final_desacob = max(saldo_final - estoque_final, 0)
```

`saidas_desacob` e `estoque_final_desacob` são mutuamente exclusivos. Se um é positivo, o outro fica zerado.

### PME e PMS Anuais

Excluem-se movimentos inválidos:

- Devoluções: `dev_simples`, `dev_venda`, `dev_compra`, `dev_ent_simples` ou `finnfe = 4`
- Linhas com `excluir_estoque = true`
- Linhas com `q_conv <= 0`

**Fórmulas:**
```
pme = soma(valor das entradas válidas) / soma(qtd das entradas válidas)
pms = soma(valor das saídas válidas) / soma(qtd das saídas válidas)
```

O valor unitário usa `preco_item` e, na falta dele, `Vl_item`.

### Regra Anual de ST

Cruza `co_sefin_agr` com `sitafe_produto_sefin_aux.parquet` e mantém vigências que intersectam o ano analisado.

**Campos:**
- `ST`: histórico textual dos períodos anuais
- `__tem_st_ano__`: flag interna
- `aliq_interna`: prioridade para alíquota da referência SEFIN, com fallback para última alíquota da movimentação

### ICMS Anual

**Base de saída:**
```
se pms > 0:
    base_saida = saidas_desacob * pms
senão:
    base_saida = saidas_desacob * pme * 1.30
```

**Base de estoque:**
```
se pms > 0:
    base_estoque = estoque_final_desacob * pms
senão:
    base_estoque = estoque_final_desacob * pme * 1.30
```

**Aplicação da alíquota:**
```
aliq_factor = aliq_interna / 100
ICMS_saidas_desac = base_saida * aliq_factor
ICMS_estoque_desac = base_estoque * aliq_factor
```

**Regra de ST:**
- Se `__tem_st_ano__ = true`, então `ICMS_saidas_desac = 0`
- `ICMS_estoque_desac` não é zerado por ST

### Arredondamento

- Quantidades e saldos: 4 casas decimais
- `pme`, `pms`, `aliq_interna`, `ICMS_saidas_desac`, `ICMS_estoque_desac`: 2 casas decimais

## Saída Gerada

```
dados/CNPJ/<cnpj>/analises/produtos/aba_anual_<cnpj>.parquet
```

## Notas

- `id_agregado` é o nome de saída para `id_agrupado`
- Essencial para auditoria fiscal anual e apuração de ICMS
- `saidas_desacob` indica saídas sem cobertura de entrada
- `estoque_final_desacob` indica possível omissão de entrada
- O multiplicador 1.30 é aplicado quando não há PMS (preço de venda)
- Se há ST vigente, ICMS sobre saídas desacobertadas é zerado
