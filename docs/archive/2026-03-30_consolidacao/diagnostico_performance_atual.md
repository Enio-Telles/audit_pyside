# Diagnostico de Performance Atual

Baseado no arquivo `logs/performance/perf_events.jsonl` existente em `24/03/2026`.

## Escopo da amostra atual

A amostra ainda esta concentrada quase toda em eventos de `ParquetService`.

Eventos encontrados:

- `parquet_service.get_page.total`
- `parquet_service.get_schema`
- `parquet_service.get_page.count_rows`
- `parquet_service.get_page.collect_page`
- `parquet_service.load_dataset`
- `parquet_service.get_page.build_lazyframe`

Total de eventos analisados:

- `29`

## Resumo consolidado

| Evento | Qtd | Media (s) | Max (s) | Cache hit true | Cache hit false |
|---|---:|---:|---:|---:|---:|
| `parquet_service.get_page.total` | 8 | 0.007942 | 0.051169 | 5 | 3 |
| `parquet_service.get_schema` | 10 | 0.004094 | 0.029849 | 8 | 2 |
| `parquet_service.get_page.count_rows` | 3 | 0.010464 | 0.030103 | 1 | 2 |
| `parquet_service.get_page.collect_page` | 3 | 0.006242 | 0.015961 | 0 | 0 |
| `parquet_service.load_dataset` | 2 | 0.004224 | 0.008283 | 1 | 1 |
| `parquet_service.get_page.build_lazyframe` | 3 | 0.000088 | 0.000094 | 0 | 0 |

## Leitura do resultado

### 1. O cache de parquet ja esta funcionando

Ha evidencia clara de ganho nas chamadas com cache:

- `get_page.total` caiu de cerca de `0.051s` no pior miss para cerca de `0.0001s` a `0.0002s` em hits de pagina
- `load_dataset` caiu de `0.008283s` para `0.000165s`
- `get_schema` passou a operar majoritariamente em cache

Conclusao:

- as otimizações recentes em `ParquetService` tiveram efeito real

### 2. O maior custo residual da camada de parquet esta em contagem e primeira coleta

Na amostra atual, os maiores tempos ainda aparecem em:

- `get_page.count_rows`
- `get_page.collect_page`
- `get_page.total` em cache miss

Conclusao:

- o problema de I/O nao desapareceu, mas agora esta mais concentrado em primeira abertura e primeira leitura paginada

### 3. O `build_lazyframe` nao e o gargalo principal

Os tempos dessa etapa ficaram praticamente nulos na amostra.

Conclusao:

- a proxima rodada de ganhos nao deve focar primeiro na construcao do lazy frame
- o foco deve continuar em contagem, primeira coleta e pipeline derivado

## O que ainda falta medir

A amostra atual ainda nao permite ranquear os gargalos do sistema inteiro, porque faltam logs de uso real em:

- `movimentacao_estoque`
- `calculos_mensais`
- `calculos_anuais`
- `aggregation_service`
- `query_worker`

Sem esses eventos em volume real, ainda nao da para afirmar se o principal gargalo global esta:

- na interface
- no parquet
- no pipeline derivado
- na consulta Oracle

## Conclusao operacional

No estado atual, a leitura mais segura e:

1. A camada de parquet melhorou e o cache esta entregando ganho real.
2. O proximo gargalo provavel esta no pipeline derivado, especialmente em `movimentacao_estoque`.
3. A proxima coleta deve executar fluxos reais de:
   - abertura de `mov_estoque`
   - abertura de `aba_mensal`
   - abertura de `aba_anual`
   - recalculo via Conversao
   - recalculo via Agregacao

## Evolucao mais recente

Depois da nova rodada de otimização em `movimentacao_estoque.py`, o mesmo CNPJ `84654326000394`
mostrou a seguinte melhora no principal gargalo:

- `movimentacao_estoque.calcular_saldo_anual`: de `19.558562s` para `9.686466s`
- `movimentacao_estoque.total`: de `20.393983s` para `10.479591s`

Leitura pratica:

- o gargalo principal continua no calculo anual por grupo
- mas houve uma reducao de aproximadamente 50% nesse trecho
- isso valida a estrategia de atacar conversoes e flags repetidas antes do agrupamento

## Proxima acao recomendada

Executar a aplicacao com um CNPJ grande e depois rodar:

```bash
python scripts/resumir_performance.py
```

O script agora mostra:

- resumo por modulo
- resumo por evento
- contagem de erros por grupo

Depois disso, comparar os eventos do `ParquetService` com:

- `movimentacao_estoque.*`
- `calculos_mensais.*`
- `calculos_anuais.*`
- `aggregation_service.*`

Se esses eventos aparecerem, o proximo passo deve ser atacar o maior total acumulado entre eles.
