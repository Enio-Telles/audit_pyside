# Baseline de Performance

Use este arquivo para registrar as medições reais da Fase 0/E5 (Polars).

## Ambiente

- Data: 04 de maio de 2026
- SO: Windows
- Parquet Backend: Polars (para arquivos < 512 MB)
- Tamanho usado no Benchmark: 256 MB (~8.5M linhas sintéticas)

## Resultados por Operação (5 rounds)

| Operação | Backend | Mean | P95 | CV% | RSS Delta |
|---|---|---|---|---|---|
| ttfp | polars | 0.061s | 0.292s | 210.3% | +4.8 MB |
| page_2 | polars | 0.005s | 0.020s | 172.8% | +1.3 MB |
| page_3 | polars | 0.005s | 0.022s | 187.0% | +1.4 MB |
| page_10 | polars | 0.005s | 0.020s | 176.8% | +1.3 MB |
| filter_contem | polars | 0.384s | 1.138s | 109.8% | +330.9 MB |
| distinct | polars | 0.358s | 0.504s | 23.8% | +99.6 MB |

> Nota: O CV% alto para as operações deve-se à discrepância entre a primeira execução (fria/alocação de memória) e as subsequentes (cacheadas).

## Validação KPIs (Plano Mestre §7)

| KPI | Meta | P95 medido (256 MB) | Resultado |
|---|---|---|---|
| TTFP | <= 5.0s | 0.292s | [PASS] |
| Page Change (page_2) | <= 2.0s | 0.020s | [PASS] |
| Page Change (page_3) | <= 2.0s | 0.022s | [PASS] |
| Page Change (page_10) | <= 2.0s | 0.020s | [PASS] |
| Filter (contém) | <= 5.0s | 1.138s | [PASS] |
| Distinct values | <= 1.0s | 0.504s | [PASS] |

## Top Gargalos Observados

1. **Filter (contem)**: É a operação mais pesada (P95 de 1.138s), devido ao scan linear parcial na tabela de 8.5M de linhas e ao impacto no RSS (pico de +1.8 GB na primeira chamada, média +330 MB).
2. **Distinct**: Razoavelmente pesado na primeira chamada, mas dentro do threshold de < 1.0s.

## Hipóteses e Mitigações

- A alocação de memória do Polars (Polars String Cache / MemPool) pode resultar em aumentos de RSS elevados para filtros grandes. DuckDB é recomendado para arquivos > 512 MB justamente por ser out-of-core e mais restrito no consumo de RAM.
- A estabilidade das próximas chamadas mostra que o LRU cache cumpre seu papel em queries repetidas.
