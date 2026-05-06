# Baseline de Performance (Arquivos Sintéticos)

Este relatório apresenta as medições de baseline para operações comuns na GUI utilizando diferentes tamanhos de arquivos Parquet sintéticos.

## Ambiente
- Data: 2026-05-05T16:50:56
- Python: 3.12.13

## Resultados das Medições

| Tamanho (MB) | Operação | Backend | P95 Time (s) | RSS Delta (MB) |
|---|---|---|---|---|
| 256 | ttfp | polars | 0.039s | +24.5 MB |
| 256 | page_change | polars | 0.027s | +6.9 MB |
| 256 | filter_apply | polars | 0.318s | +343.9 MB |
| 256 | export_50k | polars | 0.063s | -230.9 MB |
| 1000 | ttfp | duckdb | 0.121s | -73.0 MB |
| 1000 | page_change | duckdb | 0.062s | +15.4 MB |
| 1000 | filter_apply | duckdb | 0.948s | +16.5 MB |
| 1000 | export_50k | duckdb | 0.077s | +45.1 MB |
| 2048 | ttfp | duckdb | 0.123s | -146.3 MB |
| 2048 | page_change | duckdb | 0.065s | +47.8 MB |
| 2048 | filter_apply | duckdb | 0.810s | -9.6 MB |
| 2048 | export_50k | duckdb | 0.091s | +37.9 MB |

## Resumo por Tamanho

### Arquivo de 256 MB
| KPI | Meta | P95 Medido | Resultado |
|---|---|---|---|
| TTFP | <= 5.0s | 0.039s | [PASS] |
| Page Change | <= 2.0s | 0.027s | [PASS] |
| Filter Apply | <= 5.0s | 0.318s | [PASS] |
| Export 50k rows | N/A | 0.063s | - |

### Arquivo de 1000 MB
| KPI | Meta | P95 Medido | Resultado |
|---|---|---|---|
| TTFP | <= 5.0s | 0.121s | [PASS] |
| Page Change | <= 2.0s | 0.062s | [PASS] |
| Filter Apply | <= 5.0s | 0.948s | [PASS] |
| Export 50k rows | N/A | 0.077s | - |

### Arquivo de 2048 MB
| KPI | Meta | P95 Medido | Resultado |
|---|---|---|---|
| TTFP | <= 5.0s | 0.123s | [PASS] |
| Page Change | <= 2.0s | 0.065s | [PASS] |
| Filter Apply | <= 5.0s | 0.810s | [PASS] |
| Export 50k rows | N/A | 0.091s | - |

