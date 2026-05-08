# Baseline de Performance (Arquivos Sintéticos)

Este relatório apresenta medições de baseline para operações comuns na GUI (fixtures conforme política D4).

## Ambiente
- **Data:** 2026-05-08T00:36:10
- **SO:** Linux 6.8.0
- **CPU:** x86_64
- **RAM:** 7.77 GB
- **Python:** 3.12.13
- **Polars:** 1.40.1
- **DuckDB:** 1.5.2
- **PyArrow:** 24.0.0
- **Rounds:** 2

## Resultados das Medições

| Tamanho (MB) | Operação | Backend | Mean (s) | P95 (s) | RSS Delta | CV% |
|---|---|---|---|---|---|---|
| 256 | ttfp | polars | 0.044s | 0.084s | +10.9 MB | 127.11% |
| 256 | page_2 | polars | 0.011s | 0.022s | +0.7 MB | 136.62% |
| 256 | filter_apply | polars | 1.115s | 2.196s | +455.3 MB | 137.06% |
| 256 | distinct | polars | 1.159s | 1.862s | +187.0 MB | 85.75% |
| 256 | export_50k | polars | 2.607s | 4.081s | +152.8 MB | 79.99% |
| 1024 | ttfp | duckdb | 0.138s | 0.164s | -1.6 MB | 27.57% |
| 1024 | page_2 | duckdb | 0.052s | 0.054s | +3.3 MB | 5.94% |
| 1024 | filter_apply | duckdb | 0.392s | 0.413s | +5.8 MB | 7.63% |
| 1024 | distinct | duckdb | 0.117s | 0.118s | -0.7 MB | 1.4% |
| 1024 | export_50k | duckdb | 28.317s | 42.031s | -291.1 MB | 68.49% |

## Resumo por Tamanho

### Arquivo de 256 MB
| KPI | Meta | Valor (P95/Mean) | Resultado |
|---|---|---|---|
| TTFP | <= 5.0s | 0.084s | [PASS] |
| Page Change (p2) | <= 2.0s | 0.022s | [PASS] |
| Filter Apply | <= 5.0s | 2.196s | [PASS] |
| Distinct Values | <= 1.0s | 1.862s | [FAIL] |
| Export 50k rows | <= 2.0s | 4.081s | [FAIL] |

### Arquivo de 1024 MB
| KPI | Meta | Valor (P95/Mean) | Resultado |
|---|---|---|---|
| TTFP | <= 5.0s | 0.164s | [PASS] |
| Page Change (p2) | <= 2.0s | 0.054s | [PASS] |
| Filter Apply | <= 5.0s | 0.413s | [PASS] |
| Distinct Values | <= 1.0s | 0.118s | [PASS] |
| Export 50k rows | <= 2.0s | 42.031s | [FAIL] |
