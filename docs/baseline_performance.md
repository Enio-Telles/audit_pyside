# Baseline de Performance (Arquivos Sintéticos)

Este relatório apresenta medições de baseline para operações comuns na GUI (fixtures conforme política D4).

## Ambiente
- **Data:** 2026-05-08T01:39:26
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
| 256 | ttfp | polars | 0.041s | 0.079s | +10.9 MB | 130.65% |
| 256 | page_2 | polars | 0.029s | 0.059s | +2.3 MB | 139.7% |
| 256 | filter_apply | polars | 1.481s | 2.916s | +456.1 MB | 136.97% |
| 256 | distinct | polars | 1.355s | 2.192s | +187.8 MB | 87.34% |
| 256 | export_50k | polars | 3.363s | 5.097s | +152.2 MB | 72.96% |
| 1024 | ttfp | duckdb | 0.129s | 0.174s | +2.1 MB | 49.0% |
| 1024 | page_2 | duckdb | 0.092s | 0.139s | +1.5 MB | 70.98% |
| 1024 | filter_apply | duckdb | 0.380s | 0.393s | +1.4 MB | 4.94% |
| 1024 | distinct | duckdb | 0.103s | 0.103s | -2.4 MB | 0.39% |
| 1024 | export_50k | duckdb | 19.990s | 25.466s | -289.2 MB | 38.74% |

## Resumo por Tamanho

### Arquivo de 256 MB
| KPI | Meta | Valor (P95/Mean) | Resultado |
|---|---|---|---|
| TTFP | <= 5.0s | 0.079s | [PASS] |
| Page Change (p2) | <= 2.0s | 0.059s | [PASS] |
| Filter Apply | <= 5.0s | 2.916s | [PASS] |
| Distinct Values | <= 1.0s | 2.192s | [FAIL] |
| Export 50k rows | <= 2.0s | 5.097s | [FAIL] |

### Arquivo de 1024 MB
| KPI | Meta | Valor (P95/Mean) | Resultado |
|---|---|---|---|
| TTFP | <= 5.0s | 0.174s | [PASS] |
| Page Change (p2) | <= 2.0s | 0.139s | [PASS] |
| Filter Apply | <= 5.0s | 0.393s | [PASS] |
| Distinct Values | <= 1.0s | 0.103s | [PASS] |
| Export 50k rows | <= 2.0s | 25.466s | [FAIL] |
