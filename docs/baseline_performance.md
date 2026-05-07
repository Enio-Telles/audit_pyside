# Baseline de Performance (Arquivos Sintéticos)

Este relatório apresenta as medições de baseline para operações comuns na GUI utilizando arquivos Parquet sintéticos.

## Ambiente
- **Data:** 2026-05-07T18:55:13
- **SO:** N/A
- **CPU:** N/A
- **RAM:** N/A GB
- **Disco:** N/A
- **Python:** 3.12.13
- **Polars:** N/A
- **DuckDB:** N/A
- **Rounds:** 2

## Resultados das Medições

| Tamanho (MB) | Operação | Backend | Mean (s) | P95 (s) | RSS Delta | CV% |
|---|---|---|---|---|---|---|
| 256 | ttfp | polars | 0.021s | 0.039s | +11.6 MB | 117.72% |
| 256 | page_2 | polars | 0.012s | 0.023s | +3.0 MB | 136.23% |
| 256 | page_3 | polars | 0.011s | 0.021s | +1.6 MB | 135.79% |
| 256 | page_10 | polars | 0.010s | 0.020s | +1.2 MB | 135.52% |
| 256 | filter_contem | polars | 1.204s | 2.339s | +776.7 MB | 133.36% |
| 256 | distinct | polars | 1.181s | 2.126s | +207.8 MB | 113.07% |
| 1024 | ttfp | duckdb | 0.129s | 0.156s | -25.9 MB | 29.72% |
| 1024 | page_2 | duckdb | 0.060s | 0.061s | +5.0 MB | 4.38% |
| 1024 | page_3 | duckdb | 0.059s | 0.060s | -5.0 MB | 3.57% |
| 1024 | page_10 | duckdb | 0.064s | 0.071s | +2.2 MB | 15.2% |
| 1024 | filter_contem | duckdb | 0.444s | 0.458s | +7.3 MB | 4.55% |
| 1024 | distinct | duckdb | 0.135s | 0.140s | -0.9 MB | 5.59% |

## Resumo por Tamanho

### Arquivo de 256 MB
| KPI | Meta | Valor (P95/Mean) | Resultado |
|---|---|---|---|
| TTFP | <= 5.0s | 0.039s | [PASS] |
| Page Change (p2) | <= 2.0s | 0.023s | [PASS] |
| Filter Apply | <= 5.0s | 2.339s | [PASS] |
| Distinct Values | <= 1.0s | 2.126s | [FAIL] |

### Arquivo de 1024 MB
| KPI | Meta | Valor (P95/Mean) | Resultado |
|---|---|---|---|
| TTFP | <= 5.0s | 0.156s | [PASS] |
| Page Change (p2) | <= 2.0s | 0.061s | [PASS] |
| Filter Apply | <= 5.0s | 0.458s | [PASS] |
| Distinct Values | <= 1.0s | 0.140s | [PASS] |
