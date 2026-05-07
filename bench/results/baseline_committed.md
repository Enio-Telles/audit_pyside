# Baseline KPIs — audit_pyside

_Capturado em 2026-05-07T20:07:14+00:00._

## Sumário

- **Parquets benchmarcados:** 3
- **Total de linhas:** 5,517,464
- **Tamanho total on-disk:** 231.1 MB
- **Polars version:** 1.40.1

## KPIs agregados (mediana das medianas por Parquet)

| KPI | Unit | Mediana | P95 | Min | Max | Runs |
|---|---|---:|---:|---:|---:|---:|
| `file_size_on_disk` | MB | 24.2489 | 24.2489 | 15.2007 | 191.6355 | 3 |
| `filter_cfop_in` | ms | 0.0 | 0.0 | 0.0 | 0.0 | 3 |
| `filter_cst_eq` | ms | 0.0 | 0.0 | 0.0 | 1.6567 | 3 |
| `groupby_id_agrupado_sum` | ms | 11.9374 | 11.9374 | 5.7134 | 82.1649 | 3 |
| `invariants_hash` | mixed | 1.0 | 1.0 | 0.0 | 1.0 | 3 |
| `pagination_p95` | ms | 0.0 | 0.0 | 0.0 | 0.0 | 3 |
| `rle_dictionary_coverage` | pct | 0.0 | 0.0 | 0.0 | 0.0 | 3 |
| `rss_peak_full_scan` | MB | 124.2578 | 124.2578 | 85.7422 | 502.8086 | 3 |

## Detalhe por Parquet

### `produtos_final_04240370002877.parquet`

- Path: `C:\audit_pyside\dados\CNPJ\04240370002877\analises\produtos\produtos_final_04240370002877.parquet`
- Linhas: 55,296
- Tamanho: 15.2 MB
- KPIs (mediana):
  - `rss_peak_full_scan`: 85.7422
  - `groupby_id_agrupado_sum`: 5.7134
  - `filter_cfop_in`: 0.0
  - `filter_cst_eq`: 0.0
  - `pagination_p95`: 0.0
  - `file_size_on_disk`: 15.2007
  - `rle_dictionary_coverage`: 0.0
  - `invariants_hash`: 1.0

### `tb_documentos_04240370002877.parquet`

- Path: `C:\audit_pyside\dados\CNPJ\04240370002877\analises\produtos\tb_documentos_04240370002877.parquet`
- Linhas: 4,967,008
- Tamanho: 191.6 MB
- KPIs (mediana):
  - `rss_peak_full_scan`: 502.8086
  - `groupby_id_agrupado_sum`: 82.1649
  - `filter_cfop_in`: 0.0
  - `filter_cst_eq`: 0.0
  - `pagination_p95`: 0.0
  - `file_size_on_disk`: 191.6355
  - `rle_dictionary_coverage`: 0.0
  - `invariants_hash`: 0.0

### `c170_xml_04240370002877.parquet`

- Path: `C:\audit_pyside\dados\CNPJ\04240370002877\arquivos_parquet\c170_xml_04240370002877.parquet`
- Linhas: 495,160
- Tamanho: 24.2 MB
- KPIs (mediana):
  - `rss_peak_full_scan`: 124.2578
  - `groupby_id_agrupado_sum`: 11.9374
  - `filter_cfop_in`: 0.0
  - `filter_cst_eq`: 1.6567
  - `pagination_p95`: 0.0
  - `file_size_on_disk`: 24.2489
  - `rle_dictionary_coverage`: 0.0
  - `invariants_hash`: 1.0

## Como usar este baseline

Após gerar este JSON, **comitá-lo** em `bench/results/baseline_committed.json` para que o workflow `perf-gates.yml` da CI tenha referência. Para regenerar:

```bash
uv run python bench/capture_baseline.py \
    --root /data/audit_pyside/parquets/<CNPJ> \
    --output bench/results/baseline_committed.json \
    --report bench/results/baseline_report.md
```

Repetir a cada release/onda significativa, ou anualmente quando o cadastro `ref/fiscal_codes_YYYY.json` for atualizado.
