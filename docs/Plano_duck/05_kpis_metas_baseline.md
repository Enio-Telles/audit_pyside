# 05 — KPIs, Metas e Baseline

## Metas de Performance
| Operação | Meta (SSD Local) |
|---|---|
| Abrir primeira página (2 GB) | < 5 s |
| RAM adicional (2 GB arquivo) | < 1 GB |
| Trocar página | < 2 s |
| Filtro por `id_agrupado` | < 5 s |

## Baseline Atual
Os resultados das medições oficiais de baseline encontram-se em:
- [docs/baseline_performance.md](../baseline_performance.md)
- [docs/baseline_performance.json](../baseline_performance.json)

## Como rodar Benchmarks
Use as ferramentas consolidadas em `bench/`:
```bash
uv run python bench/data/generate_fixtures.py
uv run pytest bench/test_gui_performance.py
```
