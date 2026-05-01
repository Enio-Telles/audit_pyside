# Bench — infraestrutura de benchmark reproduzivel

Benchmarks de performance para o pipeline de transformacao.
Ficam fora de `tests/` e fora do CI padrao para nao impactar
o tempo do ciclo de desenvolvimento.

## Pre-requisitos

```bash
uv pip install pytest-benchmark>=4.0
# ou
pip install pytest-benchmark>=4.0
```

## Como rodar

```bash
# Executar todos os benchmarks (minimo 3 rounds cada)
pytest bench/ --benchmark-only --benchmark-min-rounds=3 -v

# Executar apenas um grupo
pytest bench/ --benchmark-only -k normalizacao

# Salvar resultado para comparacao posterior
pytest bench/ --benchmark-only --benchmark-save=baseline --benchmark-min-rounds=5

# Comparar com resultado salvo
pytest bench/ --benchmark-only --benchmark-compare=baseline

# Ativar fixture de 1M de descricoes (mais lento)
pytest bench/ --benchmark-only --bench-1m
```

## Nomear salvamentos

Use nomes descritivos ao salvar:

```bash
pytest bench/ --benchmark-only --benchmark-save=antes_da_optimizacao
# faca a otimizacao...
pytest bench/ --benchmark-only --benchmark-save=depois_da_optimizacao
pytest bench/ --benchmark-compare=antes_da_optimizacao
```

Os resultados ficam em `.benchmarks/` (gitignored).

## Por que fora do CI padrao

- Benchmarks sao sensiveis ao ambiente (CPU, memoria, carga do host).
  Rodados no CI compartilhado, os numeros seriam ruidosos e enganosos.
- O CI padrao (`pytest -m "not gui_smoke and not bench"`) permanece rapido.
- Acione via workflow manual (`Actions > manual-bench`) quando quiser
  uma medicao comparativa em ambiente controlado.

## Estrutura

```
bench/
  conftest.py                   # fixtures deterministicas (seed=42)
  test_normalizacao_descricao.py  # baseline vs. expr_normalizar_descricao
  test_calculo_saldos.py          # gerar_eventos_estoque + saldo anual
  data/                           # parquets gerados on-demand (gitignored)
  README.md                       # este arquivo
```

## Interpretando os resultados

| Coluna          | Significado                              |
|-----------------|------------------------------------------|
| `min`           | Melhor caso (elimina jitter de OS)       |
| `mean`          | Media aritmetica dos rounds             |
| `stddev`        | Desvio padrao — indica estabilidade      |
| `rounds`        | Numero de rounds executados              |
| `iterations`    | Iteracoes por round (warmup automatico) |

Para comparacoes entre otimizacoes, use `min` como referencia principal.
