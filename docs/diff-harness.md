# Differential Harness — Guia de uso

O differential harness valida que mudancas em `src/transformacao/` nao alteram
as 5 chaves invariantes: `id_agrupado`, `id_agregado`, `__qtd_decl_final_audit__`,
`q_conv`, `q_conv_fisica`.

## Por que existe

PRs `perf` ou `refactor` em `src/transformacao/` podem parecer inocentes mas
alterar silenciosamente o calculo fiscal. Este harness exige uma prova byte-a-byte
antes do merge.

## Arquivos do harness

```
tests/diff_harness/
  __init__.py
  golden_dataset.py     # load_golden(seed=42, n_rows=100_000)
  run_harness.py        # run_harness(impl_old, impl_new) -> DifferentialReport
  golden/               # parquets on-demand (gitignored)
  test_baseline_self.py # contrato: run_harness(f, f) == 0 divergencias
```

## Passo a passo para uma PR de refatoracao em src/transformacao/

### 1. Criar branch

```bash
git checkout main
git checkout -b refactor/minha-otimizacao
```

### 2. Capturar a versao antiga via git show

```bash
# Antes de qualquer mudanca, exportar a versao atual do modulo
git show main:src/transformacao/movimentacao_estoque_pkg/calculo_saldos.py \
  > /tmp/calculo_saldos_old.py
```

### 3. Fazer a mudanca em src/transformacao/

Editar o arquivo alvo normalmente.

### 4. Criar o teste diferencial

```python
# tests/diff_harness/test_calculo_saldos_refactor.py
import importlib.util, sys
import polars as pl
import pytest
from tests.diff_harness.golden_dataset import load_golden
from tests.diff_harness.run_harness import run_harness

pytestmark = pytest.mark.diff_harness


def _carregar_modulo_antigo(path: str):
    spec = importlib.util.spec_from_file_location("calculo_saldos_old", path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _impl_old(df: pl.DataFrame) -> pl.DataFrame:
    mod = _carregar_modulo_antigo("/tmp/calculo_saldos_old.py")
    return mod.calcular_saldo_estoque_anual(df)


def _impl_new(df: pl.DataFrame) -> pl.DataFrame:
    from transformacao.movimentacao_estoque_pkg.calculo_saldos import (
        calcular_saldo_estoque_anual,
    )
    return calcular_saldo_estoque_anual(df)


def test_refactor_zero_divergencias() -> None:
    dataset = load_golden(seed=42, n_rows=50_000)
    report = run_harness(_impl_old, _impl_new, dataset=dataset)
    assert not report.tem_divergencia, report.resumo()
```

### 5. Rodar o harness

```bash
pytest -m diff_harness -v --tb=short
```

### 6. Anexar o relatorio no corpo da PR

Cole o output do pytest no corpo da PR. Exemplo:

```
## Differential Report

pytest -m diff_harness -v

3 passed in 1.2s
OK: 50000 linhas, 0 divergencias em todas as 5 chaves.
```

### 7. Adicionar o label differential-validated

No GitHub, adicione o label `differential-validated` na PR.
Sem ele, o workflow `diff-harness.yml` bloqueia o merge quando
a PR toca arquivos read-only.

## Arquivos read-only (requerem label)

| Arquivo | Motivo |
|---------|--------|
| `src/transformacao/rastreabilidade_produtos/_produtos_final_impl.py` | Logica fiscal de produtos finais |
| `src/transformacao/rastreabilidade_produtos/fatores_conversao.py` | Fatores de conversao de unidades |
| `src/transformacao/fatores_conversao.py` | Fatores canonicos |
| `src/transformacao/movimentacao_estoque.py` | Orquestrador de estoque |
| `src/transformacao/movimentacao_estoque_pkg/calculo_saldos.py` | Calculo de saldos (Numba) |

## DifferentialReport — estrutura

```python
@dataclass
class DifferentialReport:
    total_rows: int
    divergentes: dict[str, int]   # chave -> n linhas divergentes
    amostras: dict[str, list[dict]]  # chave -> primeiras 10 divergencias

# Cada divergencia tem:
# { "linha": int, "input": dict, "old": valor_antigo, "new": valor_novo }
```

## FAQ

**Por que tolerancia zero?**
Qualquer diferenca em `q_conv` ou `id_agrupado` propaga erro fiscal para
todas as etapas downstream. Nao existe "diferenca aceitavel".

**Posso usar seed diferente?**
Sim: `load_golden(seed=99, n_rows=200_000)`. O cache so e gerado para
seed=42, n_rows=100_000.

**O harness e lento?**
`load_golden(n_rows=10_000)` para testes rapidos no desenvolvimento;
`n_rows=100_000` (padrao) para a validacao final antes do merge.
