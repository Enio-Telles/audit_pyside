# Differential Harness - Guia de uso

O contrato canonico do gate vive em [docs/wiki/Differential-harness-contrato-e-guardas.md](wiki/Differential-harness-contrato-e-guardas.md).
Este guia resume a execucao local e aponta para os modulos que o harness usa.

## O que o harness valida

O differential harness garante que mudancas em `src/transformacao/` nao alteram as 5 chaves invariantes:
`id_agrupado`, `id_agregado`, `__qtd_decl_final_audit__`, `q_conv`, `q_conv_fisica`.

Ele reforca o gate com 3 niveis:
1. divergencias por chave nas 5 invariantes;
2. conservacao de massa por fonte (`nfe`, `nfce`, `c170`, `bloco_h`);
3. detector de colapso e tripwire downstream em `mov_estoque`.

O CLI falha fechado se um artefato esperado estiver stale: o parquet precisa ter sido gerado no run atual.

## Modulos do harness

```
tests/diff_harness/
    invariantes.py
    golden_dataset.py
    nivel_1_divergencias.py
    nivel_2_conservacao.py
    nivel_3_colapso_tripwire.py
    differential_report.py
    pipeline_runner.py
    run_harness.py
    run_harness_cli.py
    golden/               # parquets on-demand (gitignored)
    _snapshots/           # snapshots canonicos de render
```

## Como usar

### 1. Preparar a mudanca

Edite o modulo alvo em `src/transformacao/` e crie um teste diferencial em `tests/diff_harness/`.

### 2. Rodar a suite local

```bash
python -m pytest tests/diff_harness -q
```

### 3. Rodar o CLI canonico

```bash
python tests/diff_harness/run_harness_cli.py \
    --baseline-commit <sha-de-main> \
    --novo-commit HEAD \
    --cnpj 04240370002877 \
    --cnpj 84654326000394 \
    --out reports/diff/
```

Se nenhum `--cnpj` for informado, o CLI usa os dois CNPJs amostra da wiki.

### 4. Anexar o relatorio na PR

**NUNCA** commite o relatório gerado (`reports/diff/diff-*.txt`) no repositório.
Cole o texto de `DifferentialReport.render()` no corpo da PR ou anexe o arquivo manualmente na interface do GitHub.

### 5. Adicionar o label `differential-validated`

Se a PR tocar qualquer arquivo read-only, o workflow `diff-harness.yml` exige o label `differential-validated`.

## Arquivos read-only

| Arquivo | Motivo |
|---|---|
| `src/transformacao/rastreabilidade_produtos/_produtos_final_impl.py` | Logica fiscal de produtos finais |
| `src/transformacao/rastreabilidade_produtos/fatores_conversao.py` | Fatores de conversao de unidades |
| `src/transformacao/fatores_conversao.py` | Fatores canonicos |
| `src/transformacao/movimentacao_estoque.py` | Orquestrador de estoque |
| `src/transformacao/movimentacao_estoque_pkg/calculo_saldos.py` | Calculo de saldos (Numba) |
| `src/transformacao/movimentacao_estoque_pkg/movimentacao_estoque.py` | Pipeline fiscal de estoque |

## Contrato do relatório

O `DifferentialReport` registra:
- `FonteArtefatos` para principal, `sem_id` e `fora_escopo` de cada fonte;
- `StatusFonte` com `APROVADO`, `REPROVADO` e `STALE`;
- `aprovado_global` como veredito final;
- `assert_artefato_nao_stale()` para bloquear parquet reutilizado de um run antigo;
- `_eq_nan_safe()` para tratar `NaN` como igual a `NaN` no nivel 1.

## FAQ

**Por que tolerancia zero?**
Qualquer diferenca em `q_conv` ou `id_agrupado` propaga erro fiscal para as etapas downstream.

**Por que dois CNPJs?**
Um CNPJ grande valida escala e um menor cobre cantos do pipeline e regressao #196.

**O harness ficou mais lento?**
O custo adicional vem do nivel 2/3 e do carregamento de artefatos auxiliares, mas o objetivo e fechar falsos verdes.
