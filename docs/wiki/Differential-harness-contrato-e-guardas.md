# Differential harness — contrato e guardas

> Contrato tecnico do `tests/diff_harness/` para PRs `perf`/`refactor` em `src/transformacao/`. Esta pagina e a referencia canonica apontada por `src/transformacao/AGENTS.md` e pelo gate `transformacao_perf_or_refactor_gate`.

## Objetivo

Dar evidencia **byte-a-byte** de que mudancas em hot paths de transformacao nao alteram nenhuma das 5 invariantes fiscais e nao fazem fontes inteiras colapsarem silenciosamente. E a contraparte testavel dos golden hashes (P10-01).

## Escopo de aplicacao

| Tipo de PR | Harness obrigatorio? | Justificativa |
|---|---|---|
| `perf` em `src/transformacao/` | **Sim** | Qualquer ganho de performance so e aceitavel se preserva saida. |
| `refactor` em `src/transformacao/` | **Sim** | Refatoracao nao pode mudar comportamento observavel. |
| `fix` em `src/transformacao/` que toca filtro/join/agregacao | **Sim** | Ex.: PR #196 (separacao `fora_escopo_canonico`). |
| `feat` que adiciona novo campo/saida sem alterar existentes | Recomendado | Use harness para provar que campos antigos nao regrediram. |
| `docs`, `chore`, `test` puro, mudancas em `src/interface_grafica/` | Nao | Sem impacto em transformacao. |

## As 5 invariantes fiscais

| Campo | Tabela primaria | Como verificar |
|---|---|---|
| `id_agrupado` | `produtos_final`, `*_agr_*.parquet` | Igualdade de string em intersecao por chave estavel. |
| `id_agregado` | `calculos_*` | Alias de `id_agrupado` — igualdade direta. |
| `__qtd_decl_final_audit__` | `calculos_periodos`, `aba_resumo_global` | Igualdade numerica exata (nao tolerancia). |
| `q_conv` | `movimentacao_estoque` | Igualdade numerica exata. |
| `q_conv_fisica` | `movimentacao_estoque` | Igualdade numerica exata. |

> Tolerancia numerica = **0**. Float comparado bit-a-bit. Se houver jitter, corrija a fonte — nao relaxe o gate.

## Os tres niveis de gate

Uma PR so passa pelo `transformacao_perf_or_refactor_gate` se atende aos tres niveis simultaneamente.

### Nivel 1 — Divergencias por chave

Para as linhas que existem nos dois lados, os valores das 5 invariantes batem?

Implementacao: `tests/diff_harness/nivel_1_divergencias.py::assert_zero_divergencias`

Chaves estaveis por etapa — ver `tests/diff_harness/invariantes.py::CHAVES_POR_ETAPA`.

### Nivel 2 — Conservacao de massa por fonte

Nenhuma linha sumiu no caminho? `baseline_principal + baseline_sem_id == novo_principal + novo_sem_id + novo_fora_escopo`

Implementacao: `tests/diff_harness/nivel_2_conservacao.py::assert_conservacao_de_massa`

Fontes auditadas: `nfe`, `nfce`, `c170`, `bloco_h`. Esta guarda foi adicionada apos o incidente NFC-e em #196.

### Nivel 3 — Detector de colapso + tripwire downstream

Alguma fonte virou vazia? `mov_estoque` perdeu mais de 1% das linhas?

Implementacao: `tests/diff_harness/nivel_3_colapso_tripwire.py`

## CNPJs-amostra obrigatorios

| CNPJ | Perfil | Por que esta na lista |
|---|---|---|
| `04240370002877` | Grande (5M+ linhas, 7 anos de NFC-e) | Stress de escala. |
| `84654326000394` | Pequeno-medio (24k NFE, 62 NFC-e) | Cobre cantos do pipeline. Detectou regressao #196. |

Um CNPJ so nao cobre — NFC-e tem encoding distinto de NFE.

## Estrutura do DifferentialReport

Formato canonico que vai no corpo da PR. Texto simples. Ver exemplo em `tests/diff_harness/differential_report.py::DifferentialReport.render`.

## Como invocar

```bash
uv run python tests/diff_harness/run_harness_cli.py \
    --baseline-commit <sha-de-main> \
    --novo-commit HEAD \
    --cnpj 04240370002877 \
    --cnpj 84654326000394 \
    --out reports/diff/
```

Exit code 0 somente se todos os CNPJs aprovarem nos 3 niveis.

## Caso de estudo — regressao NFC-e em #196 (2026-05-03)

**O que aconteceu:** PR #196 (`fix(transformacao-fontes): separar fora_escopo_canonico antes do join`) anexou um DifferentialReport que aprovou a mudanca com 0 divergencias em `id_agrupado` para NFE e NFC-e. Todas as 62 linhas de NFC-e cairaem em `fora_escopo_canonico`, deixando `nfce_agr` vazio. `mov_estoque` perdeu 572 linhas (1.9%).

**Por que o gate passou:** o harness so tinha nivel 1. Intersecao vazia = "0 divergencias" — falso-verde estrutural.

**O que mudou:** niveis 2 e 3 viraram obrigatorios. `tests/diff_harness/test_regressao_pr_196.py` reproduz o cenario e garante que o reforcado pega o bug.

## Checklist do autor antes de tirar PR de draft

- [ ] Rodei `run_harness_cli.py` em `04240370002877` E `84654326000394`.
- [ ] Os dois DifferentialReport estao colados no corpo da PR.
- [ ] Os 3 niveis aprovaram nos dois CNPJs.
- [ ] Se algum reprovou, abri ADR em `docs/adr/` e adicionei label `breaking-fiscal`.
- [ ] Se a mudanca afeta invariante intencionalmente, atualizei golden hashes em commit dedicado apos o merge.
- [ ] Atualizei a pagina de wiki da tabela afetada.

## Paginas relacionadas

- `docs/wiki/Tabelas-de-Agrupamento-Campos-e-Formulas.md` — contrato das tabelas.
- PR #179 — implementacao inicial do differential harness.
- PR #192 — golden hashes (P10-01).
- PR #196 — caso de estudo da regressao NFC-e.
