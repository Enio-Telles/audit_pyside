# CLEANUP_PLAN — `audit_pyside` Onda 0 (Sprint Higiene)

**Status:** Aprovado pelo owner em 2026-05-05 (D8, D9, D10, D11 resolvidos).
**Branch:** `chore/cleanup-root`
**Janela:** 5 dias úteis, paralelo à Onda 1 do plano-mestre.
**Plano-mestre:** [Plano Performance-First — audit_pyside](https://www.notion.so/4870a1ffc6de4bfda0b88164b2dad759) §6 → Onda 0.

> **Cada seção é um commit atômico.** Execute, valide com `git status`, faça commit e siga. Isso facilita reverter qualquer etapa sem perder progresso.

---

## Pré-requisitos

```bash
cd ~/path/to/audit_pyside
git checkout main && git pull
git checkout -b chore/cleanup-root
git tag pre-cleanup-$(date +%Y%m%d)   # snapshot de segurança
```

---

## 1. Estender `.gitignore` (commit 1)

```bash
cat >> .gitignore <<'EOF'

# === Onda 0 — chore/cleanup-root (2026-05-05) ===
# Backups de stash
.stash_backups/

# Diretórios de trabalho temporário
tmp/
scratch/
playground/
validacao_tmp/
pr100_fixes/
artifacts/

# Arquivos de descrição de PR (devem ir no body do PR, não no repo)
pr_body.md
pr_desc.md
pr_desc.txt
pr_description.md
pr_description.txt

# Scripts/arquivos temporários
tmp_*.py
traceback.txt

# Patches soltos (versionar em PR, não como arquivo)
*.diff
EOF

git add .gitignore
git commit -m "chore(onda-0): estender .gitignore para artefatos temporários"
```

---

## 2. Mover testes da raiz para `tests/` (commit 2)

```bash
mkdir -p tests/perf tests/polars

git mv test_perf.py  tests/perf/test_perf_01.py
git mv test_perf2.py tests/perf/test_perf_02.py
git mv test_perf3.py tests/perf/test_perf_03.py

git mv test_polars.py  tests/polars/test_polars_01.py
git mv test_polars2.py tests/polars/test_polars_02.py
git mv test_polars3.py tests/polars/test_polars_03.py
git mv test_polars4.py tests/polars/test_polars_04.py

git mv test_produtos_agrupados_filter.py tests/test_produtos_agrupados_filter.py

git commit -m "chore(onda-0): mover test_*.py da raiz para tests/"
```

> Se algum desses for **benchmark** (não teste funcional), mova-o para `bench/` em vez de `tests/perf/` e marque com `@pytest.mark.bench`.

---

## 3. Mover scripts utilitários para `scripts/` (commit 3)

```bash
mkdir -p scripts/excel scripts/patches scripts/exploratorio

git mv read_excel.py            scripts/excel/read_excel.py
git mv update_excel.py          scripts/excel/update_excel.py
git mv convert_map.py           scripts/excel/convert_map.py

git mv patch_agrupados.py       scripts/patches/patch_agrupados.py
git mv patch_produtos_final.py  scripts/patches/patch_produtos_final.py

git mv analise_estoque_2021.py  scripts/exploratorio/analise_estoque_2021.py
git mv ai_studio_code.py        scripts/exploratorio/ai_studio_code.py

git commit -m "chore(onda-0): mover utilitários da raiz para scripts/"
```

---

## 4. Remover lixo óbvio (commit 4)

```bash
git rm tmp_import_main_window.py
git rm tmp_import_test.py
git rm traceback.txt

# JSONs/TXTs duplicados — manter apenas o JSON (mais estruturado)
git rm runlist.txt
git rm sitafe_cols.txt

git commit -m "chore(onda-0): remover arquivos temporários e duplicações txt/json"
```

---

## 5. Remover patches soltos (commit 5)

> **Validação obrigatória antes:** confirme que cada patch já foi aplicado ao código.
> ```bash
> git apply --check manual_grouping_periods_patch.clean2.diff
> ```
> Se algum não foi aplicado, aplique antes de deletar.

```bash
git rm manual_grouping_periods_patch.diff
git rm manual_grouping_periods_patch.clean.diff
git rm manual_grouping_periods_patch.clean2.diff

git rm "patch_consistencia_canonica_qconv (1).diff"
git rm patch_consistencia_canonica_qconv.clean.diff
git rm patch_consistencia_canonica_qconv.clean2.diff

git rm patch_semantica_qconv_fisica.diff
git rm patch_semantica_qconv_fisica.clean.diff
git rm patch_semantica_qconv_fisica.clean2.diff

git commit -m "chore(onda-0): remover diffs/patches soltos da raiz"
```

---

## 6. Remover descrições de PR commitadas (commit 6)

```bash
git rm pr_body.md pr_desc.md pr_desc.txt pr_description.md pr_description.txt
git rm cleanup-removed-snapshots-example.json  # se não for fixture de teste

git commit -m "chore(onda-0): remover descrições de PR e exemplos do tracking"
```

---

## 7. Remover backups de stash versionados (commit 7)

```bash
# Confirme antes que não há nada importante lá
ls -la .stash_backups/autostash-before-sync/

git rm -r .stash_backups/
git commit -m "chore(onda-0): remover .stash_backups/ (já coberto pelo .gitignore)"
```

---

## 8. Consolidar planos em `docs/` (commit 8)

```bash
mkdir -p docs/historico

# O plano vivo (escolha o mais recente/completo)
git mv plano.md docs/roadmap.md

# Os outros viram histórico arquivado
git mv plan.md                 docs/historico/plan-legacy.md
git mv plano_otimizacao_q.md   docs/historico/plano-otimizacao-q.md
git mv PLANO_CLAUDE_CODE.md    docs/historico/plano-claude-code.md
git mv PROMPT_INICIAL.md       docs/historico/prompt-inicial.md

git commit -m "docs(onda-0): consolidar planos em docs/roadmap.md e arquivar histórico"
```

> Após esse commit, edite `docs/roadmap.md` e adicione no topo:
> ```markdown
> # Roadmap audit_pyside
> _Fonte única de verdade. Planos antigos em docs/historico/ são arquivo morto._
> _Plano-mestre vivo: Notion → 🎯 Plano de Execução Performance-First — audit_pyside_
> ```

---

## 9. Consolidar `bench/` e `benchmarks/` (commit 9) — D10

**Decisão D10:** manter `bench/` (já referenciado em `pyproject.toml` como coverage omit).

```bash
ls -la bench/ benchmarks/

# Mover conteúdo de benchmarks/ para bench/
git mv benchmarks/* bench/ 2>/dev/null || true
git rm -r benchmarks/ 2>/dev/null || true

git commit -m "chore(onda-0): consolidar benchmarks/ em bench/ (D10)"
```

---

## 10. Resolver `.Jules` vs `.jules` (commit 10) — R2

**Bloqueador real para clones em macOS/Windows.** Investigue primeiro:

```bash
diff -r .Jules .jules
```

Se forem idênticos ou quase, mantenha `.jules` (lowercase é convenção):

```bash
# Se .Jules tem algo único, mover primeiro:
# cp -rn .Jules/* .jules/

git rm -r .Jules
git commit -m "chore(onda-0): remover .Jules duplicado (manter .jules minúsculo)"
```

---

## 11. Deletar `app_safe.py` (commit 11) — D9

**Decisão D9:** Opção A — `app_safe.py` é entrypoint legado, será deletado.

```bash
# 1. Deletar o arquivo
git rm app_safe.py

# 2. Atualizar pyproject.toml — remover "app_safe.py" da lista include
```

Edite `pyproject.toml`:

```diff
 [tool.hatch.build.targets.wheel]
 packages = ["src"]
-include = ["app.py", "app_safe.py"]
+include = ["app.py"]
```

E também a seção `per-file-ignores` do ruff:

```diff
 [tool.ruff.lint.per-file-ignores]
 "conftest.py" = ["E402"]
 "app.py" = ["E402"]
-"app_safe.py" = ["E402"]
```

```bash
git add pyproject.toml
git commit -m "chore(onda-0): deletar app_safe.py legado (D9 opção A)

ADR-0003 a ser criado: app.py é entrypoint único. app_safe.py
era versão legada sem documentação da diferença. Decisão tomada em
2026-05-05 pelo owner; rastreado em snapshot Notion 358edc8b7d5d.
"
```

> **Criar ADR-0003** após o merge:
> ```bash
> mkdir -p docs/adr
> cat > docs/adr/0003-app-entrypoint-unico.md <<'EOF'
> # ADR-0003 — Entrypoint único app.py
>
> ## Status
> Accepted (2026-05-05)
>
> ## Context
> Existiam dois arquivos de entrada: app.py e app_safe.py. A diferença
> não era documentada, e ambos estavam no [tool.hatch.build.targets.wheel].include.
> Isso gerava confusão sobre qual era o entrypoint real e era bloqueador
> para release v1.0.0 (M6).
>
> ## Decision
> app.py é o **único** entrypoint. app_safe.py foi deletado por ser
> versão legada sem propósito documentado.
>
> ## Consequences
> - pyproject.toml simplificado.
> - Se um modo "safe/diagnóstico" for necessário no futuro, será via flag
>   no próprio app.py (ex.: app.py --mode=safe), não via arquivo separado.
> - Bundle PyInstaller (audit_pyside.spec) precisa apontar só para app.py.
> EOF
> git add docs/adr/0003-app-entrypoint-unico.md
> git commit -m "docs(adr): ADR-0003 entrypoint único app.py"
> ```

---

## 12. Validação final

```bash
# A raiz deve estar bem mais enxuta
ls -la | grep -v "^d" | wc -l   # esperado: ≤25 entradas

# Garantir que nada essencial quebrou
uv sync --group dev
uv run pytest -q -m "not oracle and not gui"
uv run ruff check src/transformacao/
uv run python -c "import app; print('entrypoint OK')"

# Confirmar que .gitignore está pegando o que deveria
git status --ignored | head -30

# Smoke do build wheel (opcional mas recomendado)
uv build --wheel
ls -la dist/
```

---

## 13. Push e PR

```bash
git push -u origin chore/cleanup-root
gh pr create --title "chore(onda-0): higiene estrutural da raiz (D8, D9, D10, R1, R2, R7)" \
             --label "onda-0,housekeeping" \
             --body-file PR_BODY.md
```

Sugestão para o body do PR (`PR_BODY.md`, **não** commitar — está coberto no `.gitignore`):

```markdown
# Onda 0 — Higiene Estrutural

Implementa decisões D8/D9/D10 do plano-mestre. Branch única com 11 commits atômicos para revisão progressiva.

## Decisões aplicadas
- **D8** (Onda 0 aprovada): branch `chore/cleanup-root`
- **D9** (`app_safe.py` deletado): opção A; ADR-0003 segue em commit separado
- **D10** (`bench/` canônico): conteúdo de `benchmarks/` migrado

## Antes / Depois (raiz)
- Antes: ~80 entradas
- Depois: ≤25 entradas

## Não-objetivos
- Sem refactor de `src/transformacao/`
- Sem mudança no pipeline fiscal
- Sem alteração das 5 invariantes (`id_agrupado`, `id_agregado`, `__qtd_decl_final_audit__`, `q_conv`, `q_conv_fisica`)

## Validação
- [x] `uv sync --group dev` ok
- [x] `uv run pytest -q -m "not oracle and not gui"` verde
- [x] `uv run ruff check src/transformacao/` verde
- [x] `python -c "import app"` ok
- [x] `uv build --wheel` ok

## Próximos
- ADR-0003 (entrypoint único) em PR separado após merge
- ADR-0004 (URL canônica) em PR separado
- Onda 1 (#248 baseline benchmark) destravada
```

---

## Resumo do impacto esperado

| Antes | Depois |
|---|---|
| ~80 entradas na raiz | ≤25 entradas |
| 7 `test_*.py` na raiz | 0 (todos em `tests/`) |
| 9 `.diff` versionados | 0 |
| 5 arquivos de plano | 1 (`docs/roadmap.md`) + histórico |
| 5 arquivos de PR | 0 |
| `.Jules` + `.jules` | só `.jules` (clone macOS funciona) |
| `bench/` + `benchmarks/` | só `bench/` |
| `.stash_backups/` versionado | gitignored |
| `app.py` + `app_safe.py` | só `app.py` (ADR-0003) |

Após mergear, a Onda 1 (#248 baseline benchmark) pode rodar em estrutura limpa.
