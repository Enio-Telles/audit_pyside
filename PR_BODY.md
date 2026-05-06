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
