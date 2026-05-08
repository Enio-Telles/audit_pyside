# 04 — Governança, Qualidade e Invariantes

## Invariantes Fiscais (Invioláveis)
Estas colunas nunca devem ser modificadas, renomeadas ou descartadas pelo DuckDB ou qualquer refatoração de performance:

| Coluna | Significado |
|---|---|
| `id_agrupado` | Chave de agrupamento de produtos |
| `id_agregado` | Chave de agregação |
| `__qtd_decl_final_audit__` | Quantidade declarada final de auditoria |
| `q_conv` | Quantidade convertida |
| `q_conv_fisica` | Quantidade convertida física |

## Regras Invioláveis
1. **Não substituir Polars no núcleo fiscal.**
2. **Preservar as 5 invariantes acima.**
3. **Não materializar DataFrames gigantes na GUI.**
4. **Não compartilhar conexões DuckDB entre threads.**
5. **Usar sempre parâmetros em queries SQL (evitar injeção).**

## Fluxo Obrigatório por PR
1. Criar branch a partir da `main`.
2. Implementar escopo atômico da fase.
3. Rodar `uv run pytest -q -m "not oracle and not gui_smoke"`.
4. Validar com `ruff check` e `mypy`.
5. Abrir PR Draft com métricas de performance (se aplicável).
6. **Differential Harness:** Obrigatório se tocar em qualquer lógica fiscal em `src/transformacao/`.
