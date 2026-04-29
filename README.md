# Setup Codex — audit_pyside (PySide only)

Este pacote adapta o repositório `audit_pyside` para o formato de instruções do **Codex**.

## Arquivos
- `AGENTS.md`
- `src/transformacao/AGENTS.md`
- `src/interface_grafica/AGENTS.md`
- `tests/AGENTS.md`
- `docs/AGENTS.md`
- `docs/codex_usage.md`

## Estratégia
Em vez de `copilot-instructions.md`, o Codex usa `AGENTS.md` por escopo de pasta.

## Como aplicar
1. copie os arquivos para a raiz do repositório
2. commit e push
3. abra o projeto no Codex
4. peça tarefas específicas por área do código

## Instalação

### Com uv (recomendado)
```bash
uv sync                   # instala dependências runtime
uv sync --group dev       # instala dependências de desenvolvimento
uv run pytest -q          # roda testes
uv run python app.py      # lança a aplicação
```

### Com pip (legacy)
```bash
pip install -e ".[dev]"
```

## Scripts

Há utilitários em `scripts/` para gerar documentação e artefatos de amostra. Exemplos de uso (execute a partir da raiz do repositório):

- Gerar índice de Parquet (dry-run, imprime em stdout):

```bash
python scripts/generate_parquet_references.py --root . --out-dir docs/referencias --max-rows 3 --dry-run
```

- Gerar amostras de saída para um CNPJ (gera arquivos em `docs/referencias/samples`):

```bash
python scripts/generate_output_samples.py --cnpj 84654326000394 --base-dir dados/CNPJ --out-dir docs/referencias/samples --max-rows 3
```

- Gerar fatores de conversão a partir de `produtos_final_<cnpj>.parquet`:

```bash
python scripts/gen_fatores_84654326000394.py --prod-file dados/CNPJ/84654326000394/analises/produtos/produtos_final_84654326000394.parquet --out-file dados/CNPJ/84654326000394/analises/fatores_conversao_84654326000394.parquet
```

Notas:
- Os scripts usam `polars` para leitura/escrita de Parquet — instale com `pip install polars` se necessário.
- Prefira passar caminhos explícitos (`--prod-file`, `--out-dir`) em vez de editar código com CNPJs hardcoded.
- Para geração de arquivos grandes (índice Parquet), use `--dry-run` primeiro para validar comportamento.

Para detalhes e opções avançadas, veja `docs/scripts_usage.md`.
