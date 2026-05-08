# Batch rewrite Parquets v2

Este runbook operacionaliza o rewrite side-by-side de Parquets v1 para v2 usando
`src.io.categorical_writer.batch_rewrite_parquets`.

Ele nao troca caminhos de producao, nao altera consumidores e nao toca
`src/transformacao/**`.

## Planejar sem escrever

```powershell
uv run python scripts/batch_rewrite_parquets_v2.py `
  --input-root C:\dados\CNPJ\00000000000000\arquivos_parquet `
  --output-root C:\dados_v2\CNPJ\00000000000000\arquivos_parquet `
  --dry-run `
  --report-json tmp\batch-rewrite-plan.json `
  --report-md tmp\batch-rewrite-plan.md
```

## Executar side-by-side

```powershell
uv run python scripts/batch_rewrite_parquets_v2.py `
  --input-root C:\dados\CNPJ\00000000000000\arquivos_parquet `
  --output-root C:\dados_v2\CNPJ\00000000000000\arquivos_parquet `
  --report-json tmp\batch-rewrite-result.json `
  --report-md tmp\batch-rewrite-result.md
```

## Guard rails

- `--output-root` deve ser diferente de `--input-root`.
- Use `--dry-run` antes de qualquer escrita.
- A saida v2 deve ficar em raiz separada.
- O rollback e remover a raiz v2 gerada.
- Antes de promover qualquer consumo como padrao, rodar diff harness e bench
  sobre os Parquets reais.
