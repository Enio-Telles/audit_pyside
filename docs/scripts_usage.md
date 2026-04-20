## Uso dos scripts de geração

Este documento resume opções e exemplos de execução para os utilitários de geração presentes em `scripts/`.

Preâmbulo
- Execute sempre a partir da raiz do repositório.
- Os scripts utilizam `polars` para leitura/escrita de Parquet.

1) `generate_parquet_references.py`

- Objetivo: gerar um índice Markdown com metadados e amostras (até N linhas) dos arquivos `.parquet` no repositório.

Exemplo (dry-run — imprime o índice sem gravar):

```bash
python scripts/generate_parquet_references.py --root . --out-dir docs/referencias --max-rows 3 --dry-run
```

Opções relevantes:
- `--root`: diretório a partir do qual procurar arquivos Parquet (default: `.`)
- `--out-dir`: diretório de saída (default: `docs/referencias`)
- `--max-rows`: número máximo de linhas a incluir nas amostras
- `--dry-run`: imprime o resultado em stdout em vez de gravar o arquivo

2) `generate_output_samples.py`

- Objetivo: gerar pequenas amostras Markdown dos arquivos de saída gerados para um `CNPJ` (mov_estoque, aba_periodos, aba_mensal, aba_anual).

Exemplo:

```bash
python scripts/generate_output_samples.py --cnpj 84654326000394 --base-dir dados/CNPJ --out-dir docs/referencias/samples --max-rows 3
```

Opções relevantes:
- `--cnpj`: CNPJ alvo (obrigatório)
- `--base-dir`: raiz dos diretórios por CNPJ (default: `dados/CNPJ`)
- `--out-dir`: diretório de saída para os MDs (default: `docs/referencias/samples`)
- `--max-rows`: número máximo de linhas na amostra (default: 3)

3) `gen_fatores_84654326000394.py`

- Objetivo: gerar um Parquet com fatores de conversão a partir do `produtos_final_<cnpj>.parquet`.

Exemplo (usar caminho explícito do arquivo de produtos):

```bash
python scripts/gen_fatores_84654326000394.py --prod-file dados/CNPJ/84654326000394/analises/produtos/produtos_final_84654326000394.parquet --out-file dados/CNPJ/84654326000394/analises/fatores_conversao_84654326000394.parquet
```

Opções relevantes:
- `--cnpj`: localiza automaticamente `produtos_final_<cnpj>.parquet` quando fornecido
- `--prod-file`: caminho explícito para o `produtos_final` (recomendado)
- `--out-file`: caminho de saída para o parquet de fatores

Boas práticas
- Não versionar arquivos gerados (ex.: índice Parquet) — prefira gerar em CI ou em um passo de documentação.
- Use `--dry-run` para validar antes de gravar arquivos potencialmente grandes.
- Evite CNPJs hardcoded: prefira `--cnpj` ou `--prod-file`.

Se quiser, posso também adicionar exemplos ao `docs/` com links diretos e um pequeno workflow de CI que gera (mas não commita) esses artefatos durante o build de documentação.
