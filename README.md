# Fiscal Parquet Analyzer

Ferramenta de extraÃ§Ã£o, transformaÃ§Ã£o e auditoria de dados fiscais com persistÃªncia em Parquet, pipeline modular em `src/transformacao/` e interface grÃ¡fica em PySide6.

## Objetivo

O projeto organiza o fluxo fiscal em trÃªs camadas:

- extraÃ§Ã£o Oracle para Parquet por CNPJ;
- transformaÃ§Ã£o analÃ­tica com foco em rastreabilidade e auditoria;
- consulta e operaÃ§Ã£o do pipeline pela interface grÃ¡fica.

O princÃ­pio central Ã© preservar a linha original do documento fiscal e permitir que qualquer total analÃ­tico seja auditado de volta Ã  origem.

## Pipeline oficial

A ordem ativa do pipeline estÃ¡ em `src/orquestrador_pipeline.py`:

1. `tb_documentos`
2. `item_unidades`
3. `itens`
4. `descricao_produtos`
5. `produtos_final`
6. `fontes_produtos`
7. `fatores_conversao`
8. `c170_xml`
9. `c176_xml`
10. `movimentacao_estoque`
11. `calculos_mensais`
12. `calculos_anuais`

Os wrappers em `src/transformacao/` existem em boa parte para compatibilidade. Ao corrigir ou evoluir regras, a implementaÃ§Ã£o real costuma estar nos subpacotes `*_pkg`.

## ExecuÃ§Ã£o rÃ¡pida

InstalaÃ§Ã£o mÃ­nima:

```bash
pip install polars PySide6 openpyxl python-docx python-dotenv rich oracledb
```

Abrir a aplicaÃ§Ã£o:

```bash
python app.py
```

Rodar a suÃ­te de testes:

```bash
python -m pytest
```

Rodar testes direcionados:

```bash
python -m pytest tests/test_movimentacao_estoque.py
python -m pytest tests/test_calculos_mensais.py
python -m pytest tests/test_calculos_anuais.py
```

## DocumentaÃ§Ã£o oficial

Os documentos ativos do projeto ficam na raiz de `docs/`:

- [MovimentaÃ§Ã£o de Estoque](docs/mov_estoque.md)
- [Tabela Mensal](docs/tabela_mensal.md)
- [Tabela Anual](docs/tabela_anual.md)
- [ConversÃ£o de Unidades](docs/conversao_unidades.md)
- [AgregaÃ§Ã£o de Produtos](docs/agregacao_produtos.md)

## ConvenÃ§Ãµes importantes

- `id_agrupado` Ã© a chave mestra de produto no pipeline.
- `id_agregado` aparece em algumas saÃ­das analÃ­ticas como alias de apresentaÃ§Ã£o de `id_agrupado`.
- `__qtd_decl_final_audit__` guarda a quantidade declarada no estoque final para auditoria, sem alterar o saldo fÃ­sico.
- ajustes manuais de conversÃ£o e agrupamento devem ser preservados em reprocessamentos.

## DocumentaÃ§Ã£o histÃ³rica

Materiais antigos, planos intermediÃ¡rios, diagnÃ³sticos e anexos foram movidos para `docs/archive/`. Eles permanecem como histÃ³rico e apoio, mas a referÃªncia operacional atual Ã© somente a documentaÃ§Ã£o oficial listada acima.


## CatÃ¡logo SQL canÃ´nico

Todas as consultas ativas do sistema ficam exclusivamente dentro de `sql/`, organizada por domÃ­nio:

```text
sql/
  fiscal/
    efd/
    documentos/
    fronteira/
    validacao/
  fisconforme/
    cadastro/
    malhas/
  apoio/
    dicionarios/
    verificacoes/
  archive/
```

Regras operacionais:

- `sql/` Ã© a Ãºnica fonte de verdade das consultas usadas pelo backend, frontend e desktop.
- seleÃ§Ãµes persistidas usam IDs relativos ao catÃ¡logo, como `fiscal/efd/c170.sql`.
- `sql/archive/` preserva material histÃ³rico, mas nÃ£o entra na descoberta automÃ¡tica do pipeline.
