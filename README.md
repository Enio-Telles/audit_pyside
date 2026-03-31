# Fiscal Parquet Analyzer

Ferramenta de extração, transformação e auditoria de dados fiscais com persistência em Parquet, pipeline modular em `src/transformacao/` e interface gráfica em PySide6.

## Objetivo

O projeto organiza o fluxo fiscal em três camadas:

- extração Oracle para Parquet por CNPJ;
- transformação analítica com foco em rastreabilidade e auditoria;
- consulta e operação do pipeline pela interface gráfica.

O princípio central é preservar a linha original do documento fiscal e permitir que qualquer total analítico seja auditado de volta à origem.

## Pipeline oficial

A ordem ativa do pipeline está em `src/orquestrador_pipeline.py`:

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

Os wrappers em `src/transformacao/` existem em boa parte para compatibilidade. Ao corrigir ou evoluir regras, a implementação real costuma estar nos subpacotes `*_pkg`.

## Execução rápida

Instalação mínima:

```bash
pip install polars PySide6 openpyxl python-docx python-dotenv rich oracledb
```

Abrir a aplicação:

```bash
python app.py
```

Rodar a suíte de testes:

```bash
python -m pytest
```

Rodar testes direcionados:

```bash
python -m pytest tests/test_movimentacao_estoque.py
python -m pytest tests/test_calculos_mensais.py
python -m pytest tests/test_calculos_anuais.py
```

## Documentação oficial

Os documentos ativos do projeto ficam na raiz de `docs/`:

- [Movimentação de Estoque](docs/mov_estoque.md)
- [Tabela Mensal](docs/tabela_mensal.md)
- [Tabela Anual](docs/tabela_anual.md)
- [Conversão de Unidades](docs/conversao_unidades.md)
- [Agregação de Produtos](docs/agregacao_produtos.md)

## Convenções importantes

- `id_agrupado` é a chave mestra de produto no pipeline.
- `id_agregado` aparece em algumas saídas analíticas como alias de apresentação de `id_agrupado`.
- `__qtd_decl_final_audit__` guarda a quantidade declarada no estoque final para auditoria, sem alterar o saldo físico.
- ajustes manuais de conversão e agrupamento devem ser preservados em reprocessamentos.

## Documentação histórica

Materiais antigos, planos intermediários, diagnósticos e anexos foram movidos para `docs/archive/`. Eles permanecem como histórico e apoio, mas a referência operacional atual é somente a documentação oficial listada acima.
