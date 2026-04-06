# Extracao Atomizada da EFD

## Objetivo

Implementar uma extracao Oracle mais eficiente e compativel com a abordagem de atomizacao usada no projeto de referencia `audit_react_atomizacao_v2`, preservando a estrutura atual do pipeline e a rastreabilidade por CNPJ.

## O que mudou

- A extracao Oracle passou a gravar os resultados em lotes diretamente no Parquet, sem acumular toda a consulta em memoria com `fetchall()`.
- O descobrimento de SQLs agora e recursivo, o que permite usar estruturas como `sql/arquivos_parquet/atomizadas/...`.
- Quando houver a mesma consulta em diretorios SQL locais e legados, a versao do diretorio priorizado e mantida e a duplicata nao volta para a UI.
- O orquestrador principal passou a respeitar `consultas_selecionadas` ao chamar a extracao.
- A UI passou a reutilizar o mesmo nucleo de extracao do modo CLI, reduzindo duplicacao de logica.

## Estrutura suportada

Consultas em:

```text
sql/**/*.sql
```

geram arquivos em:

```text
dados/CNPJ/<cnpj>/arquivos_parquet/<subpastas_relativas>/<consulta>_<cnpj>.parquet
```

Exemplo:

```text
sql/arquivos_parquet/atomizadas/c100/10_c100_raw.sql
-> dados/CNPJ/<cnpj>/arquivos_parquet/atomizadas/c100/10_c100_raw_<cnpj>.parquet
```

## Regras de eficiencia adotadas

- `cursor.fetchmany()` com lotes de `50_000` linhas.
- `arraysize` e `prefetchrows` alinhados com o tamanho do lote.
- Escrita incremental com `pyarrow.parquet.ParquetWriter`.
- Materializacao em memoria restrita ao lote corrente.
- Manutencao da hierarquia relativa da SQL na saida para facilitar rastreabilidade.

## Camada de recomposicao lazy

Foi adicionada a pasta:

```text
src/transformacao/atomizacao_pkg/
```

com leitores lazy para os parquets atomizados e uma primeira recomposicao do `C100`:

- `carregar_c100_bruto`
- `carregar_c170_bruto`
- `construir_c100_tipado`
- `salvar_c100_tipado`

Essa camada segue a estrategia:

1. extrair SQL minima e tipagem bruta;
2. persistir em Parquet;
3. recompor e enriquecer fora do banco com `polars.LazyFrame`.

## Compatibilidade

- O pipeline analitico atual continua consumindo os parquets tradicionais.
- A camada atomizada ainda e incremental e nao foi acoplada como dependencia obrigatoria do pipeline principal.
- A mudanca preserva a regra de negocio existente; o foco desta etapa e eficiencia de extracao e preparacao da arquitetura atomizada.

## Pontos de atencao

- Consultas sem bind `:CNPJ` continuam sendo ignoradas para evitar extracoes massivas e acidentais.
- Como a UI lista SQLs recursivamente, consultas atomizadas futuras passam a aparecer para selecao.
- A adicao das SQLs atomizadas de negocio foi deixada fora deste ciclo para evitar ampliar o escopo funcional sem validacao fiscal dedicada.
