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

## Estrutura criada nesta etapa

Foram adicionados os SQLs atomizados abaixo:

```text
sql/arquivos_parquet/atomizadas/shared/01_reg0000_historico.sql
sql/arquivos_parquet/atomizadas/shared/02_reg0000_versionado.sql
sql/arquivos_parquet/atomizadas/shared/03_reg0000_ultimo_periodo.sql
sql/arquivos_parquet/atomizadas/c100/10_c100_raw.sql
sql/arquivos_parquet/atomizadas/c170/20_c170_raw.sql
sql/arquivos_parquet/atomizadas/c176/30_c176_raw.sql
sql/arquivos_parquet/atomizadas/bloco_h/40_h005_raw.sql
sql/arquivos_parquet/atomizadas/bloco_h/41_h010_raw.sql
sql/arquivos_parquet/atomizadas/bloco_h/42_h020_raw.sql
sql/arquivos_parquet/atomizadas/dimensions/50_reg0200_raw.sql
```

Essa camada cobre:

- `reg_0000` historico, versionado e ultimo arquivo por periodo;
- `c100` bruto;
- `c170` bruto;
- `c176` bruto;
- `bloco_h` separado em `H005`, `H010` e `H020`;
- `0200` bruto.

As SQLs legadas em `sql/*.sql` continuam preservadas para compatibilidade com o fluxo atual.

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

com leitores lazy para os parquets atomizados e recomposicoes tipadas de `0200`, `C100`, `C170`, `C176` e `Bloco H`:

- `carregar_c100_bruto`
- `carregar_c170_bruto`
- `carregar_c176_bruto`
- `carregar_h005_bruto`
- `carregar_h010_bruto`
- `carregar_h020_bruto`
- `carregar_reg0200_bruto`
- `construir_reg0200_tipado`
- `construir_c100_tipado`
- `construir_c170_tipado`
- `construir_c176_tipado`
- `construir_h005_tipado`
- `construir_h010_tipado`
- `construir_h020_tipado`
- `construir_bloco_h_tipado`
- `salvar_c100_tipado`
- `materializar_camadas_atomizadas`

Essa camada segue a estrategia:

1. extrair SQL minima e tipagem bruta;
2. persistir em Parquet;
3. recompor e enriquecer fora do banco com `polars.LazyFrame`.

Na etapa atual, a materializacao principal gera:

- `reg0200_tipado_<cnpj>.parquet`
- `c100_tipado_<cnpj>.parquet`
- `c170_tipado_<cnpj>.parquet`
- `c176_tipado_<cnpj>.parquet`
- `h005_tipado_<cnpj>.parquet`
- `h010_tipado_<cnpj>.parquet`
- `h020_tipado_<cnpj>.parquet`
- `bloco_h_tipado_<cnpj>.parquet`

## Compatibilidade

- O pipeline analitico atual continua consumindo os parquets tradicionais.
- A camada atomizada foi registrada como etapa principal opcional do pipeline (`efd_atomizacao`) no orquestrador e na UI.
- No frontend React, a etapa aparece como a acao dedicada `EFD Atomizada` no painel lateral.
- As etapas legadas ainda nao dependem obrigatoriamente dela, porque o consumo funcional continua nos parquets tradicionais.
- A mudanca preserva a regra de negocio existente; o foco desta etapa e eficiencia de extracao e preparacao da arquitetura atomizada.

## Pontos de atencao

- Consultas sem bind `:CNPJ` continuam sendo ignoradas para evitar extracoes massivas e acidentais.
- Como a UI lista SQLs recursivamente, consultas atomizadas futuras passam a aparecer para selecao.
- A adicao das SQLs atomizadas de negocio foi deixada fora deste ciclo para evitar ampliar o escopo funcional sem validacao fiscal dedicada.
