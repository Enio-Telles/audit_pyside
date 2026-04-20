# produtos_final

## Visao Geral

Tabela detalhada por descricao principal normalizada que consolida o resultado do grupo-base automatico e da agregacao manual vigente, elegendo descricoes padrao, unidades de referencia sugeridas e classificacoes fiscais consolidadas para cada `id_agrupado`.

Observacoes importantes:

- `produtos_agrupados_<cnpj>.parquet` e a tabela mestre do agrupamento vigente;
- `produtos_final_<cnpj>.parquet` e a visao detalhada por `id_descricao`, usada como camada canonica de apoio para vinculacao e enriquecimento;
- as listas usadas para decisao manual, como `lista_desc_compl`, `lista_ncm`, `lista_cest`, `lista_gtin`, `lista_co_sefin` e `lista_unidades`, vivem principalmente em `descricao_produtos_<cnpj>.parquet` e `produtos_agrupados_<cnpj>.parquet`;
- descricao complementar nao compoe a chave automatica do grupo-base.

## Funcao de Geracao

```python
def gerar_produtos_final(cnpj: str, pasta_cnpj: Path | None = None) -> bool
```

Modulo de entrada: `src/transformacao/produtos_final_v2.py`

Implementacao canonica: `src/transformacao/rastreabilidade_produtos/_produtos_final_impl.py`

Proxies legados compativeis:

- `src/transformacao/produtos_final.py`
- `src/transformacao/04_produtos_final.py`
- `src/transformacao/rastreabilidade_produtos/04_produtos_final.py`

## Dependencias

- **Depende de**: `descricao_produtos`, `item_unidades`
- **E dependencia de**: `fontes_produtos`, `fatores_conversao`

## Fontes de Entrada

- `descricao_produtos_<cnpj>.parquet`
- `item_unidades_<cnpj>.parquet`

Dependencia indireta relevante:

- `itens_<cnpj>.parquet` via `descricao_produtos`

## Objetivo

Materializar a camada canonica de produto por descricao principal normalizada, preservando:

- `id_descricao`
- `id_agrupado`
- `id_agrupado_base`
- atributos consolidados do grupo vigente
- trilha entre grupo-base automatico e agregacao manual

## Principais Colunas

| Coluna | Tipo | Descricao |
|--------|------|-----------|
| `id_descricao` | str | Chave da descricao principal normalizada na camada detalhada |
| `id_agrupado` | str | Chave vigente do agrupamento no pipeline |
| `id_agrupado_base` | str | Chave automatica original do grupo-base por descricao principal normalizada |
| `descricao_normalizada` | str | Descricao principal normalizada usada no grupo-base automatico |
| `descricao` | str | Descricao principal de origem da camada detalhada |
| `descricao_final` | str | Descricao final consolidada da linha |
| `descr_padrao` | str | Descricao padrao eleita para o grupo vigente |
| `ncm_final` | str | NCM consolidado da linha |
| `cest_final` | str | CEST consolidado da linha |
| `gtin_final` | str | GTIN consolidado da linha |
| `co_sefin_final` | str | Codigo SEFIN consolidado da linha |
| `co_sefin_padrao` | str | Codigo SEFIN padrao do grupo vigente |
| `unid_ref_sugerida` | str | Unidade de referencia sugerida |
| `criterio_agrupamento` | str | Criterio efetivo do agrupamento vigente |
| `origem_agrupamento` | str | Origem efetiva do agrupamento vigente |

`codigo_fonte` nao e coluna canonica de `produtos_final_<cnpj>.parquet`; ele permanece na tabela ponte `map_produto_agrupado_<cnpj>.parquet`.

## Regras de Processamento

### Formacao do grupo-base automatico

O fluxo vigente deve ser lido assim:

1. normaliza apenas a `descricao` principal;
2. a normalizacao faz: maiusculas, trim, remocao de acentos e colapso de espacos internos duplicados;
3. descricoes principais com o mesmo texto normalizado compartilham o mesmo `id_agrupado_base`;
4. descricao complementar nao entra nessa chave automatica.

### Agregacao manual

Qualquer agregacao alem da igualdade estrita da descricao principal normalizada e manual.

Na implementacao atual:

- `mapa_agrupamento_manual_<cnpj>.parquet` pode sobrescrever o `id_agrupado`;
- a trilha automatica continua preservada em `id_agrupado_base`;
- `id_descricao` e o melhor identificador para decisoes manuais pontuais;
- `descricao_normalizada` pode ser usada quando a decisao vale para todo o grupo-base.

### Eleicao de atributos

- `descr_padrao`: descricao priorizada no grupo vigente;
- `ncm_padrao`: NCM mais representativo do grupo;
- `cest_padrao`: CEST mais representativo do grupo;
- `co_sefin_padrao`: codigo SEFIN mais representativo do grupo;
- `unid_ref_sugerida`: unidade sugerida a partir das unidades consolidadas do grupo.

### Onde ficam as listas para auditoria manual

As listas abaixo nao pertencem ao contrato principal de `produtos_final`, mas sao o suporte correto para revisao manual:

- `lista_desc_compl`
- `lista_ncm`
- `lista_cest`
- `lista_gtin`
- `lista_co_sefin`
- `lista_unidades`
- `lista_codigos`
- `fontes`

Essas listas existem para apoiar a decisao manual e nao devem ser tratadas como parte da chave automatica.

## Golden Thread

Leitura operacional correta:

```text
linha original -> id_linha_origem -> codigo_fonte
-> map_produto_agrupado (chave_produto / descricao_normalizada)
-> id_descricao -> id_agrupado_base -> id_agrupado -> tabelas analiticas
```

`produtos_final` participa do fio de ouro como camada detalhada canonica do agrupamento, mas nao substitui a tabela ponte.

## Saida Gerada

```text
dados/CNPJ/<cnpj>/analises/produtos/produtos_final_<cnpj>.parquet
```

## Notas

- `id_agrupado` e a chave primaria usada nas tabelas analiticas subsequentes;
- a tabela ponte `map_produto_agrupado_<cnpj>.parquet` continua sendo o elo principal entre a origem e o grupo;
- a uniao automatica minima e por igualdade estrita da descricao principal normalizada;
- descricao complementar e demais atributos devem ser lidos como listas de apoio para revisao manual;
- a regra viva de agrupamento e consolidacao deve ser alterada apenas em `_produtos_final_impl.py`;
- os demais arquivos listados acima existem para compatibilidade de import e execucao legada.
