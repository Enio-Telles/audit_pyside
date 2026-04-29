# Agregacao de produtos: regra consolidada e estado real
<a id="mds-02-agregacao-produtos"></a>

Este documento consolida a regra metodologica desejada com o estado real do runtime no `audit_pyside`.

Sempre que houver conflito, separar explicitamente:

- regra desejada simples;
- comportamento implementado hoje;
- gap remanescente.

## Objetivo

Formar grupos-base auditaveis por descricao principal e apoiar agregacao manual posterior, preservando rastreabilidade entre:

- linha original;
- descricao principal normalizada;
- descricoes complementares listadas;
- demais caracteristicas listadas;
- artefatos finais consumidos pelo pipeline.

## Regra metodologica consolidada

### 1. Pre-agrupamento automatico minimo

A unica uniao automatica permitida e por igualdade estrita da descricao principal apos normalizacao.

Em termos praticos:

- a chave automatica nasce somente da coluna `descricao`;
- duas linhas entram no mesmo grupo-base apenas se a `descricao` principal normalizada for igual;
- isso deve ser entendido como pre-agrupamento, nao como equivalencia semantica final.

### 2. Normalizacao da descricao principal

A normalizacao desejada e exatamente esta:

1. converter para maiusculas;
2. remover espacos excedentes do inicio e do fim;
3. retirar acentos;
4. colapsar espacos internos duplicados para um unico espaco.

### 3. O que nao entra na chave automatica

Os campos abaixo nao devem compor a chave automatica do grupo-base:

- descricao complementar;
- NCM;
- CEST;
- GTIN;
- CO_SEFIN;
- unidade;
- codigo de origem;
- qualquer outra caracteristica fiscal ou comercial auxiliar.

### 4. O que deve ser listado para decisao manual

Para cada grupo-base, o sistema deve expor explicitamente:

- `lista_desc_compl`;
- `lista_ncm`;
- `lista_cest`;
- `lista_gtin`;
- `lista_co_sefin`;
- `lista_unid`;
- `lista_codigos`;
- `fontes`;
- `lista_codigo_fonte`, quando existir.

Essas listas existem para auditoria e para decisao manual. Elas nao autorizam uniao automatica.

### 5. Agregacao final

Qualquer agregacao alem da igualdade estrita da descricao principal normalizada e manual.

Isso inclui:

- unir grupos-base diferentes;
- manter grupos-base separados mesmo quando parecem semelhantes;
- desagregar unioes anteriores;
- decidir o `id_agrupado` final quando houver criterio de negocio.

## Nomes reais hoje

| Conceito | Nome real hoje | Como interpretar |
| --- | --- | --- |
| grupo-base / agrupamento vigente no pipeline | `id_agrupado` | chave ativa do pipeline |
| alias de apresentacao nas abas finais | `id_agregado` | apenas apresentacao |
| chave automatica da descricao principal normalizada | `id_agrupado_base` | grupo-base automatico |
| chave da descricao listada | `id_descricao` / `chave_produto` | bucket detalhado por descricao principal normalizada |
| chave vinda da origem | `codigo_fonte` | fio de ouro preferencial quando existe |

`id_produto_agrupado` e `id_produto_origem` continuam uteis como nomes conceituais, mas nao sao as colunas canonicas do runtime atual.

## Cadeia de rastreabilidade correta

A cadeia mais fiel ao codigo atual, com a regra consolidada acima, e:

```text
linha SQL -> id_linha_origem -> codigo_fonte -> descricao principal normalizada
-> id_descricao -> id_agrupado_base -> id_agrupado -> id_agregado
```

Observacoes importantes:

- `codigo_fonte` tem prioridade no vinculo das fontes quando existe;
- `descricao_normalizada` e a normalizacao da descricao principal;
- `descricao complementar` nao participa da chave automatica;
- `id_agregado` aparece nas abas finais apenas como alias de apresentacao.

## O que o runtime atual ja faz

### `03_descricao_produtos.py`

O runtime ja implementa a parte central da regra simples:

- normaliza apenas a `descricao` principal em `descricao_normalizada`;
- agrupa `item_unidades` por `descricao_normalizada`;
- preserva `lista_desc_compl`;
- preserva `lista_ncm`, `lista_cest`, `lista_gtin`, `lista_co_sefin`, `lista_unid`, `lista_codigos`, `fontes` e `lista_codigo_fonte`.

Ou seja: a descricao complementar ja entra como lista de apoio, nao como chave.

### `_produtos_final_impl.py`

Na camada final de agrupamento:

1. `id_agrupado_base` e gerado deterministicamente a partir de `descricao_normalizada`;
2. `id_agrupado` nasce igual ao grupo-base;
3. `mapa_agrupamento_manual_<cnpj>.parquet` pode redefinir o `id_agrupado` final.

Interpretacao correta:

- a automacao atual forma grupos-base por descricao principal normalizada;
- a agregacao final entre grupos-base deve ser tratada como manual.

## Como interpretar `id_descricao`, `id_agrupado_base` e `id_agrupado`

- `id_descricao`: identifica a descricao principal normalizada listada na camada detalhada;
- `id_agrupado_base`: identifica o grupo-base automatico minimo;
- `id_agrupado`: identifica o agrupamento vigente do pipeline;
- sem decisao manual, `id_agrupado` coincide com `id_agrupado_base`;
- com decisao manual, `id_agrupado` passa a refletir o agrupamento final escolhido.

## Agrupamento manual

O arquivo `mapa_agrupamento_manual_<cnpj>.parquet` aceita hoje:

- `id_descricao` + `id_agrupado`;
- `descricao_normalizada` + `id_agrupado`;
- ou ambos.

Precedencia real:

1. manual por `id_descricao`;
2. manual por `descricao_normalizada`;
3. grupo-base automatico por `id_agrupado_base`.

Leitura recomendada:

- `id_descricao` e a forma mais precisa de decisao manual;
- `descricao_normalizada` e aceitavel quando a decisao vale para todo o grupo-base;
- a etapa automatica nao deve ser descrita como agregacao semantica final.

## Artefatos principais e papel de cada um

### `descricao_produtos_<cnpj>.parquet`

Camada detalhada por descricao principal normalizada, onde ficam:

- `descricao_normalizada`;
- `descricao`;
- `lista_desc_compl`;
- `lista_ncm`;
- `lista_cest`;
- `lista_gtin`;
- `lista_co_sefin`;
- `lista_unid`;
- `lista_codigos`;
- `lista_codigo_fonte`;
- `fontes`.

### `produtos_agrupados_<cnpj>.parquet`

Tabela mestre do agrupamento vigente, com destaque para:

- `id_agrupado`;
- `descr_padrao`;
- `lista_descricoes`;
- `lista_desc_compl`;
- `lista_ncm`;
- `lista_cest`;
- `lista_gtin`;
- `lista_co_sefin`;
- `lista_unidades`;
- `fontes`;
- `ids_origem_agrupamento`;
- `criterio_agrupamento`;
- `origem_agrupamento`;
- `qtd_descricoes_grupo`;
- `versao_agrupamento`.

### `produtos_final_<cnpj>.parquet`

Tabela detalhada por descricao, com destaque para:

- `id_descricao`;
- `id_agrupado`;
- `id_agrupado_base`;
- `descricao_normalizada`;
- `descricao_final`;
- `descr_padrao`;
- `ncm_final`;
- `cest_final`;
- `gtin_final`;
- `co_sefin_final`;
- `unid_ref_sugerida`.

### `map_produto_agrupado_<cnpj>.parquet`

Tabela ponte de vinculo, hoje com:

- `chave_produto`;
- `id_agrupado`;
- `codigo_fonte`;
- `descricao_normalizada`.

## Regra de auditoria

Casos ambiguos nao devem ser resolvidos silenciosamente.

Na implementacao atual:

- o mapa manual sem match gera auditoria dedicada;
- colisoes de descricao ou codigo de fonte sao tratadas com logs e artefatos de auditoria;
- `codigo_fonte` continua sendo o fio de ouro preferencial quando disponivel.

## Regra pratica de documentacao

Ao descrever esta metodologia, use:

- `id_agrupado` como chave real do pipeline;
- `id_agregado` apenas para apresentacao;
- `descricao_normalizada` como normalizacao da descricao principal;
- `id_agrupado_base` como grupo-base automatico minimo;
- `lista_desc_compl` e demais listas como insumo de decisao manual;
- `agregacao manual` como a etapa que decide unioes alem da igualdade estrita da descricao principal normalizada.
