# Metodologia MDS - estado atual e consolidacao

## Objetivo

Consolidar a metodologia MDS de forma coerente com o runtime atual do `audit_pyside`, sem confundir:

- conceito metodologico;
- contrato real materializado em Parquet;
- nomenclatura alvo ainda nao migrada;
- comportamento efetivamente exercido pelos modulos de pipeline;
- regra desejada simples de agregacao versus o que o codigo ja materializa hoje.

## Escopo confirmado no codigo

A metodologia nao esta apenas "planejada". Ha implementacao ativa em:

- `src/metodologia_mds/service.py`
- `src/metodologia_mds/orchestrator.py`
- `src/transformacao/rastreabilidade_produtos/03_descricao_produtos.py`
- `src/transformacao/rastreabilidade_produtos/_produtos_final_impl.py`
- `src/transformacao/rastreabilidade_produtos/fatores_conversao.py`
- `src/transformacao/movimentacao_estoque_pkg/movimentacao_estoque.py`
- `src/transformacao/calculos_periodo_pkg/calculos_periodo.py`
- `src/transformacao/calculos_mensais_pkg/calculos_mensais.py`
- `src/transformacao/calculos_anuais_pkg/calculos_anuais.py`

Tambem ha cobertura focal em:

- `tests/test_metodologia_mds.py`
- `tests/test_metodologia_mds_conversion.py`
- `tests/test_metodologia_mds_rules.py`
- `tests/test_q_conv_semantica_estoque.py`
- `tests/test_calculos_mensais.py`
- `tests/test_calculos_anuais.py`
- `tests/test_agregacao_produtos.py`
- `tests/test_agrupamento_manual.py`

## Correcoes aplicadas nesta revisao

### 1. Contrato real vs nomenclatura conceitual

Os documentos anteriores tratavam nomes conceituais como se ja fossem o contrato canonico do runtime. Isso nao e verdade hoje.

Mapa correto:

| Conceito metodologico | Coluna / saida real hoje |
| --- | --- |
| `id_produto_agrupado` | `id_agrupado` no pipeline e `id_agregado` nas abas finais |
| `id_produto_origem` | `codigo_fonte` e `chave_produto` / `id_descricao`, conforme a camada |
| `quantidade_convertida` | `q_conv` |
| `quantidade_fisica` | `q_conv_fisica` |
| `quantidade_fisica_sinalizada` | `__q_conv_sinal__` |
| `estoque_final_declarado` | `__qtd_decl_final_audit__` |
| `unidade_referencia` | `unid_ref` |
| `tabela_periodos` | `aba_periodos_<cnpj>.parquet` |
| `tabela_mensal` | `aba_mensal_<cnpj>.parquet` |
| `tabela_anual` | `aba_anual_<cnpj>.parquet` |

### 2. Pre-agrupamento automatico e agregacao manual

A regra consolidada mais simples passa a ser esta:

1. unir automaticamente apenas descricoes principais iguais apos normalizacao;
2. a normalizacao da descricao principal e: maiusculas, trim nas pontas, remocao de acentos e colapso de espacos internos duplicados;
3. `descricao complementar` nao entra na chave automatica;
4. NCM, CEST, GTIN, CO_SEFIN, unidade e demais atributos tambem nao entram na chave automatica;
5. descricao complementar e demais atributos devem ser listados para auditoria;
6. qualquer agregacao alem da igualdade estrita da descricao principal normalizada e manual.

O runtime atual ja implementa quase toda essa base em `03_descricao_produtos.py`:

- normaliza `descricao`;
- agrupa por `descricao_normalizada`;
- preserva `lista_desc_compl`;
- preserva listas de NCM, CEST, GTIN, CO_SEFIN, unidades, codigos e fontes.

Em `_produtos_final_impl.py`, essa base vira:

- `id_descricao`: chave da descricao listada;
- `id_agrupado_base`: chave automatica do grupo-base;
- `id_agrupado`: chave vigente apos decisao manual, quando houver.

Correcao conceitual importante:

- o que antes vinha sendo descrito genericamente como "agrupamento automatico" deve ser lido como `grupo-base automatico por descricao principal normalizada`;
- a agregacao final de negocio deve ser tratada como manual.

### 3. Agrupamento manual

O documento antigo citava override por `id_linha_origem`. Isso nao corresponde ao modulo atual.

O arquivo `mapa_agrupamento_manual_<cnpj>.parquet` aceita hoje:

- `id_descricao + id_agrupado`
- `descricao_normalizada + id_agrupado`

Precedencia real:

1. manual por `id_descricao`
2. manual por `descricao_normalizada`
3. grupo-base automatico por `id_agrupado_base`

Leitura recomendada:

- use `id_descricao` quando a decisao manual for pontual;
- use `descricao_normalizada` quando a decisao valer para todo o grupo-base;
- nao venda a etapa automatica como agregacao semantica final.

### 4. Conversao de unidades

O texto anterior apresentava equivalencia fisica como se fosse a unica regra ativa. O runtime atual ainda usa heuristica por preco medio no calculo de `fator`, com rastreabilidade explicita em `fator_origem`.

Regra real hoje:

- unidade de referencia: `unid_ref_override` -> `unid_ref_sugerida` -> `unid_ref_auto`
- fator: `fator_override` -> razao de preco medio -> fallback
- origem do fator na tabela canonica: `manual`, `preco`, `fallback_sem_preco`, `fallback_sem_preco_ref`

O service MDS expoe aliases conceituais (`fator_conversao`, `fator_conversao_origem`), mas o artefato canonico do pipeline continua baseado em `fator` / `unid_ref`.

### 5. Movimentacao e tabelas finais

Os documentos antigos tratavam `quantidade_fisica` e `estoque_final_declarado` como se fossem as colunas consumidas diretamente pelas abas finais. Na pratica:

- `mov_estoque` preserva as colunas conceituais e tambem as colunas legadas;
- `aba_periodos`, `aba_mensal` e `aba_anual` continuam consumindo principalmente `q_conv`, `q_conv_fisica`, `__qtd_decl_final_audit__`, `id_agrupado`, `unid_ref`.

### 6. Status do plano

O plano anterior dizia "fase 2 em progresso" e "fases seguintes planejadas". Isso tambem ficou desatualizado. O estado correto e:

| Area | Estado |
| --- | --- |
| service MDS | implementado |
| orchestrator MDS | implementado |
| descricao_produtos | implementado |
| grupo-base por descricao normalizada | implementado |
| agregacao manual | implementado |
| fatores de conversao | implementado |
| mov_estoque | implementado |
| aba_periodos | implementado |
| aba_mensal | implementado |
| aba_anual | implementado |
| consolidacao documental | precisava de revisao e foi corrigida nesta rodada |

## Diretriz de documentacao daqui para frente

Sempre separar:

- nome conceitual;
- nome real do artefato;
- regra desejada;
- regra de fato implementada.

Quando o tema for agregacao:

- explicitar o que e grupo-base automatico;
- explicitar o que e agregacao manual;
- explicitar que descricao complementar e demais atributos sao listas de apoio, nao chave automatica.

## Lacunas remanescentes a tratar com cuidado

- Ha nomes diferentes entre camada interna (`id_agrupado`) e apresentacao (`id_agregado`); isso e intencional hoje e deve ser documentado, nao mascarado.
- A heuristica por preco em fatores continua ativa; portanto nao se pode vender a metodologia como 100 por cento fisica.
- Qualquer migracao de `q_conv` para nomes apenas conceituais exigira transicao nos consumidores atuais.
- `_produtos_final_impl.py` ainda chama a etapa automatica de agrupamento; conceitualmente, o nome mais preciso agora e `grupo-base automatico`.

## Resultado desta consolidacao

Os arquivos em `metodologia_mds/` passam a descrever:

- o contrato vigente do runtime;
- os aliases conceituais usados pela metodologia;
- a regra simples de grupo-base automatico por descricao principal normalizada;
- a descricao complementar e demais atributos como listas de apoio;
- a agregacao final como decisao manual;
- as formulas e nomes efetivamente expostos nos Parquets finais.
