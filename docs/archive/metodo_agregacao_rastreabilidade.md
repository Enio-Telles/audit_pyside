# Método de Agregação, Desagregação e Rastreabilidade de Produtos (Fio de Ouro)

Este documento descreve detalhadamente o fluxo de engenharia de dados e regras de negócio utilizadas para identificar, agrupar, desagregar e rastrear produtos fiscais até sua origem — linha a linha — nos documentos (NFe, NFCe, EFD C170, Bloco H).

A metodologia empregada garante o conceito de **"Fio de Ouro" (Golden Thread)**: qualquer métrica agregada pode ser auditada de volta ao seu registro original irrefutável (XML ou TXT original).

---

## 1. O Identificador Universal de Origem (`codigo_fonte`)

A primeira etapa (Etapa 0) ocorre diretamente nas extrações de banco de dados (SQL). Para garantir que dois produtos de fornecedores diferentes não sejam acidentalmente mesclados antes do tempo por terem nomes idênticos (ex: "COCA COLA 2L"), foi criado o `codigo_fonte`.

**O que é o `codigo_fonte`?**  
Uma chave composta (string) que garante a unicidade absoluta de um "item de catálogo" na visão do emitente.
- **Regra Geral:** `CNPJ_Emitente + '|' + codigo_produto_original`
- **Exemplo na NFe/NFCe:** `00000000000191|12345`

Nos arquivos extraídos (`nfe_{cnpj}.parquet`, `nfce_{cnpj}.parquet`, `c170_{cnpj}.parquet`, `bloco_h_{cnpj}.parquet`), toda linha originária recebe este `codigo_fonte` nativamente. O identificador físico da linha (ex: `chave_acesso + prod_nitem` ou `reg_0000_id + num_doc + num_item`) também é estritamente preservado.

---

## 2. A Construção do Catálogo de Produtos Brutos

Os scripts `produtos_unidades.py` e `produtos.py` processam essas fontes originais para extrair um catálogo limpo dos produtos.

1. **Granularidade:** O catálogo bruto garante **exatamente uma linha física por `codigo_fonte`**. O campo `codigo_fonte` passa a se chamar `chave_produto` (ou `chave_item`) dentro dos artefatos analíticos de domínio de produto.
2. **Sem pre-agrupamentos:** Não há perda de granularidade. Variações como unidades de medida (PC, UN, KG) e históricos de preço de compra/venda são indexados individualmente atrelados a seu respectivo `codigo_fonte`.
3. **Normalização:** A descrição do produto original sofre remoção de acentos, caracteres especiais e padronização de espaços em branco para originar a `descricao_normalizada`, essencial para a próxima etapa.

---

## 3. O Método de Agregação Automática (Heurística)

Uma vez que temos o catálogo bruto, o algoritmo (`produtos_agrupados.py`) processa os produtos de forma autônoma para conectá-los (Agrupamento / Master Data Management). O objetivo é unir produtos essencialmente idênticos (mesmo que com `codigo_fonte` distintos) sob um idêntico guarda-chuva (`id_agrupado`).

### 3.1. Condições de Agregação Automática (Match)
O sistema avalia cada `chave_produto` e tenta associá-lo a um grupo existente ou cria um novo grupo. Dois produtos se aglomeram se validarem pelo menos uma das regras:

- **Regra Dourada (GTIN):**  
  Se as chaves de produto compartilharem o mesmo **GTIN/EAN** presente no banco de dados. O GTIN é um código de barra universal exato. Se houver sobreposição na lista de GTINs de ambos, eles são agrupados.
  
- **Regra de Semântica e Tributação (Descrição + NCM):**  
  Se não houver GTIN, o sistema exige duas provas simultâneas:
  1. A `descricao_normalizada` deve ser **exatamente igual**.
  2. As nomenclaturas do Mercosul (`lista_ncm`) devem ter **pelo menos um cruzamento/interseção**.
  *(Se não tiverem NCM - por estarem ausentes - e a descrição for idêntica, eles também são agrupados por fallback de tolerância).*

### 3.2. A Tabela Mestra e a Tabela Ponte
Satisfeitas as condições, o script aloca um **`id_agrupado`** (ex: `PROD_MSTR_00012`). Para não destruir o "Fio de Ouro", o agrupamento exporta 2 (duas) tabelas distintas:

1. **Tabela Mestre (`produtos_agrupados.parquet`):**
   - Contém metadados consolidados (eleitos por contagem frequencista): `descr_padrao`, `ncm_padrao`, `cest_padrao`, `co_sefin_padrao`, `co_sefin_agr`.
   - É nela que a interface gráfica opera e onde as revisões são auditadas.
   
2. **Tabela Ponte (`map_produto_agrupado.parquet`):**
   - O coração do relacionamento. Possui formato Chave-Valor (`chave_produto` -> `id_agrupado`).

---

## 4. O Método de Análise e Intervenção Manual (Agregação e Desagregação)

Nem sempre a regra automática é perfeita. Erros de cadastro podem separar produtos parentes ou unir errados.
O serviço em Python que alimenta a UI (`aggregation_service.py`) permite edições destrutíveis reversíveis suportando 100% de rastreabilidade.

### 4.1. Agregação Manual
- **Ação:** O analista seleciona 3 registros mestres (`id_agrupado_1`, `id_agrupado_A`, `id_agrupado_X`) via Grid interativo.
- **Efeito Computacional:** O backend compila os metadados dos três em um recém-criado `id_agrupado_Y`.
- **Sincronização Física:** Todas as antigas chaves brutas de origem (`chave_produto`) vinculadas aos 3 IDs antigos são extirpadas do `map_produto_agrupado.parquet` e reescritas apontando agora para o `id_agrupado_Y`. Esta deleção/restauração da **Tabela Ponte** mantém o JOIN unificado no processamento. A tabela Mestra substitui os IDs deletados pelo novo ID único.

### 4.2. Desagregação Manual
- **Ação:** O analista isola ("split") o `id_agrupado_Y` em seus componentes originais.
- **Efeito Computacional:** A tabela ponte restaura o relacionamento autônomo. Mapeamentos individuais reconstroem o `id_agrupado` 1-para-1 com a `chave_produto` estrita de cada vertente divergente, refazendo totalizadores de NCM_Padrao de modo atômico.

---

## 5. Como Identificamos os Agrupamentos nas Fontes Originais? (A Rastreabilidade)

O objetivo primário era: *"Identificar em cada uma das linhas das tabelas c170, bloco_h, NFe e NFCe os produtos já identificados (agregados e não agregados)"*. 

Isto é realizado primariamente no processo **`enriquecimento_fontes.py`** (ou rotinas legadas estabilizadas de relatórios em `fontes_produtos.py`).

**A Mágica do Fluxo Relacional:**
1. A fonte (ex: NFCe) é lida. Lembre-se, ela possui nativamente o `codigo_fonte` (coluna criada nativamente via SQL Etapa 0).
2. O Pandas/Polars faz um **`LEFT JOIN`** do arquivo original (ex: `nfce_3767...parquet`) com a **Tabela Ponte** (`map_produto_agrupado.parquet`) ligando a string de `codigo_fonte` (fonte) à string `chave_produto` (tabela ponte).
3. **Imediatamente, cada linha bruta ganha a coluna virtual `id_agrupado`.**
4. Se o `id_agrupado` for detectado como um agrupamento majoritário (>1 fornecedor com a mesma chave) ou puro (não agrupado), é irrelevante do ponto de vista sistêmico, pois a tabela de ponte já absorveu a lógica inteira de redução.
5. Com o `id_agrupado` espetado na linha bruta, aplica-se num segundo **`LEFT JOIN`** com a **Tabela Mestra** (`produtos_agrupados.parquet`) e com a Tabela de **Fatores de Conversão** (`fatores_conversao.parquet`).

**O que o usuário vê na auditoria ao final da pipeline:**
- A linha de documento NFe original exata: `chave_acesso`, `prod_nitem`, `prod_ucom`
- Como ela foi classificada: `codigo_fonte` da nota originária e a qual `id_agrupado` foi amarrada em nosso datawarehouse virtual.
- As medidas padronizadas (a métrica de Fator multiplicativo e a Quantidade Padronizada `qtd_padronizada`).

Tudo isso sem jamais re-agrupar e destruir a linha da NFe. O "Fio de Ouro" encontra-se assim completado em via de mão dupla (top-down, e bottom-up).

---

## 6. A Tabela Unificada de Movimentação de Estoque (`movimentacao_estoque`)

Para fornecer uma visão consolidada e cronológica de todas as entradas, saídas e saldos de inventário, o sistema gera a tabela final **`movimentacao_estoque_{cnpj}.parquet`** a partir das fontes enriquecidas (C170, Bloco H, NFe e NFCe).

A geração desta tabela atinge três objetivos principais que expandem a rastreabilidade e facilitam a auditoria fiscal:

### 6.1. Harmonização de Colunas (De/Para)
O processo (`movimentacao_estoque.py`) utiliza o mapeamento dinâmico definido em `Tabela_estoques.xlsx` para traduzir a diversidade de nomes de colunas das 4 fontes (ex: `ind_oper` no C170, `tipo_operacao` na NFe) para um schema padronizado e universal.
Principais colunas harmonizadas incluem:
- **Identificadores Dourados:** `Chv_nfe`, `num_nfe`, `Num_item`, `nsu`.
- **Datas:** `Dt_doc` (Data do documento/emissão), `Dt_e_s` (Data de Entrada/Saída).
- **Classificação Básica:** `Cod_item`, `Ncm`, `Cest`, `Cfop`, `Cst`.
- **Valores e Quantidades:** `Qtd`, `Unid`, `Vl_item`, `Aliq_icms`, `Vl_bc_icms`, `Vl_icms`, etc.
- **Rastreabilidade (MDM):** `id_agrupado`, `ncm_padrao`, `cest_padrao`, `descr_padrao` (absorvidos do enriquecimento prévio).

### 6.2. Geração de Inventário Sintético (Faltantes)
Para garantir que a modelagem matemática e os algoritmos de preço médio funcionem perfeitamente em produtos que tiveram movimentação mas que a empresa **não declarou em inventário (Bloco H)**, o sistema intervém:
- Ele identifica produtos agrupados ativos que não possuem registro de `Tipo_operacao = "inventario"`.
- Sintetiza um registro artificial para suprir a lacuna, onde:
  - `Qtd = 0.0` e `Vl_item = 0.0`
  - `Unid = null`
  - `Ser = "gerado"` (Para alertar o sistema de que trata-se de um registro sintético de abertura/fechamento nulo).

### 6.3. Enriquecimento de Parâmetros de Pauta Fiscal (CO SEFIN)
Através do módulo `co_sefin_class.py`, cada linha da movimentação de estoque recebe os parâmetros oficiais de tributação da Secretaria de Finanças.
1. O algoritmo calcula a chave `co_sefin_final` com base no `ncm_padrao` e `cest_padrao`. Essa chave assume o nome da coluna **`co_sefin_agr`**.
2. Ele cruza esse código com a base oficial de pautas Sefin (`sitafe_produto_sefin_aux.parquet`).
3. O JOIN obedece à rigorosa **validação temporal**: a pauta só é válida se a data de referência da movimentação (considerada como a maior data entre `Dt_doc` e `Dt_e_s`) estiver entre a data de vigência inicial e final da pauta (`it_da_inicio` a `it_da_final`).
4. Se houver correspondência ativa no período, as seguintes colunas de tributação fiscal e Substituição Tributária (ST) são injetadas nativamente na movimentação:
   - `it_pc_interna` (Alíquota Interna)
   - `it_in_st` (Indicador de ST)
   - `it_pc_mva` e `it_in_mva_ajustado` (Fatores MVA)
   - `it_in_isento_icms`, `it_pc_reducao`, `it_in_reducao_credito` (Benefícios base)
   - `it_in_combustivel`, `it_in_pmpf` (Pautas específicas)
