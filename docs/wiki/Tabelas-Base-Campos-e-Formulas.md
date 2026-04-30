# Tabelas Base — Campos, Relações e Fórmulas

As tabelas base transformam documentos fiscais brutos em itens, descrições normalizadas e chaves iniciais de rastreabilidade. Elas são a fundação do restante do pipeline.

## 1. `tb_documentos_<cnpj>.parquet`

### Papel

Consolida documentos fiscais extraídos de Oracle, XML, SPED C170 e Bloco H em uma base comum. É a primeira tabela do pipeline e alimenta `item_unidades`.

### Relações

| Relação | Campos | Como interpretar |
|---|---|---|
| `tb_documentos` -> `item_unidades` | `chave_acesso`, `num_doc`, `cfop`, `fonte` | Os documentos são desdobrados em itens e unidades. |
| `tb_documentos` -> `itens` | `chave_acesso`, `num_doc`, `num_item` quando disponível | Mantém o contexto documental da linha de item. |

### Campos

| Campo | Tipo | Explicação | Relação/Fórmula |
|---|---|---|---|
| `chave_acesso` | `str` | Chave de acesso da NF-e/NFC-e, normalmente com 44 dígitos. | Liga documento, item e XML quando a chave existe. |
| `num_doc` | `str` | Número do documento fiscal. | Usado com `num_item` e fonte para rastreabilidade. |
| `cnpj_emitente` | `str` | CNPJ de quem emitiu o documento. | Ajuda a classificar entrada/saída conforme o CNPJ analisado. |
| `cnpj_destinatario` | `str` | CNPJ de quem recebeu o documento. | Ajuda a classificar entrada/saída conforme o CNPJ analisado. |
| `dt_doc` | `date` | Data de emissão do documento. | Base temporal para análises quando não houver data de entrada/saída. |
| `cfop` | `str` | Código Fiscal de Operações e Prestações. | Ajuda a determinar natureza da operação e elegibilidade fiscal. |
| `fonte` | `str` | Origem do registro: `nfe`, `nfce`, `c170`, `bloco_h` etc. | Preserva origem para auditoria e regras específicas por fonte. |

### Fórmulas e normalizações

```text
cnpj_normalizado = somente_digitos(cnpj_original)
```

```text
fonte = identificador_da_origem_do_registro
```

A tabela não calcula estoque nem imposto. Seu papel é padronizar documentos e preservar origem.

---

## 2. `item_unidades_<cnpj>.parquet`

### Papel

Agrega itens por descrição e unidade de medida, separando compras, vendas, quantidades e classificações fiscais. É uma das bases para fatores de conversão.

### Relações

| Relação | Campos | Como interpretar |
|---|---|---|
| `tb_documentos` -> `item_unidades` | `descricao`, `unid`, `ncm`, `cest`, `cfop` | O documento é resumido por produto/unidade. |
| `item_unidades` -> `itens` | `descricao`, `descricao_normalizada`, `unid`, `ncm`, `cest` | Os itens detalhados herdam dados normalizados. |
| `item_unidades` -> `fatores_conversao` | `id_item_unid`, `descricao_normalizada`, `unid`, preços médios | Permite calcular a unidade de referência e fatores. |
| `item_unidades` -> `produtos_final` | `descricao_normalizada`, `id_item_unid` quando houver mapa | Ajuda a vincular a descrição ao produto agrupado. |

### Campos

| Campo | Tipo | Explicação | Relação/Fórmula |
|---|---|---|---|
| `id_item_unid` | `str` | Chave física da combinação item/unidade, quando materializada. | Preferencial para ligar `item_unidades` ao agrupamento e fatores. |
| `descricao` | `str` | Descrição original do produto. | Origem da normalização textual. |
| `descricao_normalizada` | `str` | Descrição padronizada. | `normalizar(descricao)`. Usada para agrupamento e fallback. |
| `unid` | `str` | Unidade de medida da linha: `UN`, `CX`, `KG`, etc. | Relaciona-se com `unid_ref` em `fatores_conversao`. |
| `compras` | `float` | Valor total comprado para a combinação produto/unidade. | Base para preço médio de compra. |
| `vendas` | `float` | Valor total vendido para a combinação produto/unidade. | Base para preço médio de venda. |
| `qtd_compras` | `float` | Quantidade total comprada. | Denominador do preço médio de compra. |
| `qtd_vendas` | `float` | Quantidade total vendida. | Denominador do preço médio de venda. |
| `ncm` | `str` | Código NCM associado ao item. | Propagado para classificação e produto final. |
| `cest` | `str` | Código CEST associado ao item. | Usado em classificação fiscal e ST. |

### Fórmulas

```text
descricao_normalizada = remover_acentos(upper(trim(colapsar_espacos(descricao))))
```

```text
preco_medio_compra = compras / qtd_compras, se qtd_compras > 0
```

```text
preco_medio_venda = vendas / qtd_vendas, se qtd_vendas > 0
```

Quando não houver quantidade positiva, o preço médio correspondente deve ser nulo ou tratado como sem preço.

---

## 3. `itens_<cnpj>.parquet`

### Papel

Detalha os itens dos documentos fiscais com contexto documental. Mantém o vínculo entre a linha de origem e os atributos fiscais usados nas próximas etapas.

### Relações

| Relação | Campos | Como interpretar |
|---|---|---|
| `item_unidades` -> `itens` | `descricao`, `descricao_normalizada`, `unid`, `ncm`, `cest` | A tabela detalhada recebe informações normalizadas. |
| `tb_documentos` -> `itens` | `chave_acesso`, `num_doc`, `num_item`, `fonte` | O item mantém o contexto do documento. |
| `itens` -> `descricao_produtos` | `descricao`, `ncm`, `cest`, `fonte` | As descrições são consolidadas e contadas. |
| `itens` -> tabelas enriquecidas | `id_linha_origem` | Permite rastrear linhas processadas. |

### Campos

| Campo | Tipo | Explicação | Relação/Fórmula |
|---|---|---|---|
| `id_linha_origem` | `str` | Identificador único da linha original. | Campo central de auditoria ponta a ponta. |
| `chave_acesso` | `str` | Chave da NF-e/NFC-e. | Liga item ao documento e ao XML. |
| `num_doc` | `str` | Número do documento fiscal. | Usado para conciliar SPED/XML e auditoria. |
| `num_item` | `int` | Número do item dentro do documento. | Diferencia itens no mesmo documento. |
| `descricao` | `str` | Descrição original do produto no documento. | Base para `descricao_produtos`. |
| `ncm` | `str` | NCM informado no item. | Classificação fiscal usada em consolidação. |
| `cest` | `str` | CEST informado no item. | Classificação usada em ST/SITAFE. |
| `cfop` | `str` | CFOP da operação. | Ajuda a interpretar entrada, saída, devolução e natureza fiscal. |
| `qtd` | `float` | Quantidade original do item. | Depois será convertida por `fator`. |
| `valor_unitario` | `float` | Valor unitário original. | Pode alimentar valores convertidos. |
| `valor_total` | `float` | Valor total da linha. | Normalmente `qtd * valor_unitario`, com variações por fonte. |
| `fonte` | `str` | Origem da linha: `nfe`, `nfce`, `c170`, etc. | Permite aplicar regras específicas por fonte. |

### Fórmulas

```text
valor_total ≈ qtd * valor_unitario
```

A igualdade pode não ser exata por arredondamento, descontos, acréscimos, impostos ou forma de extração.

```text
id_linha_origem = composição_estável_da_fonte + documento + item
```

A composição exata pode variar por fonte, mas deve ser estável o bastante para auditoria e reconciliação.

---

## 4. `descricao_produtos_<cnpj>.parquet`

### Papel

Consolida descrições de produtos e prepara o agrupamento. É a etapa que transforma textos de origem em descrições normalizadas e frequências.

### Relações

| Relação | Campos | Como interpretar |
|---|---|---|
| `itens` -> `descricao_produtos` | `descricao`, `ncm`, `cest`, `fonte` | Cada descrição observada vira uma candidata a produto. |
| `descricao_produtos` -> `produtos_final` | `descricao_normalizada`, `id_descricao` | A descrição normalizada entra no agrupamento canônico. |
| `descricao_produtos` -> revisão manual | listas de NCM/CEST/GTIN/unidades, quando disponíveis | Apoia decisões humanas de agrupamento e correção. |

### Campos

| Campo | Tipo | Explicação | Relação/Fórmula |
|---|---|---|---|
| `id_descricao` | `str` | Chave da descrição normalizada, quando materializada. | Liga descrição a `produtos_final` e mapas manuais. |
| `descricao_original` | `str` | Texto original observado nos documentos. | Evidência de origem para auditoria. |
| `descricao_normalizada` | `str` | Texto padronizado. | Base do agrupamento automático por descrição. |
| `ncm` | `str` | NCM associado à descrição. | Pode compor listas ou atributos consolidados. |
| `cest` | `str` | CEST associado à descrição. | Apoia classificação fiscal e ST. |
| `frequencia` | `int` | Número de ocorrências da descrição. | Ajuda a escolher descrição representativa. |
| `fontes` | `list` | Fontes onde a descrição aparece. | Ajuda a medir cobertura e origem da descrição. |
| `lista_desc_compl` | `list` | Lista de descrições complementares, quando disponível. | Apoio manual; não deve compor a chave automática. |
| `lista_ncm` | `list` | NCMs observados para o grupo/descrição. | Auditoria de divergência fiscal. |
| `lista_cest` | `list` | CESTs observados. | Auditoria de divergência de ST/classificação. |
| `lista_gtin` | `list` | GTINs observados. | Apoio a identificação de produto. |
| `lista_co_sefin` | `list` | Códigos SEFIN observados. | Apoio à classificação SITAFE. |
| `lista_unidades` | `list` | Unidades observadas. | Apoio à escolha da unidade de referência. |
| `lista_codigos` | `list` | Códigos de produto observados nas fontes. | Apoio à revisão manual. |

### Fórmulas e critérios

```text
descricao_normalizada = remover_acentos(upper(trim(colapsar_espacos(descricao_original))))
```

```text
frequencia = count(linhas com a mesma descricao_normalizada)
```

```text
fontes = distinct(fonte)
```

```text
listas_de_auditoria = valores_distintos_observados_por_descricao_ou_grupo
```

A descrição complementar, GTIN, NCM, CEST e unidade ajudam a revisar o agrupamento, mas a regra automática mínima do agrupamento é a igualdade da descrição principal normalizada.
