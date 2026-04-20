# Referências de arquivos Parquet
Gerado em 2026-04-20T16:28:28.371883 UTC

Listagem de arquivos .parquet detectados no repositório com metadados e amostras (até 3 linhas).

## dados/CNPJ/84654326000394/analises/produtos/descricao_produtos_84654326000394.parquet
- Caminho: dados/CNPJ/84654326000394/analises/produtos/descricao_produtos_84654326000394.parquet
- Tamanho: 177.5KB (181771 bytes)
- Linhas: 1962
- Colunas:
  - `id_descricao`: String
  - `descricao_normalizada`: String
  - `descricao`: String
  - `lista_desc_compl`: List(String)
  - `lista_codigos`: List(String)
  - `lista_tipo_item`: List(String)
  - `lista_ncm`: List(String)
  - `lista_cest`: List(String)
  - `lista_co_sefin`: List(String)
  - `lista_gtin`: List(String)
  - `lista_unid`: List(String)
  - `lista_codigo_fonte`: List(String)
  - `fontes`: List(String)
  - `lista_id_item_unid`: List(String)
  - `lista_id_item`: List(String)

Amostra (até 3 linhas):

| id_descricao | descricao_normalizada | descricao | lista_desc_compl | lista_codigos | lista_tipo_item | lista_ncm | lista_cest | lista_co_sefin | lista_gtin | lista_unid | lista_codigo_fonte | fontes | lista_id_item_unid | lista_id_item |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| id_descricao_1 | 10.16.5 10PR JET TRA | 10.16.5 10PR JET TRA | [] | ['260723'] | ['00'] | ['40111000'] | [] | [] | [] | ['1'] | ['84654326000394\|260723'] | ['bloco_h'] | ['id_item_unid_1'] | ['id_item_1'] |
| id_descricao_2 | 10.5/80-18 MAGGION | 10.5/80-18 MAGGION | [] | ['100330'] | ['00'] | ['40111000'] | [] | [] | [] | ['1'] | ['84654326000394\|100330'] | ['bloco_h'] | ['id_item_unid_2'] | ['id_item_2'] |
| id_descricao_3 | 1000R20 16PR JUC3 MISTO JK | 1000R20 16PR JUC3 MISTO JK | [] | ['991121'] | ['00'] | ['40112090', '40119090'] | ['1600200'] | [] | ['SEM GTIN'] | ['10', 'PC'] | ['84654326000394\|991121'] | ['bloco_h', 'nfe'] | ['id_item_unid_3', 'id_item_unid_4'] | ['id_item_3'] |

---

## dados/CNPJ/84654326000394/analises/produtos/fatores_conversao_84654326000394.parquet
- Caminho: dados/CNPJ/84654326000394/analises/produtos/fatores_conversao_84654326000394.parquet
- Tamanho: 17.9KB (18325 bytes)
- Linhas: 1962
- Colunas:
  - `id_agrupado`: String
  - `unid_ref`: String
  - `unid`: String
  - `fator`: Float64

Amostra (até 3 linhas):

| id_agrupado | unid_ref | unid | fator |
| --- | --- | --- | --- |
| id_agrupado_auto_0015370f1a25 | UN1 | UN1 | 1.0 |
| id_agrupado_auto_006b0f6767e8 | 1 | 1 | 1.0 |
| id_agrupado_auto_006d5e4b28bb | UN1 | UN1 | 1.0 |

---

## dados/CNPJ/84654326000394/analises/produtos/item_unidades_84654326000394.parquet
- Caminho: dados/CNPJ/84654326000394/analises/produtos/item_unidades_84654326000394.parquet
- Tamanho: 164.8KB (168792 bytes)
- Linhas: 4356
- Colunas:
  - `id_item_unid`: String
  - `codigo`: String
  - `descricao`: String
  - `descr_compl`: String
  - `tipo_item`: String
  - `ncm`: String
  - `cest`: String
  - `co_sefin_item`: String
  - `gtin`: String
  - `unid`: String
  - `compras`: Float64
  - `qtd_compras`: Float64
  - `vendas`: Float64
  - `qtd_vendas`: Float64
  - `lista_codigo_fonte`: List(String)
  - `fontes`: List(String)

Amostra (até 3 linhas):

| id_item_unid | codigo | descricao | descr_compl | tipo_item | ncm | cest | co_sefin_item | gtin | unid | compras | qtd_compras | vendas | qtd_vendas | lista_codigo_fonte | fontes |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| id_item_unid_1 | 260723 | 10.16.5 10PR JET TRA |  | 00 | 40111000 |  |  |  | 1 | 0.0 | 0.0 | 0.0 | 0.0 | ['84654326000394\|260723'] | ['bloco_h'] |
| id_item_unid_2 | 100330 | 10.5/80-18 MAGGION |  | 00 | 40111000 |  |  |  | 1 | 0.0 | 0.0 | 0.0 | 0.0 | ['84654326000394\|100330'] | ['bloco_h'] |
| id_item_unid_3 | 991121 | 1000R20 16PR JUC3 MISTO JK |  | 00 | 40119090 |  |  |  | 10 | 0.0 | 0.0 | 0.0 | 0.0 | ['84654326000394\|991121'] | ['bloco_h'] |

---

## dados/CNPJ/84654326000394/analises/produtos/itens_84654326000394.parquet
- Caminho: dados/CNPJ/84654326000394/analises/produtos/itens_84654326000394.parquet
- Tamanho: 135.5KB (138736 bytes)
- Linhas: 1962
- Colunas:
  - `id_item`: String
  - `codigo`: String
  - `descricao_normalizada`: String
  - `descricao`: String
  - `descr_compl`: String
  - `tipo_item`: String
  - `ncm`: String
  - `cest`: String
  - `co_sefin_item`: String
  - `gtin`: String
  - `lista_unid`: List(String)
  - `fontes`: List(String)
  - `lista_id_item_unid`: List(String)

Amostra (até 3 linhas):

| id_item | codigo | descricao_normalizada | descricao | descr_compl | tipo_item | ncm | cest | co_sefin_item | gtin | lista_unid | fontes | lista_id_item_unid |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| id_item_1 | 260723 | 10.16.5 10PR JET TRA | 10.16.5 10PR JET TRA |  | 00 | 40111000 |  |  |  | ['1'] | ['bloco_h'] | ['id_item_unid_1'] |
| id_item_2 | 100330 | 10.5/80-18 MAGGION | 10.5/80-18 MAGGION |  | 00 | 40111000 |  |  |  | ['1'] | ['bloco_h'] | ['id_item_unid_2'] |
| id_item_3 | 991121 | 1000R20 16PR JUC3 MISTO JK | 1000R20 16PR JUC3 MISTO JK |  | 00 | 40112090 | 1600200 |  | SEM GTIN | ['10', 'PC'] | ['bloco_h', 'nfe'] | ['id_item_unid_3', 'id_item_unid_4'] |

---

## dados/CNPJ/84654326000394/analises/produtos/map_produto_agrupado_84654326000394.parquet
- Caminho: dados/CNPJ/84654326000394/analises/produtos/map_produto_agrupado_84654326000394.parquet
- Tamanho: 118.2KB (121025 bytes)
- Linhas: 3387
- Colunas:
  - `chave_produto`: String
  - `id_agrupado`: String
  - `codigo_fonte`: String
  - `descricao_normalizada`: String

Amostra (até 3 linhas):

| chave_produto | id_agrupado | codigo_fonte | descricao_normalizada |
| --- | --- | --- | --- |
| id_descricao_1277 | id_agrupado_auto_466fd8d39586 | 84308980000931\|165084 | INSET AERO MP BUZZ OFF 400ML LIMONENO |
| id_descricao_4 | id_agrupado_auto_ac8de518bbbc | 84654326000394\|181116 | 1000R20 16PR LISO |
| id_descricao_341 | id_agrupado_auto_b3eda7cf8298 | 84654326000203\|991204 | 235/75R17.5 143/141J 18PR BORR |

---

## dados/CNPJ/84654326000394/analises/produtos/mov_estoque_84654326000394.parquet
- Caminho: dados/CNPJ/84654326000394/analises/produtos/mov_estoque_84654326000394.parquet
- Tamanho: 1.9MB (1958333 bytes)
- Linhas: 30056
- Colunas:
  - `ordem_operacoes`: UInt32
  - `Tipo_operacao`: String
  - `nsu`: Int64
  - `Chv_nfe`: String
  - `mod`: String
  - `Ser`: String
  - `num_nfe`: String
  - `Dt_doc`: Datetime(time_unit='us', time_zone=None)
  - `Dt_e_s`: Datetime(time_unit='us', time_zone=None)
  - `Num_item`: String
  - `finnfe`: Int64
  - `co_uf_emit`: String
  - `co_uf_dest`: String
  - `Cod_item`: String
  - `Cod_barra`: String
  - `Ncm`: String
  - `Cest`: String
  - `Tipo_item`: String
  - `Descr_item`: String
  - `Descr_compl`: String
  - `Cfop`: String
  - `Cst`: String
  - `Qtd`: Float64
  - `Unid`: String
  - `Vl_item`: Float64
  - `preco_item`: Float64
  - `Aliq_icms`: Float64
  - `Vl_bc_icms`: Float64
  - `Vl_icms`: Float64
  - `vl_icms_st`: Float64
  - `vl_bc_icms_st`: Float64
  - `aliq_st`: Float64
  - `id_agrupado`: String
  - `ncm_padrao`: String
  - `cest_padrao`: String
  - `descr_padrao`: String
  - `unid_ref`: String
  - `fator`: Float64
  - `origem_vinculo_produto`: String
  - `origem_evento_estoque`: String
  - `evento_sintetico`: Boolean
  - `fonte`: String
  - `co_sefin_final`: String
  - `co_sefin_agr`: String
  - `mov_rep`: Boolean
  - `excluir_estoque`: Null
  - `dev_simples`: Null
  - `dev_venda`: Null
  - `dev_compra`: Null
  - `dev_ent_simples`: Null
  - `fator_original`: Float64
  - `fator_conversao`: Float64
  - `fator_conversao_origem`: String
  - `unid_ref_sugerida`: String
  - `unidade_referencia`: String
  - `infprot_cstat`: String
  - `__ano_saldo__`: Int32
  - `periodo_inventario`: Int32
  - `__qtd_decl_final_audit__`: Float64
  - `q_conv`: Float64
  - `q_conv_fisica`: Float64
  - `__q_conv_sinal__`: Float64
  - `preco_unit`: Float64
  - `quantidade_convertida`: Float64
  - `tipo_operacao`: String
  - `quantidade_fisica`: Float64
  - `quantidade_fisica_sinalizada`: Float64
  - `estoque_final_declarado`: Float64
  - `saldo_estoque_anual`: Float64
  - `entr_desac_anual`: Float64
  - `custo_medio_anual`: Float64
  - `saldo_estoque_periodo`: Float64
  - `entr_desac_periodo`: Float64
  - `custo_medio_periodo`: Float64
  - `delta_decl_final_anual`: Float64
  - `delta_decl_final_periodo`: Float64

Amostra (até 3 linhas):

| ordem_operacoes | Tipo_operacao | nsu | Chv_nfe | mod | Ser | num_nfe | Dt_doc | Dt_e_s | Num_item | finnfe | co_uf_emit | co_uf_dest | Cod_item | Cod_barra | Ncm | Cest | Tipo_item | Descr_item | Descr_compl | Cfop | Cst | Qtd | Unid | Vl_item | preco_item | Aliq_icms | Vl_bc_icms | Vl_icms | vl_icms_st | vl_bc_icms_st | aliq_st | id_agrupado | ncm_padrao | cest_padrao | descr_padrao | unid_ref | fator | origem_vinculo_produto | origem_evento_estoque | evento_sintetico | fonte | co_sefin_final | co_sefin_agr | mov_rep | excluir_estoque | dev_simples | dev_venda | dev_compra | dev_ent_simples | fator_original | fator_conversao | fator_conversao_origem | unid_ref_sugerida | unidade_referencia | infprot_cstat | __ano_saldo__ | periodo_inventario | __qtd_decl_final_audit__ | q_conv | q_conv_fisica | __q_conv_sinal__ | preco_unit | quantidade_convertida | tipo_operacao | quantidade_fisica | quantidade_fisica_sinalizada | estoque_final_declarado | saldo_estoque_anual | entr_desac_anual | custo_medio_anual | saldo_estoque_periodo | entr_desac_periodo | custo_medio_periodo | delta_decl_final_anual | delta_decl_final_periodo |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 1 | 0 - ESTOQUE INICIAL gerado |  |  |  | gerado |  | 2025-01-01 00:00:00 | 2025-01-01 00:00:00 |  |  |  |  | 4543 | 7891022101300 | 38089419 |  | 00 | DESINF KALIPTO 5L EUCALIPTO |  | 1556 |  | 0.0 |  | 0.0 |  |  |  |  |  |  |  | id_agrupado_auto_0015370f1a25 | 38089419 |  | DESINF KALIPTO 5L EUCALIPTO | UN1 | 1.0 |  | estoque_inicial_gerado | True | gerado |  |  | False |  |  |  |  |  | 1.0 | 1.0 | fisico | UN1 | UN1 |  | 2025 | 1 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0 - ESTOQUE INICIAL gerado | 0.0 | 0.0 |  | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 |  |  |
| 2 | 1 - ENTRADA |  | 11251284308980000931550010000945621468437079 | 55 | 1 | 94562 | 2025-12-03 00:00:00 | 2025-12-03 00:00:00 | 15 |  | 11 |  | 4543 | 7891022101300 | 38089419 |  | 00 | DESINF KALIPTO 5L EUCALIPTO |  | 1556 | 090 | 1.0 | UN1 | 29.99 | 29.99 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | id_agrupado_auto_0015370f1a25 | 38089419 |  | DESINF KALIPTO 5L EUCALIPTO | UN1 | 1.0 | codigo_fonte | registro | False | c170 |  |  | False |  |  |  |  |  | 1.0 | 1.0 | fisico | UN1 | UN1 |  | 2025 | 1 | 0.0 | 1.0 | 1.0 | 1.0 | 29.99 | 1.0 | 1 - ENTRADA | 1.0 | 1.0 |  | 1.0 | 0.0 | 29.99 | 1.0 | 0.0 | 29.99 |  |  |
| 3 | 3 - ESTOQUE FINAL |  | bloco_h |  | registro |  | 2025-12-31 00:00:00 | 2025-12-31 00:00:00 |  |  |  |  | 4543 | 7891022101300 | 38089419 |  | 00 | DESINF KALIPTO 5L EUCALIPTO |  |  |  | 1.0 | UN1 | 0.0 | 0.0 |  |  |  |  |  |  | id_agrupado_auto_0015370f1a25 | 38089419 |  | DESINF KALIPTO 5L EUCALIPTO | UN1 | 1.0 | codigo_fonte | inventario_bloco_h | False | bloco_h |  |  | False |  |  |  |  |  | 1.0 | 1.0 | fisico | UN1 | UN1 |  | 2025 | 1 | 1.0 | 1.0 | 0.0 | 0.0 | 0.0 | 1.0 | 3 - ESTOQUE FINAL | 0.0 | 0.0 | 1.0 | 1.0 | 0.0 | 29.99 | 1.0 | 0.0 | 29.99 | 0.0 | 0.0 |

---

## dados/CNPJ/84654326000394/analises/produtos/produtos_agrupados_84654326000394.parquet
- Caminho: dados/CNPJ/84654326000394/analises/produtos/produtos_agrupados_84654326000394.parquet
- Tamanho: 237.0KB (242736 bytes)
- Linhas: 1962
- Colunas:
  - `id_agrupado`: String
  - `lista_chave_produto`: List(String)
  - `descr_padrao`: String
  - `ncm_padrao`: String
  - `cest_padrao`: String
  - `gtin_padrao`: String
  - `lista_ncm`: List(String)
  - `lista_cest`: List(String)
  - `lista_gtin`: List(String)
  - `lista_descricoes`: List(String)
  - `lista_desc_compl`: List(String)
  - `lista_co_sefin`: List(String)
  - `co_sefin_padrao`: String
  - `lista_unidades`: List(String)
  - `co_sefin_divergentes`: Boolean
  - `fontes`: List(String)
  - `ids_origem_agrupamento`: List(String)
  - `lista_itens_agrupados`: List(String)
  - `criterio_agrupamento`: String
  - `origem_agrupamento`: String
  - `qtd_descricoes_grupo`: Int64
  - `versao_agrupamento`: Int64

Amostra (até 3 linhas):

| id_agrupado | lista_chave_produto | descr_padrao | ncm_padrao | cest_padrao | gtin_padrao | lista_ncm | lista_cest | lista_gtin | lista_descricoes | lista_desc_compl | lista_co_sefin | co_sefin_padrao | lista_unidades | co_sefin_divergentes | fontes | ids_origem_agrupamento | lista_itens_agrupados | criterio_agrupamento | origem_agrupamento | qtd_descricoes_grupo | versao_agrupamento |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| id_agrupado_auto_8ff9f9613594 | ['id_descricao_991'] | DESENGRIPANTE M500 300 ML | 38249941 |  | 7898436148273 | ['38249941'] | [] | ['7898436148273'] | ['DESENGRIPANTE M500 300 ML'] | [] | [] |  | ['UNID'] | False | ['c170', 'nfe'] | ['id_agrupado_auto_8ff9f9613594'] | ['DESENGRIPANTE M500 300 ML'] | automatico_descricao_normalizada | automatico | 1 | 1 |
| id_agrupado_auto_cbfb857463eb | ['id_descricao_733'] | CAFE URUPA SACH 500G | 09012100 | 1709600 | 7897087100036 | ['09012100'] | ['1709600'] | ['7897087100036'] | ['CAFE URUPA SACH 500G'] | [] | [] |  | ['UN1'] | False | ['c170', 'nfe'] | ['id_agrupado_auto_cbfb857463eb'] | ['CAFE URUPA SACH 500G'] | automatico_descricao_normalizada | automatico | 1 | 1 |
| id_agrupado_auto_4e8e982d603e | ['id_descricao_594'] | ADAPTADOR P/ JARDIM | 39174090 |  | SEM GTIN | ['39174090'] | [] | ['SEM GTIN'] | ['ADAPTADOR P/ JARDIM'] | [] | [] |  | ['UN'] | False | ['nfe'] | ['id_agrupado_auto_4e8e982d603e'] | ['ADAPTADOR P/ JARDIM'] | automatico_descricao_normalizada | automatico | 1 | 1 |

---

## dados/CNPJ/84654326000394/analises/produtos/produtos_final_84654326000394.parquet
- Caminho: dados/CNPJ/84654326000394/analises/produtos/produtos_final_84654326000394.parquet
- Tamanho: 405.5KB (415250 bytes)
- Linhas: 1962
- Colunas:
  - `id_descricao`: String
  - `descricao_normalizada`: String
  - `descricao`: String
  - `lista_desc_compl`: List(String)
  - `lista_codigos`: List(String)
  - `lista_tipo_item`: List(String)
  - `lista_ncm`: List(String)
  - `lista_cest`: List(String)
  - `lista_co_sefin`: List(String)
  - `lista_gtin`: List(String)
  - `lista_unid`: List(String)
  - `lista_codigo_fonte`: List(String)
  - `fontes`: List(String)
  - `lista_id_item_unid`: List(String)
  - `lista_id_item`: List(String)
  - `id_agrupado_base`: String
  - `criterio_agrupamento`: String
  - `origem_agrupamento`: String
  - `id_agrupado`: String
  - `id_agrupado_right`: String
  - `descr_padrao`: String
  - `ncm_padrao`: String
  - `cest_padrao`: String
  - `gtin_padrao`: String
  - `lista_co_sefin_agr`: List(String)
  - `co_sefin_padrao`: String
  - `lista_unidades_agr`: List(String)
  - `co_sefin_divergentes`: Boolean
  - `fontes_agr`: List(String)
  - `versao_agrupamento`: Int64
  - `criterio_agrupamento_right`: String
  - `origem_agrupamento_right`: String
  - `descricao_final`: String
  - `ncm_final`: String
  - `cest_final`: String
  - `gtin_final`: String
  - `co_sefin_final`: String
  - `unid_ref_sugerida`: String

Amostra (até 3 linhas):

| id_descricao | descricao_normalizada | descricao | lista_desc_compl | lista_codigos | lista_tipo_item | lista_ncm | lista_cest | lista_co_sefin | lista_gtin | lista_unid | lista_codigo_fonte | fontes | lista_id_item_unid | lista_id_item | id_agrupado_base | criterio_agrupamento | origem_agrupamento | id_agrupado | id_agrupado_right | descr_padrao | ncm_padrao | cest_padrao | gtin_padrao | lista_co_sefin_agr | co_sefin_padrao | lista_unidades_agr | co_sefin_divergentes | fontes_agr | versao_agrupamento | criterio_agrupamento_right | origem_agrupamento_right | descricao_final | ncm_final | cest_final | gtin_final | co_sefin_final | unid_ref_sugerida |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| id_descricao_995 | DESINF KALIPTO 5L EUCALIPTO | DESINF KALIPTO 5L EUCALIPTO | [] | ['4543'] | ['00'] | ['38089419'] | [] | [] | ['7891022101300'] | ['UN1'] | ['84308980000931\|4543', '84654326000394\|4543'] | ['bloco_h', 'c170', 'nfe'] | ['id_item_unid_2869', 'id_item_unid_2870'] | ['id_item_995'] | id_agrupado_auto_0015370f1a25 | automatico_descricao_normalizada | automatico | id_agrupado_auto_0015370f1a25 | id_agrupado_auto_0015370f1a25 | DESINF KALIPTO 5L EUCALIPTO | 38089419 |  | 7891022101300 | [] |  | ['UN1'] | False | ['bloco_h', 'c170', 'nfe'] | 1 | automatico_descricao_normalizada | automatico | DESINF KALIPTO 5L EUCALIPTO | 38089419 |  | 7891022101300 |  | UN1 |
| id_descricao_1820 | TARRACHA JUMBO | TARRACHA JUMBO | [] | ['1623'] | ['00'] | ['84818093'] | ['1007900'] | [] | ['SEM GTIN'] | ['1', 'UN'] | ['84654326000394\|1623'] | ['bloco_h', 'nfe'] | ['id_item_unid_4123', 'id_item_unid_4124'] | ['id_item_1820'] | id_agrupado_auto_006b0f6767e8 | automatico_descricao_normalizada | automatico | id_agrupado_auto_006b0f6767e8 | id_agrupado_auto_006b0f6767e8 | TARRACHA JUMBO | 84818093 | 1007900 | SEM GTIN | [] |  | ['1', 'UN'] | False | ['bloco_h', 'nfe'] | 1 | automatico_descricao_normalizada | automatico | TARRACHA JUMBO | 84818093 | 1007900 | SEM GTIN |  | 1 |
| id_descricao_866 | CANETA ESFER TRIS 0.7MM 4UN SLIDE AZUL | CANETA ESFER TRIS 0.7MM 4UN SLIDE AZUL | [] | ['151144'] | ['00'] | ['96081000'] | ['1902700'] | [] | ['7897476687544'] | ['UN1'] | ['84308980000931\|151144', '84654326000394\|151144'] | ['c170', 'nfe'] | ['id_item_unid_2626', 'id_item_unid_2627'] | ['id_item_866'] | id_agrupado_auto_006d5e4b28bb | automatico_descricao_normalizada | automatico | id_agrupado_auto_006d5e4b28bb | id_agrupado_auto_006d5e4b28bb | CANETA ESFER TRIS 0.7MM 4UN SLIDE AZUL | 96081000 | 1902700 | 7897476687544 | [] |  | ['UN1'] | False | ['c170', 'nfe'] | 1 | automatico_descricao_normalizada | automatico | CANETA ESFER TRIS 0.7MM 4UN SLIDE AZUL | 96081000 | 1902700 | 7897476687544 |  | UN1 |

---

## dados/CNPJ/84654326000394/analises/produtos/tb_documentos_84654326000394.parquet
- Caminho: dados/CNPJ/84654326000394/analises/produtos/tb_documentos_84654326000394.parquet
- Tamanho: 3.4MB (3521980 bytes)
- Linhas: 24152
- Colunas:
  - `tipo_operacao`: String
  - `co_destinatario`: String
  - `co_emitente`: String
  - `cnpj_filtro`: String
  - `nsu`: Int64
  - `chave_acesso`: String
  - `prod_nitem`: Int64
  - `codigo_fonte`: String
  - `ide_co_cuf`: Int64
  - `ide_co_indpag`: Null
  - `ide_co_mod`: Int64
  - `ide_serie`: String
  - `nnf`: Int64
  - `dhemi`: Datetime(time_unit='us', time_zone=None)
  - `dhsaient`: Datetime(time_unit='us', time_zone=None)
  - `co_tp_nf`: Int64
  - `co_iddest`: Int64
  - `co_cmun_fg`: String
  - `co_tpemis`: Int64
  - `co_finnfe`: Int64
  - `co_indfinal`: Int64
  - `co_indpres`: Int64
  - `xnome_emit`: String
  - `xfant_emit`: String
  - `co_uf_emit`: String
  - `co_cad_icms_emit`: String
  - `co_cad_icms_st`: String
  - `co_crt`: Int64
  - `xlgr_emit`: String
  - `nro_emit`: String
  - `xcpl_emit`: String
  - `xbairro_emit`: String
  - `co_cmun_emit`: String
  - `xmun_emit`: String
  - `cep_emit`: String
  - `cpais_emit`: String
  - `fone_emit`: String
  - `xpais_emit`: String
  - `cnae_emit`: String
  - `xnome_dest`: String
  - `co_uf_dest`: String
  - `co_indiedest`: String
  - `co_cad_icms_dest`: String
  - `nro_dest`: String
  - `xlgr_dest`: String
  - `xcpl_dest`: String
  - `xbairro_dest`: String
  - `co_cmun_dest`: String
  - `xmun_dest`: String
  - `cep_dest`: String
  - `cpais_dest`: String
  - `xpais_dest`: String
  - `fone_dest`: String
  - `prod_cprod`: String
  - `prod_cean`: String
  - `prod_xprod`: String
  - `prod_ncm`: String
  - `prod_cest`: String
  - `prod_extipi`: String
  - `co_cfop`: Int64
  - `prod_ucom`: String
  - `prod_qcom`: Float64
  - `prod_vuncom`: Float64
  - `prod_vprod`: Float64
  - `prod_ceantrib`: String
  - `prod_utrib`: String
  - `prod_qtrib`: Float64
  - `prod_vuntrib`: Float64
  - `prod_vfrete`: Float64
  - `prod_vseg`: Float64
  - `prod_vdesc`: Float64
  - `prod_voutro`: Float64
  - `prod_indtot`: Int64
  - `icms_csosn`: Int64
  - `icms_cst`: Int64
  - `icms_modbc`: Int64
  - `icms_modbcst`: Int64
  - `icms_motdesicms`: Int64
  - `icms_orig`: Int64
  - `icms_pbcop`: Float64
  - `icms_pcredsn`: Float64
  - `icms_pdif`: Float64
  - `icms_picms`: Float64
  - `icms_picmsst`: Float64
  - `icms_pmvast`: Float64
  - `icms_predbc`: Float64
  - `icms_predbcst`: Float64
  - `icms_ufst`: Null
  - `icms_vbc`: Float64
  - `icms_vbcst`: Float64
  - `icms_vbcstdest`: Float64
  - `icms_vbcstret`: Float64
  - `icms_vcredicmssn`: Float64
  - `icms_vicms`: Float64
  - `icms_vicmsdeson`: Float64
  - `icms_vicmsdif`: Float64
  - `icms_vicmsop`: Float64
  - `icms_vicmsst`: Float64
  - `icms_vicmsstdest`: Float64
  - `icms_vicmsstret`: Float64
  - `ipi_clenq`: Null
  - `ipi_cnpjprod`: Null
  - `ipi_cselo`: Null
  - `ipi_qselo`: Int64
  - `ipi_cenq`: String
  - `ipi_cst`: Int64
  - `ipi_vbc`: Float64
  - `ipi_pipi`: Float64
  - `ipi_qunid`: Float64
  - `ipi_vunid`: Float64
  - `ipi_vipi`: Float64
  - `ii_vbc`: Float64
  - `ii_vdespadu`: Float64
  - `ii_vii`: Float64
  - `ii_viof`: Float64
  - `veic_prod_tpop`: Null
  - `veic_prod_chassi`: Null
  - `veic_prod_ccor`: Null
  - `veic_prod_xcor`: Null
  - `veic_prod_pot`: Null
  - `veic_prod_cilin`: Null
  - `veic_prod_pesol`: Null
  - `veic_prod_pesob`: Null
  - `veic_prod_nserie`: Null
  - `veic_prod_tpcomb`: Null
  - `veic_prod_nmotor`: Null
  - `veic_prod_cmt`: Null
  - `veic_prod_anomod`: Null
  - `veic_prod_anofab`: Null
  - `veic_prod_dist`: Null
  - `veic_prod_tppint`: Null
  - `veic_prod_tpveic`: Null
  - `veic_prod_espveic`: Null
  - `veic_prod_vin`: Null
  - `veic_prod_condveic`: Null
  - `veic_prod_cmod`: Null
  - `veic_prod_ccordenatran`: Null
  - `veic_prod_lota`: Null
  - `veic_prod_tprest`: Null
  - `comb_cprodanp`: Int64
  - `comb_pmixgn`: Null
  - `comb_codif`: Null
  - `comb_qtemp`: Float64
  - `comb_ufcons`: String
  - `tot_vbc`: Float64
  - `tot_vicms`: Float64
  - `tot_vicmsdeson`: Float64
  - `tot_vbcst`: Float64
  - `tot_vst`: Float64
  - `tot_vprod`: Float64
  - `tot_vfrete`: Float64
  - `tot_vseg`: Float64
  - `tot_vdesc`: Float64
  - `tot_vii`: Float64
  - `tot_vipi`: Float64
  - `tot_vpis`: Float64
  - `tot_vcofins`: Float64
  - `tot_voutro`: Float64
  - `tot_vnf`: Float64
  - `tot_vtottrib`: Float64
  - `infprot_cstat`: Int64
  - `versao`: String
  - `prod_indescala`: String
  - `prod_cnpjfab`: Null
  - `prod_cbenef`: Null
  - `icms_vbcfcp`: Float64
  - `icms_pfcp`: Float64
  - `icms_vfcp`: Float64
  - `icms_vbcfcpst`: Float64
  - `icms_pfcpst`: Float64
  - `icms_vfcpst`: Float64
  - `icms_vbcufdest`: Float64
  - `icms_vbcfcpufdest`: Float64
  - `icms_pfcpufdest`: Float64
  - `icms_picmsufdest`: Float64
  - `icms_picmsinter`: Float64
  - `icms_picmsinterpart`: Float64
  - `icms_vfcpufdest`: Float64
  - `icms_vicmsufdest`: Float64
  - `icms_vicmsufremet`: Float64
  - `icms_pst`: Float64
  - `icms_vbcfcpstret`: Float64
  - `icms_pfcpstret`: Float64
  - `icms_vfcpstret`: Float64
  - `icms_predbcefet`: Float64
  - `icms_vbcefet`: Float64
  - `icms_picmsefet`: Float64
  - `icms_vicmsefet`: Float64
  - `med_cprodanvisa`: Null
  - `med_vpmc`: Null
  - `tot_vfcpufdest`: Float64
  - `tot_vicmsufdest`: Float64
  - `tot_vicmsufremet`: Float64
  - `tot_vfcp`: Float64
  - `tot_vfcpst`: Float64
  - `tot_vfcpstret`: Float64
  - `tot_vipidevol`: Float64
  - `icms_cst_a`: String
  - `icms_csosn_a`: String
  - `dt_gravacao`: Datetime(time_unit='us', time_zone=None)
  - `seq_nitem`: Int64
  - `cofins_vcofins`: Float64
  - `cofins_vbc`: Float64
  - `cofins_pcofins`: Float64
  - `pis_vpis`: Float64
  - `pis_vbc`: Float64
  - `pis_ppis`: Float64
  - `dhemi_hora`: Datetime(time_unit='us', time_zone=None)
  - `status_carga_campo_fcp`: String
  - `status_carga_campo_rem_dest`: String
  - `in_versao`: String
  - `email_dest`: String
  - `co_indiedest_`: Int64
  - `fone_dest_a8`: String
  - `ibscbs`: String
  - `origem`: Categorical

Amostra (até 3 linhas):

| tipo_operacao | co_destinatario | co_emitente | cnpj_filtro | nsu | chave_acesso | prod_nitem | codigo_fonte | ide_co_cuf | ide_co_indpag | ide_co_mod | ide_serie | nnf | dhemi | dhsaient | co_tp_nf | co_iddest | co_cmun_fg | co_tpemis | co_finnfe | co_indfinal | co_indpres | xnome_emit | xfant_emit | co_uf_emit | co_cad_icms_emit | co_cad_icms_st | co_crt | xlgr_emit | nro_emit | xcpl_emit | xbairro_emit | co_cmun_emit | xmun_emit | cep_emit | cpais_emit | fone_emit | xpais_emit | cnae_emit | xnome_dest | co_uf_dest | co_indiedest | co_cad_icms_dest | nro_dest | xlgr_dest | xcpl_dest | xbairro_dest | co_cmun_dest | xmun_dest | cep_dest | cpais_dest | xpais_dest | fone_dest | prod_cprod | prod_cean | prod_xprod | prod_ncm | prod_cest | prod_extipi | co_cfop | prod_ucom | prod_qcom | prod_vuncom | prod_vprod | prod_ceantrib | prod_utrib | prod_qtrib | prod_vuntrib | prod_vfrete | prod_vseg | prod_vdesc | prod_voutro | prod_indtot | icms_csosn | icms_cst | icms_modbc | icms_modbcst | icms_motdesicms | icms_orig | icms_pbcop | icms_pcredsn | icms_pdif | icms_picms | icms_picmsst | icms_pmvast | icms_predbc | icms_predbcst | icms_ufst | icms_vbc | icms_vbcst | icms_vbcstdest | icms_vbcstret | icms_vcredicmssn | icms_vicms | icms_vicmsdeson | icms_vicmsdif | icms_vicmsop | icms_vicmsst | icms_vicmsstdest | icms_vicmsstret | ipi_clenq | ipi_cnpjprod | ipi_cselo | ipi_qselo | ipi_cenq | ipi_cst | ipi_vbc | ipi_pipi | ipi_qunid | ipi_vunid | ipi_vipi | ii_vbc | ii_vdespadu | ii_vii | ii_viof | veic_prod_tpop | veic_prod_chassi | veic_prod_ccor | veic_prod_xcor | veic_prod_pot | veic_prod_cilin | veic_prod_pesol | veic_prod_pesob | veic_prod_nserie | veic_prod_tpcomb | veic_prod_nmotor | veic_prod_cmt | veic_prod_anomod | veic_prod_anofab | veic_prod_dist | veic_prod_tppint | veic_prod_tpveic | veic_prod_espveic | veic_prod_vin | veic_prod_condveic | veic_prod_cmod | veic_prod_ccordenatran | veic_prod_lota | veic_prod_tprest | comb_cprodanp | comb_pmixgn | comb_codif | comb_qtemp | comb_ufcons | tot_vbc | tot_vicms | tot_vicmsdeson | tot_vbcst | tot_vst | tot_vprod | tot_vfrete | tot_vseg | tot_vdesc | tot_vii | tot_vipi | tot_vpis | tot_vcofins | tot_voutro | tot_vnf | tot_vtottrib | infprot_cstat | versao | prod_indescala | prod_cnpjfab | prod_cbenef | icms_vbcfcp | icms_pfcp | icms_vfcp | icms_vbcfcpst | icms_pfcpst | icms_vfcpst | icms_vbcufdest | icms_vbcfcpufdest | icms_pfcpufdest | icms_picmsufdest | icms_picmsinter | icms_picmsinterpart | icms_vfcpufdest | icms_vicmsufdest | icms_vicmsufremet | icms_pst | icms_vbcfcpstret | icms_pfcpstret | icms_vfcpstret | icms_predbcefet | icms_vbcefet | icms_picmsefet | icms_vicmsefet | med_cprodanvisa | med_vpmc | tot_vfcpufdest | tot_vicmsufdest | tot_vicmsufremet | tot_vfcp | tot_vfcpst | tot_vfcpstret | tot_vipidevol | icms_cst_a | icms_csosn_a | dt_gravacao | seq_nitem | cofins_vcofins | cofins_vbc | cofins_pcofins | pis_vpis | pis_vbc | pis_ppis | dhemi_hora | status_carga_campo_fcp | status_carga_campo_rem_dest | in_versao | email_dest | co_indiedest_ | fone_dest_a8 | ibscbs | origem |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 1 - SAIDA | 02718182000166 | 84654326000394 | 84654326000394 | 322563916 | 11200184654326000394550010000443061113991391 | 1 | 84654326000394\|109840 | 11 |  | 55 | 1   | 44306 | 2020-01-23 00:00:00 | 2020-01-23 00:00:00 | 1 | 1 | 1100205    | 1 | 1 | 0 | 1 | CHARLENE PNEUS LTDA | CHARLENE PVH | RO | 00000003162583 |  | 3 | AV NACOES UNIDAS | 1608 |  | ROQUE | 1100205    | PORTO VELHO | 76804436 | 1058 | 6932244030 | BRASIL |  | ALMIRANTE RENT A CAR L.V. LTDA ME | RO | 1 | 00000001466321 | 2065 | RUA ALMIRANTE BARROSO |  | NOSSA SENHORA DAS GRACAS | 1100205    | PORTO VELHO | 76804129 | 1058 | BRASIL | 69993217582 | 109840 | SEM GTIN | VALV PN S/C 414 | 84818099 | 1007900 |  | 5405 | PC | 1.0 | 1.0 | 1.0 | SEM GTIN | PC | 1.0 | 1.0 | 0.0 | 0.0 | 0.0 | 0.0 | 1 |  | 60 | 0 | 0 | 0 | 0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 |  | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 |  |  |  | 0 | 999 | 99 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 235.17 | 0.0 | 0.0 | 40.17 | 0.0 | 0.0 | 3.9 | 18.51 | 0.0 | 195.0 | 0.0 | 100 | v4.00 | S |  |  | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 |  |  |  |  |  | 0.0 | 0.0 | 0.0 | 0.0 | 60 |  | 2020-01-24 03:50:44 | 1 | 0.08 | 1.0 | 7.6 | 0.02 | 1.0 | 1.65 | 2020-01-23 09:44:00 | SIM |  |  | clienterover123@hotmail.com.br |  | 93217582 | N | NFe |
| 1 - SAIDA | 02718182000166 | 84654326000394 | 84654326000394 | 322563916 | 11200184654326000394550010000443061113991391 | 2 | 84654326000394\|990872 | 11 |  | 55 | 1   | 44306 | 2020-01-23 00:00:00 | 2020-01-23 00:00:00 | 1 | 1 | 1100205    | 1 | 1 | 0 | 1 | CHARLENE PNEUS LTDA | CHARLENE PVH | RO | 00000003162583 |  | 3 | AV NACOES UNIDAS | 1608 |  | ROQUE | 1100205    | PORTO VELHO | 76804436 | 1058 | 6932244030 | BRASIL |  | ALMIRANTE RENT A CAR L.V. LTDA ME | RO | 1 | 00000001466321 | 2065 | RUA ALMIRANTE BARROSO |  | NOSSA SENHORA DAS GRACAS | 1100205    | PORTO VELHO | 76804129 | 1058 | BRASIL | 69993217582 | 990872 | SEM GTIN | 175/70R14T 04PR LH41 | 40111000 | 1600100 |  | 5405 | PC | 1.0 | 234.17 | 234.17 | SEM GTIN | PC | 1.0 | 234.17 | 0.0 | 0.0 | 40.17 | 0.0 | 1 |  | 60 | 0 | 0 | 0 | 1 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 |  | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 |  |  |  | 0 | 999 | 99 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 235.17 | 0.0 | 0.0 | 40.17 | 0.0 | 0.0 | 3.9 | 18.51 | 0.0 | 195.0 | 0.0 | 100 | v4.00 | S |  |  | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 |  |  |  |  |  | 0.0 | 0.0 | 0.0 | 0.0 | 60 |  | 2020-01-24 03:50:44 | 2 | 18.43 | 194.0 | 9.5 | 3.88 | 194.0 | 2.0 | 2020-01-23 09:44:00 | SIM |  |  |  |  | 93217582 | N | NFe |
| 1 - SAIDA | 07756151291 | 84654326000394 | 84654326000394 | 322558002 | 11200184654326000394550010000443051111719360 | 1 | 84654326000394\|990709 | 11 |  | 55 | 1   | 44305 | 2020-01-23 00:00:00 | 2020-01-23 00:00:00 | 1 | 1 | 1100205    | 1 | 1 | 1 | 1 | CHARLENE PNEUS LTDA | CHARLENE PVH | RO | 00000003162583 |  | 3 | AV NACOES UNIDAS | 1608 |  | ROQUE | 1100205    | PORTO VELHO | 76804436 | 1058 | 6932244030 | BRASIL |  | JOSE BARBOSA FERNANDES | RO | 9 |  | 3424 | RUA ELIAS GORAYEB |  | LIBERDADE | 1100205    | PORTO VELHO | 76900000 | 1058 | BRASIL | 992662740 | 990709 | 10000000000571 | 195/55R16V 04PR K115 | 40111000 | 1600100 |  | 5405 | PC | 2.0 | 352.45 | 704.9 | 10000000000571 | PC | 2.0 | 352.45 | 0.0 | 0.0 | 66.9 | 0.0 | 1 |  | 60 | 0 | 0 | 0 | 1 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 |  | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 |  |  |  | 0 | 999 | 99 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 706.9 | 0.0 | 0.0 | 66.9 | 0.0 | 0.0 | 12.79 | 60.76 | 0.0 | 640.0 | 0.0 | 100 | v4.00 | S |  |  | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 |  |  |  |  |  | 0.0 | 0.0 | 0.0 | 0.0 | 60 |  | 2020-01-24 03:46:29 | 1 | 60.61 | 638.0 | 9.5 | 12.76 | 638.0 | 2.0 | 2020-01-23 09:28:00 | SIM |  |  | clienterover123@hotmail.com |  | 92662740 | N | NFe |

---

## dados/CNPJ/84654326000394/arquivos_parquet/bloco_h_84654326000394.parquet
- Caminho: dados/CNPJ/84654326000394/arquivos_parquet/bloco_h_84654326000394.parquet
- Tamanho: 48.7KB (49868 bytes)
- Linhas: 1493
- Colunas:
  - `cnpj`: String
  - `dt_inv`: Datetime(time_unit='us', time_zone=None)
  - `cod_mot_inv`: String
  - `mot_inv_desc`: String
  - `valor_total_inventario_h005`: Float64
  - `codigo_produto`: String
  - `codigo_fonte`: String
  - `descricao_produto`: String
  - `cod_ncm`: String
  - `cest`: String
  - `cod_barra`: String
  - `tipo_item`: String
  - `unidade_medida`: String
  - `quantidade`: Float64
  - `valor_unitario`: Float64
  - `valor_item`: Float64
  - `indicador_propriedade`: String
  - `participante_terceiro`: Null
  - `obs_complementar`: Null
  - `cst_icms`: Null
  - `bc_icms`: Null
  - `vl_icms`: Null

Amostra (até 3 linhas):

| cnpj | dt_inv | cod_mot_inv | mot_inv_desc | valor_total_inventario_h005 | codigo_produto | codigo_fonte | descricao_produto | cod_ncm | cest | cod_barra | tipo_item | unidade_medida | quantidade | valor_unitario | valor_item | indicador_propriedade | participante_terceiro | obs_complementar | cst_icms | bc_icms | vl_icms |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 84654326000394 | 2025-12-31 00:00:00 | 01 | 01 - No final do período | 354535.03 | 000101 | 84654326000394\|000101 | LIND'AGUA AGUA MINERAL GARRAFA 20L RETIRADA | 22011000 |  | 7897569700198 | 00 | UN | 8.0 | 0.0 | 0.0 | 0 |  |  |  |  |  |
| 84654326000394 | 2025-12-31 00:00:00 | 01 | 01 - No final do período | 354535.03 | 1 | 84654326000394\|1 | GASOLINA C COMUM | 27101259 | 0600200 |  | 07 | L | 144.052 | 0.0 | 0.0 | 0 |  |  |  |  |  |
| 84654326000394 | 2025-12-31 00:00:00 | 01 | 01 - No final do período | 354535.03 | 100004 | 84654326000394\|100004 | CAMARA 6.00-9 TR135 | 40139000 | 1600800 |  | 00 | PC | 1.0 | 23.40135 | 23.4 | 0 |  |  |  |  |  |

---

## dados/CNPJ/84654326000394/arquivos_parquet/c170_84654326000394.parquet
- Caminho: dados/CNPJ/84654326000394/arquivos_parquet/c170_84654326000394.parquet
- Tamanho: 162.0KB (165924 bytes)
- Linhas: 3416
- Colunas:
  - `periodo_efd`: String
  - `chv_nfe`: String
  - `codigo_fonte`: String
  - `cod_sit`: String
  - `cod_sit_desc`: String
  - `ind_emit`: String
  - `ind_emit_desc`: String
  - `ind_oper`: String
  - `ind_oper_desc`: String
  - `num_doc`: String
  - `ser`: String
  - `dt_doc`: Datetime(time_unit='us', time_zone=None)
  - `dt_e_s`: Datetime(time_unit='us', time_zone=None)
  - `num_item`: String
  - `cod_item`: String
  - `cod_barra`: String
  - `cod_ncm`: String
  - `cest`: String
  - `tipo_item`: String
  - `descr_item`: String
  - `descr_compl`: String
  - `cfop`: String
  - `cst_icms`: String
  - `qtd`: Float64
  - `unid`: String
  - `vl_item`: Float64
  - `vl_desc`: Float64
  - `vl_icms`: Float64
  - `vl_bc_icms`: Float64
  - `aliq_icms`: Float64
  - `vl_bc_icms_st`: Int64
  - `vl_icms_st`: Int64
  - `aliq_st`: Float64

Amostra (até 3 linhas):

| periodo_efd | chv_nfe | codigo_fonte | cod_sit | cod_sit_desc | ind_emit | ind_emit_desc | ind_oper | ind_oper_desc | num_doc | ser | dt_doc | dt_e_s | num_item | cod_item | cod_barra | cod_ncm | cest | tipo_item | descr_item | descr_compl | cfop | cst_icms | qtd | unid | vl_item | vl_desc | vl_icms | vl_bc_icms | aliq_icms | vl_bc_icms_st | vl_icms_st | aliq_st |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 2020/01 |  | 84654326000394\|O00034 | 00 | Documento regular | 1 | 1 - Terceiros | 0 | 0 - Entrada | 10320 |  | 2019-12-16 00:00:00 | 2020-01-02 00:00:00 | 1 | O00034 |  |  |  | 99 | SERVICO DE MONITORAMENTO E ALARME |  | 1933 | 090 | 1.0 | UN | 343.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0 | 0 | 0.0 |
| 2020/01 |  | 84654326000394\|O00034 | 00 | Documento regular | 1 | 1 - Terceiros | 0 | 0 - Entrada | 10476 |  | 2020-01-14 00:00:00 | 2020-01-14 00:00:00 | 1 | O00034 |  |  |  | 99 | SERVICO DE MONITORAMENTO E ALARME |  | 1933 | 090 | 1.0 | UN | 343.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0 | 0 | 0.0 |
| 2020/01 |  | 84654326000394\|O015 | 00 | Documento regular | 1 | 1 - Terceiros | 0 | 0 - Entrada | 1161 |  | 2020-01-09 00:00:00 | 2020-01-09 00:00:00 | 1 | O015 |  |  |  | 99 | SERVICOS DIVERSOS |  | 1933 | 090 | 1.0 | UN | 40.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0 | 0 | 0.0 |

---

## dados/CNPJ/84654326000394/arquivos_parquet/c176_84654326000394.parquet
- Caminho: dados/CNPJ/84654326000394/arquivos_parquet/c176_84654326000394.parquet
- Tamanho: 16.2KB (16554 bytes)
- Linhas: 91
- Colunas:
  - `periodo_efd`: String
  - `data_entrega_efd_periodo`: Datetime(time_unit='us', time_zone=None)
  - `cod_fin_efd`: String
  - `finalidade_efd`: String
  - `chave_saida`: String
  - `num_nf_saida`: String
  - `dt_doc_saida`: String
  - `dt_e_s_saida`: String
  - `cod_item`: String
  - `descricao_item`: Null
  - `num_item_saida`: String
  - `cfop_saida`: String
  - `unid_saida`: String
  - `qtd_item_saida`: Float64
  - `vl_total_item`: Float64
  - `cod_mot_res`: String
  - `descricao_motivo_ressarcimento`: String
  - `chave_nfe_ultima_entrada`: String
  - `c176_num_item_ult_e_declarado`: String
  - `dt_ultima_entrada`: Datetime(time_unit='us', time_zone=None)
  - `vl_unit_bc_st_entrada`: Float64
  - `vl_unit_icms_proprio_entrada`: Float64
  - `vl_unit_ressarcimento_st`: Float64
  - `vl_ressarc_credito_proprio`: Float64
  - `vl_ressarc_st_retido`: Float64
  - `vr_total_ressarcimento`: Float64

Amostra (até 3 linhas):

| periodo_efd | data_entrega_efd_periodo | cod_fin_efd | finalidade_efd | chave_saida | num_nf_saida | dt_doc_saida | dt_e_s_saida | cod_item | descricao_item | num_item_saida | cfop_saida | unid_saida | qtd_item_saida | vl_total_item | cod_mot_res | descricao_motivo_ressarcimento | chave_nfe_ultima_entrada | c176_num_item_ult_e_declarado | dt_ultima_entrada | vl_unit_bc_st_entrada | vl_unit_icms_proprio_entrada | vl_unit_ressarcimento_st | vl_ressarc_credito_proprio | vl_ressarc_st_retido | vr_total_ressarcimento |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 2021/05 | 2024-11-01 15:34:04 | 1 | 1 - Substituto | 11210584654326000394550010000494641136124129 | 49464 | 11052021 | 11052021 | 990601 |  | 97 | 5927 | 1 | 33.0 | 9991.99 | 3 | 3 - Perda ou deterioracao | 11200884654326000122550010000632181113710846 | 8 | 2020-08-19 00:00:00 | 201.771 | 49.361 | 20.73 | 1628.913 | 684.09 | 684.09 |
| 2021/05 | 2024-11-01 15:34:04 | 1 | 1 - Substituto | 11210584654326000394550010000494641136124129 | 49464 | 11052021 | 11052021 | 990614 |  | 98 | 5927 | 10 | 9.0 | 4443.75 | 3 | 3 - Perda ou deterioracao | 11200884654326000122550010000632181113710846 | 10 | 2020-08-19 00:00:00 | 329.023 | 80.49 | 33.806 | 724.41 | 304.254 | 304.254 |
| 2021/05 | 2024-11-01 15:34:04 | 1 | 1 - Substituto | 11210584654326000394550010000494641136124129 | 49464 | 11052021 | 11052021 | 990644 |  | 102 | 5927 | 1 | 8.0 | 3287.27 | 3 | 3 - Perda ou deterioracao | 11200784654326000122550010000629431115382660 | 2 | 2020-07-13 00:00:00 | 273.858 | 66.985 | 28.135 | 535.88 | 225.08 | 225.08 |

---

## dados/CNPJ/84654326000394/arquivos_parquet/dados_cadastrais_84654326000394.parquet
- Caminho: dados/CNPJ/84654326000394/arquivos_parquet/dados_cadastrais_84654326000394.parquet
- Tamanho: 1.4KB (1387 bytes)
- Linhas: 0
- Colunas:
  - `cnpj`: Null
  - `ie`: Null
  - `nome`: Null
  - `nome fantasia`: Null
  - `endereço`: Null
  - `município`: Null
  - `uf`: Null
  - `regime de pagamento`: Null
  - `situação da ie`: Null
  - `data de início da atividade`: Null
  - `data da última situação`: Null
  - `período em atividade`: Null
  - `redesim`: Null

---

## dados/CNPJ/84654326000394/arquivos_parquet/nfce_84654326000394.parquet
- Caminho: dados/CNPJ/84654326000394/arquivos_parquet/nfce_84654326000394.parquet
- Tamanho: 61.9KB (63409 bytes)
- Linhas: 62
- Colunas:
  - `tipo_operacao`: String
  - `co_destinatario`: String
  - `co_emitente`: String
  - `cnpj_filtro`: String
  - `nsu`: Int64
  - `chave_acesso`: String
  - `prod_nitem`: Int64
  - `codigo_fonte`: String
  - `ide_co_cuf`: Int64
  - `ide_co_indpag`: Null
  - `ide_co_mod`: Int64
  - `ide_serie`: String
  - `nnf`: Int64
  - `dhemi`: Datetime(time_unit='us', time_zone=None)
  - `co_tp_nf`: Int64
  - `co_iddest`: Int64
  - `co_cmun_fg`: String
  - `co_tpemis`: Int64
  - `co_finnfe`: Int64
  - `co_indpres`: Int64
  - `co_indfinal`: Int64
  - `xnome_emit`: String
  - `xfant_emit`: String
  - `co_uf_emit`: String
  - `co_cad_icms_emit`: String
  - `co_crt`: Int64
  - `xlgr_emit`: String
  - `nro_emit`: String
  - `xcpl_emit`: String
  - `xbairro_emit`: String
  - `co_cmun_emit`: String
  - `xmun_emit`: String
  - `cep_emit`: String
  - `cpais_emit`: String
  - `xpais_emit`: String
  - `fone_emit`: String
  - `xnome_dest`: String
  - `co_uf_dest`: String
  - `co_indiedest`: String
  - `xlgr_dest`: String
  - `nro_dest`: String
  - `xcpl_dest`: String
  - `xbairro_dest`: String
  - `co_cmun_dest`: String
  - `xmun_dest`: String
  - `cep_dest`: String
  - `cpais_dest`: String
  - `xpais_dest`: String
  - `fone_dest`: String
  - `prod_cprod`: String
  - `prod_cean`: String
  - `prod_xprod`: String
  - `prod_ncm`: String
  - `co_cfop`: Int64
  - `prod_ucom`: String
  - `prod_qcom`: Float64
  - `prod_vuncom`: Float64
  - `prod_vprod`: Float64
  - `prod_ceantrib`: String
  - `prod_utrib`: String
  - `prod_qtrib`: Float64
  - `prod_vuntrib`: Float64
  - `prod_vfrete`: Float64
  - `prod_vseg`: Float64
  - `prod_vdesc`: Float64
  - `prod_voutro`: Float64
  - `prod_indtot`: Int64
  - `icms_csosn`: Int64
  - `icms_cst`: Int64
  - `icms_modbc`: Int64
  - `icms_modbcst`: Int64
  - `icms_motdesicms`: Int64
  - `icms_orig`: Int64
  - `icms_pbcop`: Float64
  - `icms_pcredsn`: Float64
  - `icms_pdif`: Float64
  - `icms_picms`: Float64
  - `icms_picmsst`: Float64
  - `icms_pmvast`: Float64
  - `icms_predbc`: Float64
  - `icms_predbcst`: Float64
  - `icms_ufst`: Null
  - `icms_vbc`: Float64
  - `icms_vbcst`: Float64
  - `icms_vbcstdest`: Float64
  - `icms_vbcstret`: Float64
  - `icms_vcredicmssn`: Float64
  - `icms_vicms`: Float64
  - `icms_vicmsdeson`: Float64
  - `icms_vicmsdif`: Float64
  - `icms_vicmsop`: Float64
  - `icms_vicmsst`: Float64
  - `icms_vicmsstdest`: Float64
  - `icms_vicmsstret`: Float64
  - `icms_vbcfcp`: Float64
  - `icms_pfcp`: Float64
  - `icms_vfcp`: Float64
  - `icms_vbcfcpst`: Float64
  - `icms_pfcpst`: Float64
  - `icms_vfcpst`: Float64
  - `icms_vbcufdest`: Float64
  - `icms_vbcfcpufdest`: Float64
  - `icms_pfcpufdest`: Float64
  - `icms_picmsufdest`: Float64
  - `icms_picmsinter`: Float64
  - `icms_picmsinterpart`: Float64
  - `icms_vfcpufdest`: Float64
  - `icms_vicmsufdest`: Float64
  - `icms_vicmsufremet`: Float64
  - `icms_pst`: Float64
  - `icms_vbcfcpstret`: Float64
  - `icms_pfcpstret`: Float64
  - `icms_vfcpstret`: Float64
  - `icms_predbcefet`: Float64
  - `icms_vbcefet`: Float64
  - `icms_picmsefet`: Float64
  - `icms_vicmsefet`: Float64
  - `tot_vbc`: Float64
  - `tot_vicms`: Float64
  - `tot_vicmsdeson`: Float64
  - `tot_vbcst`: Float64
  - `tot_vst`: Float64
  - `tot_vprod`: Float64
  - `tot_vfrete`: Float64
  - `tot_vseg`: Float64
  - `tot_vdesc`: Float64
  - `tot_vii`: Float64
  - `tot_vipi`: Float64
  - `tot_vpis`: Float64
  - `tot_vcofins`: Float64
  - `tot_voutro`: Float64
  - `tot_vnf`: Float64
  - `tot_vtottrib`: Float64
  - `tot_vfcpufdest`: Float64
  - `tot_vicmsufdest`: Float64
  - `tot_vicmsufremet`: Float64
  - `tot_vfcp`: Float64
  - `tot_vfcpst`: Float64
  - `tot_vfcpstret`: Float64
  - `tot_vipidevol`: Float64
  - `infprot_cstat`: Int64
  - `icms_csosn_a`: String
  - `icms_cst_a`: String
  - `dt_gravacao`: Datetime(time_unit='us', time_zone=None)
  - `seq_nitem`: Int64
  - `dhemi_hora`: Datetime(time_unit='us', time_zone=None)
  - `status_carga_campo_fcp`: String
  - `prod_cest`: String

Amostra (até 3 linhas):

| tipo_operacao | co_destinatario | co_emitente | cnpj_filtro | nsu | chave_acesso | prod_nitem | codigo_fonte | ide_co_cuf | ide_co_indpag | ide_co_mod | ide_serie | nnf | dhemi | co_tp_nf | co_iddest | co_cmun_fg | co_tpemis | co_finnfe | co_indpres | co_indfinal | xnome_emit | xfant_emit | co_uf_emit | co_cad_icms_emit | co_crt | xlgr_emit | nro_emit | xcpl_emit | xbairro_emit | co_cmun_emit | xmun_emit | cep_emit | cpais_emit | xpais_emit | fone_emit | xnome_dest | co_uf_dest | co_indiedest | xlgr_dest | nro_dest | xcpl_dest | xbairro_dest | co_cmun_dest | xmun_dest | cep_dest | cpais_dest | xpais_dest | fone_dest | prod_cprod | prod_cean | prod_xprod | prod_ncm | co_cfop | prod_ucom | prod_qcom | prod_vuncom | prod_vprod | prod_ceantrib | prod_utrib | prod_qtrib | prod_vuntrib | prod_vfrete | prod_vseg | prod_vdesc | prod_voutro | prod_indtot | icms_csosn | icms_cst | icms_modbc | icms_modbcst | icms_motdesicms | icms_orig | icms_pbcop | icms_pcredsn | icms_pdif | icms_picms | icms_picmsst | icms_pmvast | icms_predbc | icms_predbcst | icms_ufst | icms_vbc | icms_vbcst | icms_vbcstdest | icms_vbcstret | icms_vcredicmssn | icms_vicms | icms_vicmsdeson | icms_vicmsdif | icms_vicmsop | icms_vicmsst | icms_vicmsstdest | icms_vicmsstret | icms_vbcfcp | icms_pfcp | icms_vfcp | icms_vbcfcpst | icms_pfcpst | icms_vfcpst | icms_vbcufdest | icms_vbcfcpufdest | icms_pfcpufdest | icms_picmsufdest | icms_picmsinter | icms_picmsinterpart | icms_vfcpufdest | icms_vicmsufdest | icms_vicmsufremet | icms_pst | icms_vbcfcpstret | icms_pfcpstret | icms_vfcpstret | icms_predbcefet | icms_vbcefet | icms_picmsefet | icms_vicmsefet | tot_vbc | tot_vicms | tot_vicmsdeson | tot_vbcst | tot_vst | tot_vprod | tot_vfrete | tot_vseg | tot_vdesc | tot_vii | tot_vipi | tot_vpis | tot_vcofins | tot_voutro | tot_vnf | tot_vtottrib | tot_vfcpufdest | tot_vicmsufdest | tot_vicmsufremet | tot_vfcp | tot_vfcpst | tot_vfcpstret | tot_vipidevol | infprot_cstat | icms_csosn_a | icms_cst_a | dt_gravacao | seq_nitem | dhemi_hora | status_carga_campo_fcp | prod_cest |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 0 - ENTRADA | 84654326000394 | 25141379000180 | 84654326000394 | 729144618 | 11200325141379000180650010000877261001283982 | 1 | 25141379000180\|20829 | 11 |  | 65 | 1   | 87726 | 2020-03-09 00:00:00 | 1 | 1 | 1100205    | 1 | 1 | 1 | 1 | PVH FERRAGENS E FERRAMENTAS LTDA - ME | DISAGUA PVH | RO | 00000004601971 | 3 | RUA DA BEIRA | 6461 |  | LAGOA | 1100205    | PORTO VELHO | 76812003 | 1058 | BRASIL | 6932225000 | CHARLENE PNEUS LTDA | RO | 9 | AVENIDA NACOES UNIDAS | 001608 |  | ROQUE | 1100205    | PORTO VELHO | 76804436 | 1058 | BRASIL |  | 20829 | 7898400963567 | CORRENTE N.06.0MM - 15/64 GALVANIZADA ELO LONGO | 73158200 | 5102 | KG | 50.0 | 19.5 | 975.0 | 7898400963567 | KG | 50.0 | 19.5 | 0.0 | 0.0 | 195.0 | 0.0 | 1 | 0 | 0 | 0 | 0 | 0 | 0 | 0.0 | 0.0 | 0.0 | 17.5 | 0.0 | 0.0 | 0.0 | 0.0 |  | 780.0 | 0.0 | 0.0 | 0.0 | 0.0 | 136.5 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 780.0 | 136.5 | 0.0 | 0.0 | 0.0 | 975.0 | 0.0 | 0.0 | 195.0 | 0.0 | 0.0 | 5.07 | 23.4 | 0.0 | 780.0 | 301.63 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 100 | 0 | 0 | 2020-03-09 11:35:10 | 1 | 2020-03-09 11:33:56 | SIM |  |
| 0 - ENTRADA | 84654326000394 | 18780053000138 | 84654326000394 | 738880327 | 11200318780053000138650020000556401000556413 | 1 | 18780053000138\|1 | 11 |  | 65 | 2   | 55640 | 2020-03-30 00:00:00 | 1 | 1 | 1100205    | 1 | 1 | 1 | 1 | M L SOARES COMERCIO DE ALIMENTOS LTDA - ME | MERCADO DELL | RO | 00000004154002 | 1 | R DA LUA | 410 | CENTRO COMERCIAL | FLORESTA | 1100205    | PORTO VELHO | 76806420 | 1058 | BRASIL |  | CHARLEN PNEUS LTDA | RO | 9 | AV. NACOES UNIDAS | 1608 C | ROVER PNEUS (HANKOOK) | ROQUE | 1100205    | PORTO VELHO | 76804436 | 1058 | BRASIL | 06932244030 | 1 | SEM GTIN | DIVERSOS 1 | 17049020 | 5102 | UN | 1.0 | 30.0 | 30.0 | SEM GTIN | UN | 1.0 | 30.0 | 0.0 | 0.0 | 0.0 | 0.0 | 1 | 102 | 0 | 0 | 0 | 0 | 0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 |  | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 30.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 30.0 | 7.34 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 100 | 102 | 0 | 2020-03-30 14:59:05 | 1 | 2020-03-30 14:57:49 | SIM | 0000000 |
| 0 - ENTRADA | 84654326000394 | 02861668000159 | 84654326000394 | 842151198 | 11201002861668000159651000001478441993499521 | 1 | 02861668000159\|5176 | 11 |  | 65 | 100 | 147844 | 2020-10-30 00:00:00 | 1 | 1 | 1100205    | 1 | 1 | 1 | 1 | MORAES COMERCIO DE TECIDOS EIRELI -EPP | MORAES COMERCIO DE TECIDOS EIRELI - | RO | 00000000919225 | 2 | AV. 7 DE SETEMBRO | 001010 |  | CENTRO | 1100205    | PORTO VELHO | 78916000 | 1058 | BRASIL | 6933246529 |  |  | 9 |  |  |  |  |  |  |  |  |  |  | 5176 | SEM GTIN | TAPETE VINIL CAPACHO 1,20LARG - KOMLOG------------ | 39181000 | 5102 | M | 1.2 | 149.99 | 179.99 | SEM GTIN | M | 1.2 | 149.99 | 0.0 | 0.0 | 0.0 | 0.0 | 1 | 0 | 0 | 3 | 0 | 0 | 1 | 0.0 | 0.0 | 0.0 | 17.5 | 0.0 | 0.0 | 0.0 | 0.0 |  | 179.99 | 0.0 | 0.0 | 0.0 | 0.0 | 31.5 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 179.99 | 31.5 | 0.0 | 0.0 | 0.0 | 179.99 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 179.99 | 58.8 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 100 | 0 | 0 | 2020-10-30 18:05:13 | 1 | 2020-10-30 16:57:35 |  |  |

---

## dados/CNPJ/84654326000394/arquivos_parquet/nfe_84654326000394.parquet
- Caminho: dados/CNPJ/84654326000394/arquivos_parquet/nfe_84654326000394.parquet
- Tamanho: 3.4MB (3514074 bytes)
- Linhas: 24090
- Colunas:
  - `tipo_operacao`: String
  - `co_destinatario`: String
  - `co_emitente`: String
  - `cnpj_filtro`: String
  - `nsu`: Int64
  - `chave_acesso`: String
  - `prod_nitem`: Int64
  - `codigo_fonte`: String
  - `ide_co_cuf`: Int64
  - `ide_co_indpag`: Null
  - `ide_co_mod`: Int64
  - `ide_serie`: String
  - `nnf`: Int64
  - `dhemi`: Datetime(time_unit='us', time_zone=None)
  - `dhsaient`: Datetime(time_unit='us', time_zone=None)
  - `co_tp_nf`: Int64
  - `co_iddest`: Int64
  - `co_cmun_fg`: String
  - `co_tpemis`: Int64
  - `co_finnfe`: Int64
  - `co_indfinal`: Int64
  - `co_indpres`: Int64
  - `xnome_emit`: String
  - `xfant_emit`: String
  - `co_uf_emit`: String
  - `co_cad_icms_emit`: String
  - `co_cad_icms_st`: String
  - `co_crt`: Int64
  - `xlgr_emit`: String
  - `nro_emit`: String
  - `xcpl_emit`: String
  - `xbairro_emit`: String
  - `co_cmun_emit`: String
  - `xmun_emit`: String
  - `cep_emit`: String
  - `cpais_emit`: String
  - `fone_emit`: String
  - `xpais_emit`: String
  - `cnae_emit`: String
  - `xnome_dest`: String
  - `co_uf_dest`: String
  - `co_indiedest`: String
  - `co_cad_icms_dest`: String
  - `nro_dest`: String
  - `xlgr_dest`: String
  - `xcpl_dest`: String
  - `xbairro_dest`: String
  - `co_cmun_dest`: String
  - `xmun_dest`: String
  - `cep_dest`: String
  - `cpais_dest`: String
  - `xpais_dest`: String
  - `fone_dest`: String
  - `prod_cprod`: String
  - `prod_cean`: String
  - `prod_xprod`: String
  - `prod_ncm`: String
  - `prod_cest`: String
  - `prod_extipi`: String
  - `co_cfop`: Int64
  - `prod_ucom`: String
  - `prod_qcom`: Float64
  - `prod_vuncom`: Float64
  - `prod_vprod`: Float64
  - `prod_ceantrib`: String
  - `prod_utrib`: String
  - `prod_qtrib`: Float64
  - `prod_vuntrib`: Float64
  - `prod_vfrete`: Float64
  - `prod_vseg`: Float64
  - `prod_vdesc`: Float64
  - `prod_voutro`: Float64
  - `prod_indtot`: Int64
  - `icms_csosn`: Int64
  - `icms_cst`: Int64
  - `icms_modbc`: Int64
  - `icms_modbcst`: Int64
  - `icms_motdesicms`: Int64
  - `icms_orig`: Int64
  - `icms_pbcop`: Float64
  - `icms_pcredsn`: Float64
  - `icms_pdif`: Float64
  - `icms_picms`: Float64
  - `icms_picmsst`: Float64
  - `icms_pmvast`: Float64
  - `icms_predbc`: Float64
  - `icms_predbcst`: Float64
  - `icms_ufst`: Null
  - `icms_vbc`: Float64
  - `icms_vbcst`: Float64
  - `icms_vbcstdest`: Float64
  - `icms_vbcstret`: Float64
  - `icms_vcredicmssn`: Float64
  - `icms_vicms`: Float64
  - `icms_vicmsdeson`: Float64
  - `icms_vicmsdif`: Float64
  - `icms_vicmsop`: Float64
  - `icms_vicmsst`: Float64
  - `icms_vicmsstdest`: Float64
  - `icms_vicmsstret`: Float64
  - `ipi_clenq`: Null
  - `ipi_cnpjprod`: Null
  - `ipi_cselo`: Null
  - `ipi_qselo`: Int64
  - `ipi_cenq`: String
  - `ipi_cst`: Int64
  - `ipi_vbc`: Float64
  - `ipi_pipi`: Float64
  - `ipi_qunid`: Float64
  - `ipi_vunid`: Float64
  - `ipi_vipi`: Float64
  - `ii_vbc`: Float64
  - `ii_vdespadu`: Float64
  - `ii_vii`: Float64
  - `ii_viof`: Float64
  - `veic_prod_tpop`: Null
  - `veic_prod_chassi`: Null
  - `veic_prod_ccor`: Null
  - `veic_prod_xcor`: Null
  - `veic_prod_pot`: Null
  - `veic_prod_cilin`: Null
  - `veic_prod_pesol`: Null
  - `veic_prod_pesob`: Null
  - `veic_prod_nserie`: Null
  - `veic_prod_tpcomb`: Null
  - `veic_prod_nmotor`: Null
  - `veic_prod_cmt`: Null
  - `veic_prod_anomod`: Null
  - `veic_prod_anofab`: Null
  - `veic_prod_dist`: Null
  - `veic_prod_tppint`: Null
  - `veic_prod_tpveic`: Null
  - `veic_prod_espveic`: Null
  - `veic_prod_vin`: Null
  - `veic_prod_condveic`: Null
  - `veic_prod_cmod`: Null
  - `veic_prod_ccordenatran`: Null
  - `veic_prod_lota`: Null
  - `veic_prod_tprest`: Null
  - `comb_cprodanp`: Int64
  - `comb_pmixgn`: Null
  - `comb_codif`: Null
  - `comb_qtemp`: Float64
  - `comb_ufcons`: String
  - `tot_vbc`: Float64
  - `tot_vicms`: Float64
  - `tot_vicmsdeson`: Float64
  - `tot_vbcst`: Float64
  - `tot_vst`: Float64
  - `tot_vprod`: Float64
  - `tot_vfrete`: Float64
  - `tot_vseg`: Float64
  - `tot_vdesc`: Float64
  - `tot_vii`: Float64
  - `tot_vipi`: Float64
  - `tot_vpis`: Float64
  - `tot_vcofins`: Float64
  - `tot_voutro`: Float64
  - `tot_vnf`: Float64
  - `tot_vtottrib`: Float64
  - `infprot_cstat`: Int64
  - `versao`: String
  - `prod_indescala`: String
  - `prod_cnpjfab`: Null
  - `prod_cbenef`: Null
  - `icms_vbcfcp`: Float64
  - `icms_pfcp`: Float64
  - `icms_vfcp`: Float64
  - `icms_vbcfcpst`: Float64
  - `icms_pfcpst`: Float64
  - `icms_vfcpst`: Float64
  - `icms_vbcufdest`: Float64
  - `icms_vbcfcpufdest`: Float64
  - `icms_pfcpufdest`: Float64
  - `icms_picmsufdest`: Float64
  - `icms_picmsinter`: Float64
  - `icms_picmsinterpart`: Float64
  - `icms_vfcpufdest`: Float64
  - `icms_vicmsufdest`: Float64
  - `icms_vicmsufremet`: Float64
  - `icms_pst`: Float64
  - `icms_vbcfcpstret`: Float64
  - `icms_pfcpstret`: Float64
  - `icms_vfcpstret`: Float64
  - `icms_predbcefet`: Float64
  - `icms_vbcefet`: Float64
  - `icms_picmsefet`: Float64
  - `icms_vicmsefet`: Float64
  - `med_cprodanvisa`: Null
  - `med_vpmc`: Null
  - `tot_vfcpufdest`: Float64
  - `tot_vicmsufdest`: Float64
  - `tot_vicmsufremet`: Float64
  - `tot_vfcp`: Float64
  - `tot_vfcpst`: Float64
  - `tot_vfcpstret`: Float64
  - `tot_vipidevol`: Float64
  - `icms_cst_a`: String
  - `icms_csosn_a`: String
  - `dt_gravacao`: Datetime(time_unit='us', time_zone=None)
  - `seq_nitem`: Int64
  - `cofins_vcofins`: Float64
  - `cofins_vbc`: Float64
  - `cofins_pcofins`: Float64
  - `pis_vpis`: Float64
  - `pis_vbc`: Float64
  - `pis_ppis`: Float64
  - `dhemi_hora`: Datetime(time_unit='us', time_zone=None)
  - `status_carga_campo_fcp`: String
  - `status_carga_campo_rem_dest`: String
  - `in_versao`: String
  - `email_dest`: String
  - `co_indiedest_`: Int64
  - `fone_dest_a8`: String
  - `ibscbs`: String

Amostra (até 3 linhas):

| tipo_operacao | co_destinatario | co_emitente | cnpj_filtro | nsu | chave_acesso | prod_nitem | codigo_fonte | ide_co_cuf | ide_co_indpag | ide_co_mod | ide_serie | nnf | dhemi | dhsaient | co_tp_nf | co_iddest | co_cmun_fg | co_tpemis | co_finnfe | co_indfinal | co_indpres | xnome_emit | xfant_emit | co_uf_emit | co_cad_icms_emit | co_cad_icms_st | co_crt | xlgr_emit | nro_emit | xcpl_emit | xbairro_emit | co_cmun_emit | xmun_emit | cep_emit | cpais_emit | fone_emit | xpais_emit | cnae_emit | xnome_dest | co_uf_dest | co_indiedest | co_cad_icms_dest | nro_dest | xlgr_dest | xcpl_dest | xbairro_dest | co_cmun_dest | xmun_dest | cep_dest | cpais_dest | xpais_dest | fone_dest | prod_cprod | prod_cean | prod_xprod | prod_ncm | prod_cest | prod_extipi | co_cfop | prod_ucom | prod_qcom | prod_vuncom | prod_vprod | prod_ceantrib | prod_utrib | prod_qtrib | prod_vuntrib | prod_vfrete | prod_vseg | prod_vdesc | prod_voutro | prod_indtot | icms_csosn | icms_cst | icms_modbc | icms_modbcst | icms_motdesicms | icms_orig | icms_pbcop | icms_pcredsn | icms_pdif | icms_picms | icms_picmsst | icms_pmvast | icms_predbc | icms_predbcst | icms_ufst | icms_vbc | icms_vbcst | icms_vbcstdest | icms_vbcstret | icms_vcredicmssn | icms_vicms | icms_vicmsdeson | icms_vicmsdif | icms_vicmsop | icms_vicmsst | icms_vicmsstdest | icms_vicmsstret | ipi_clenq | ipi_cnpjprod | ipi_cselo | ipi_qselo | ipi_cenq | ipi_cst | ipi_vbc | ipi_pipi | ipi_qunid | ipi_vunid | ipi_vipi | ii_vbc | ii_vdespadu | ii_vii | ii_viof | veic_prod_tpop | veic_prod_chassi | veic_prod_ccor | veic_prod_xcor | veic_prod_pot | veic_prod_cilin | veic_prod_pesol | veic_prod_pesob | veic_prod_nserie | veic_prod_tpcomb | veic_prod_nmotor | veic_prod_cmt | veic_prod_anomod | veic_prod_anofab | veic_prod_dist | veic_prod_tppint | veic_prod_tpveic | veic_prod_espveic | veic_prod_vin | veic_prod_condveic | veic_prod_cmod | veic_prod_ccordenatran | veic_prod_lota | veic_prod_tprest | comb_cprodanp | comb_pmixgn | comb_codif | comb_qtemp | comb_ufcons | tot_vbc | tot_vicms | tot_vicmsdeson | tot_vbcst | tot_vst | tot_vprod | tot_vfrete | tot_vseg | tot_vdesc | tot_vii | tot_vipi | tot_vpis | tot_vcofins | tot_voutro | tot_vnf | tot_vtottrib | infprot_cstat | versao | prod_indescala | prod_cnpjfab | prod_cbenef | icms_vbcfcp | icms_pfcp | icms_vfcp | icms_vbcfcpst | icms_pfcpst | icms_vfcpst | icms_vbcufdest | icms_vbcfcpufdest | icms_pfcpufdest | icms_picmsufdest | icms_picmsinter | icms_picmsinterpart | icms_vfcpufdest | icms_vicmsufdest | icms_vicmsufremet | icms_pst | icms_vbcfcpstret | icms_pfcpstret | icms_vfcpstret | icms_predbcefet | icms_vbcefet | icms_picmsefet | icms_vicmsefet | med_cprodanvisa | med_vpmc | tot_vfcpufdest | tot_vicmsufdest | tot_vicmsufremet | tot_vfcp | tot_vfcpst | tot_vfcpstret | tot_vipidevol | icms_cst_a | icms_csosn_a | dt_gravacao | seq_nitem | cofins_vcofins | cofins_vbc | cofins_pcofins | pis_vpis | pis_vbc | pis_ppis | dhemi_hora | status_carga_campo_fcp | status_carga_campo_rem_dest | in_versao | email_dest | co_indiedest_ | fone_dest_a8 | ibscbs |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 1 - SAIDA | 02718182000166 | 84654326000394 | 84654326000394 | 322563916 | 11200184654326000394550010000443061113991391 | 1 | 84654326000394\|109840 | 11 |  | 55 | 1   | 44306 | 2020-01-23 00:00:00 | 2020-01-23 00:00:00 | 1 | 1 | 1100205    | 1 | 1 | 0 | 1 | CHARLENE PNEUS LTDA | CHARLENE PVH | RO | 00000003162583 |  | 3 | AV NACOES UNIDAS | 1608 |  | ROQUE | 1100205    | PORTO VELHO | 76804436 | 1058 | 6932244030 | BRASIL |  | ALMIRANTE RENT A CAR L.V. LTDA ME | RO | 1 | 00000001466321 | 2065 | RUA ALMIRANTE BARROSO |  | NOSSA SENHORA DAS GRACAS | 1100205    | PORTO VELHO | 76804129 | 1058 | BRASIL | 69993217582 | 109840 | SEM GTIN | VALV PN S/C 414 | 84818099 | 1007900 |  | 5405 | PC | 1.0 | 1.0 | 1.0 | SEM GTIN | PC | 1.0 | 1.0 | 0.0 | 0.0 | 0.0 | 0.0 | 1 |  | 60 | 0 | 0 | 0 | 0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 |  | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 |  |  |  | 0 | 999 | 99 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 235.17 | 0.0 | 0.0 | 40.17 | 0.0 | 0.0 | 3.9 | 18.51 | 0.0 | 195.0 | 0.0 | 100 | v4.00 | S |  |  | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 |  |  |  |  |  | 0.0 | 0.0 | 0.0 | 0.0 | 60 |  | 2020-01-24 03:50:44 | 1 | 0.08 | 1.0 | 7.6 | 0.02 | 1.0 | 1.65 | 2020-01-23 09:44:00 | SIM |  |  | clienterover123@hotmail.com.br |  | 93217582 | N |
| 1 - SAIDA | 02718182000166 | 84654326000394 | 84654326000394 | 322563916 | 11200184654326000394550010000443061113991391 | 2 | 84654326000394\|990872 | 11 |  | 55 | 1   | 44306 | 2020-01-23 00:00:00 | 2020-01-23 00:00:00 | 1 | 1 | 1100205    | 1 | 1 | 0 | 1 | CHARLENE PNEUS LTDA | CHARLENE PVH | RO | 00000003162583 |  | 3 | AV NACOES UNIDAS | 1608 |  | ROQUE | 1100205    | PORTO VELHO | 76804436 | 1058 | 6932244030 | BRASIL |  | ALMIRANTE RENT A CAR L.V. LTDA ME | RO | 1 | 00000001466321 | 2065 | RUA ALMIRANTE BARROSO |  | NOSSA SENHORA DAS GRACAS | 1100205    | PORTO VELHO | 76804129 | 1058 | BRASIL | 69993217582 | 990872 | SEM GTIN | 175/70R14T 04PR LH41 | 40111000 | 1600100 |  | 5405 | PC | 1.0 | 234.17 | 234.17 | SEM GTIN | PC | 1.0 | 234.17 | 0.0 | 0.0 | 40.17 | 0.0 | 1 |  | 60 | 0 | 0 | 0 | 1 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 |  | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 |  |  |  | 0 | 999 | 99 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 235.17 | 0.0 | 0.0 | 40.17 | 0.0 | 0.0 | 3.9 | 18.51 | 0.0 | 195.0 | 0.0 | 100 | v4.00 | S |  |  | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 |  |  |  |  |  | 0.0 | 0.0 | 0.0 | 0.0 | 60 |  | 2020-01-24 03:50:44 | 2 | 18.43 | 194.0 | 9.5 | 3.88 | 194.0 | 2.0 | 2020-01-23 09:44:00 | SIM |  |  |  |  | 93217582 | N |
| 1 - SAIDA | 07756151291 | 84654326000394 | 84654326000394 | 322558002 | 11200184654326000394550010000443051111719360 | 1 | 84654326000394\|990709 | 11 |  | 55 | 1   | 44305 | 2020-01-23 00:00:00 | 2020-01-23 00:00:00 | 1 | 1 | 1100205    | 1 | 1 | 1 | 1 | CHARLENE PNEUS LTDA | CHARLENE PVH | RO | 00000003162583 |  | 3 | AV NACOES UNIDAS | 1608 |  | ROQUE | 1100205    | PORTO VELHO | 76804436 | 1058 | 6932244030 | BRASIL |  | JOSE BARBOSA FERNANDES | RO | 9 |  | 3424 | RUA ELIAS GORAYEB |  | LIBERDADE | 1100205    | PORTO VELHO | 76900000 | 1058 | BRASIL | 992662740 | 990709 | 10000000000571 | 195/55R16V 04PR K115 | 40111000 | 1600100 |  | 5405 | PC | 2.0 | 352.45 | 704.9 | 10000000000571 | PC | 2.0 | 352.45 | 0.0 | 0.0 | 66.9 | 0.0 | 1 |  | 60 | 0 | 0 | 0 | 1 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 |  | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 |  |  |  | 0 | 999 | 99 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 706.9 | 0.0 | 0.0 | 66.9 | 0.0 | 0.0 | 12.79 | 60.76 | 0.0 | 640.0 | 0.0 | 100 | v4.00 | S |  |  | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 |  |  |  |  |  | 0.0 | 0.0 | 0.0 | 0.0 | 60 |  | 2020-01-24 03:46:29 | 1 | 60.61 | 638.0 | 9.5 | 12.76 | 638.0 | 2.0 | 2020-01-23 09:28:00 | SIM |  |  | clienterover123@hotmail.com |  | 92662740 | N |

---

