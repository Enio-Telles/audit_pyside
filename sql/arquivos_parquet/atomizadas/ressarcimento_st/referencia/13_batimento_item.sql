/* Gerado para integração com audit_react + C176 + Fronteira */
WITH ENTRADA_ESCOLHIDA AS (
    SELECT * FROM TMP_ENTRADA_ESCOLHIDA
),
ST_CALC_ATE_2022_TOTAL_ITEM AS (
    SELECT * FROM TMP_ST_CALC_ATE_2022_TOTAL_ITEM
),
FRONTEIRA_ITEM_SIMPLES AS (
    SELECT * FROM TMP_FRONTEIRA_ITEM_SIMPLES
),
FRONTEIRA_ITEM_COMPLETO AS (
    SELECT * FROM TMP_FRONTEIRA_ITEM_COMPLETO
),
FRONTEIRA_ITEM_ENRIQUECIDO AS (
    SELECT
        fs.*,
        fc.fronteira_dt_entrada,
        fc.fronteira_comando,
        fc.fronteira_qtd_guias,
        fc.fronteira_guia_exemplo,
        fc.fronteira_receita,
        fc.fronteira_valor_devido,
        fc.fronteira_valor_pago,
        fc.fronteira_situacao,
        fc.fronteira_co_sefin_lanc,
        fc.fronteira_nome_co_sefin,
        fc.fronteira_vl_merc_item,
        fc.fronteira_vl_bc_merc_item,
        fc.fronteira_aliq_item,
        fc.fronteira_vl_tot_debito_item,
        fc.fronteira_vl_credito_rateio,
        fc.fronteira_vl_icms_recolher,
        fc.fronteira_ind_produto_st,
        fc.fronteira_ind_cest_st,
        fc.fronteira_pc_interna_merc
    FROM FRONTEIRA_ITEM_SIMPLES fs
    LEFT JOIN FRONTEIRA_ITEM_COMPLETO fc
      ON fc.chave_saida = fs.chave_saida
     AND fc.num_item_saida = fs.num_item_saida
     AND fc.cod_item_saida = fs.cod_item_saida
     AND fc.chave_nfe_ultima_entrada = fs.chave_nfe_ultima_entrada
     AND fc.prod_nitem_entrada = fs.prod_nitem_entrada
)
SELECT
    e.*,
    calc.calc_st_total_item,
    CASE WHEN NVL(e.qtd_entrada_xml, 0) <> 0 THEN calc.calc_st_total_item / e.qtd_entrada_xml END AS vl_unit_st_calc_origem,
    CASE
        WHEN NVL(e.qtd_entrada_xml, 0) <> 0 AND NVL(e.fator_entrada, 1) <> 0
        THEN (calc.calc_st_total_item / e.qtd_entrada_xml) / e.fator_entrada
    END AS vl_unit_st_calc_unid_ref,
    fr.fronteira_co_sefin_calc,
    fr.fronteira_co_sefin_lanc,
    fr.fronteira_cod_rotina_calculo,
    fr.fronteira_valor_icms_total_item,
    fr.fronteira_qtd_comercial,
    fr.fronteira_dt_entrada,
    fr.fronteira_comando,
    fr.fronteira_qtd_guias,
    fr.fronteira_guia_exemplo,
    fr.fronteira_receita,
    fr.fronteira_valor_devido,
    fr.fronteira_valor_pago,
    fr.fronteira_situacao,
    fr.fronteira_nome_co_sefin,
    CASE WHEN NVL(fr.fronteira_qtd_comercial, 0) <> 0 THEN fr.fronteira_valor_icms_total_item / fr.fronteira_qtd_comercial END AS vl_unit_st_fronteira_origem,
    CASE
        WHEN NVL(fr.fronteira_qtd_comercial, 0) <> 0 AND NVL(e.fator_entrada, 1) <> 0
        THEN (fr.fronteira_valor_icms_total_item / fr.fronteira_qtd_comercial) / e.fator_entrada
    END AS vl_unit_st_fronteira_unid_ref,
    CASE WHEN NVL(e.qtd_saida_unid_ref, 0) <> 0 THEN NVL(e.vl_unit_st_decl_unid_ref, 0) * NVL(e.qtd_saida_unid_ref, 0) END AS vl_st_decl_total_considerado,
    CASE
        WHEN NVL(e.qtd_saida_unid_ref, 0) <> 0 AND NVL(e.qtd_entrada_xml, 0) <> 0 AND NVL(e.fator_entrada, 1) <> 0
        THEN ((calc.calc_st_total_item / e.qtd_entrada_xml) / e.fator_entrada) * e.qtd_saida_unid_ref
    END AS vl_st_calc_total_considerado,
    CASE
        WHEN NVL(e.qtd_saida_unid_ref, 0) <> 0 AND NVL(fr.fronteira_qtd_comercial, 0) <> 0 AND NVL(e.fator_entrada, 1) <> 0
        THEN ((fr.fronteira_valor_icms_total_item / fr.fronteira_qtd_comercial) / e.fator_entrada) * e.qtd_saida_unid_ref
    END AS vl_st_fronteira_total_considerado,
    CASE
        WHEN fr.fronteira_valor_icms_total_item IS NULL THEN 'SEM_FRONTEIRA'
        WHEN UPPER(TRIM(fr.fronteira_cod_rotina_calculo)) <> 'ST' THEN 'ROTINA_NAO_ST'
        WHEN NVL(fr.fronteira_qtd_comercial, 0) = 0 THEN 'FRONTEIRA_SEM_QTD'
        WHEN ABS(
            NVL(e.vl_unit_st_decl_unid_ref, 0) -
            NVL((fr.fronteira_valor_icms_total_item / fr.fronteira_qtd_comercial) / NULLIF(e.fator_entrada, 0), 0)
        ) <= 0.05 THEN 'OK_ST_FRONTEIRA_UNITARIO'
        ELSE 'DIVERG_ST_FRONTEIRA_UNITARIO'
    END AS status_st_fronteira_unitario
FROM ENTRADA_ESCOLHIDA e
LEFT JOIN ST_CALC_ATE_2022_TOTAL_ITEM calc
  ON calc.chave_saida = e.chave_saida
 AND calc.num_item_saida = e.num_item_saida
 AND calc.cod_item_saida = e.cod_item_saida
 AND calc.chave_nfe_ultima_entrada = e.chave_nfe_ultima_entrada
 AND calc.prod_nitem_entrada = e.prod_nitem_entrada
LEFT JOIN FRONTEIRA_ITEM_ENRIQUECIDO fr
  ON fr.chave_saida = e.chave_saida
 AND fr.num_item_saida = e.num_item_saida
 AND fr.cod_item_saida = e.cod_item_saida
 AND fr.chave_nfe_ultima_entrada = e.chave_nfe_ultima_entrada
 AND fr.prod_nitem_entrada = e.prod_nitem_entrada;
