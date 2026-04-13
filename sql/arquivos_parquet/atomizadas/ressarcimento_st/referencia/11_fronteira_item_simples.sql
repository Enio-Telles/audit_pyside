/* Gerado para integração com audit_react + C176 + Fronteira */
WITH ENTRADA_ESCOLHIDA AS (
    SELECT * FROM TMP_ENTRADA_ESCOLHIDA
),
CHAVES_ITENS_ENTRADA_ESCOLHIDOS AS (
    SELECT DISTINCT
        e.cnpj,
        e.chave_saida,
        e.num_item_saida,
        e.cod_item_saida,
        e.chave_nfe_ultima_entrada,
        e.prod_nitem_entrada
    FROM ENTRADA_ESCOLHIDA e
    WHERE e.chave_nfe_ultima_entrada IS NOT NULL
      AND e.prod_nitem_entrada IS NOT NULL
)
SELECT
    ce.chave_saida,
    ce.num_item_saida,
    ce.cod_item_saida,
    ce.chave_nfe_ultima_entrada,
    ce.prod_nitem_entrada,
    CASE
        WHEN nfe.co_emitente = ce.cnpj AND nfe.co_tp_nf = 1 THEN '1 - SAIDA'
        WHEN nfe.co_emitente = ce.cnpj AND nfe.co_tp_nf = 0 THEN '0 - ENTRADA'
        WHEN nfe.co_destinatario = ce.cnpj AND nfe.co_tp_nf = 1 THEN '0 - ENTRADA'
        WHEN nfe.co_destinatario = ce.cnpj AND nfe.co_tp_nf = 0 THEN '1 - SAIDA'
        ELSE 'INDEFINIDO'
    END AS fronteira_tipo_operacao,
    nfe.chave_acesso AS fronteira_chave_acesso,
    nfe.seq_nitem AS fronteira_num_item,
    nfe.prod_cprod AS fronteira_cod_item,
    nfe.prod_xprod AS fronteira_desc_item,
    nfe.prod_ncm AS fronteira_ncm,
    nfe.prod_cest AS fronteira_cest,
    nfe.prod_qcom AS fronteira_qtd_comercial,
    nfe.prod_vprod AS fronteira_valor_produto,
    nfe.icms_vbcst AS fronteira_bc_icms_st_destacado,
    nfe.icms_vicmsst AS fronteira_icms_st_destacado,
    calc_front.it_co_sefin AS fronteira_co_sefin_calc,
    calc_front.it_co_rotina_calculo AS fronteira_cod_rotina_calculo,
    calc_front.it_vl_icms AS fronteira_valor_icms_total_item
FROM CHAVES_ITENS_ENTRADA_ESCOLHIDOS ce
JOIN bi.fato_nfe_detalhe nfe
  ON nfe.chave_acesso = ce.chave_nfe_ultima_entrada
 AND nfe.seq_nitem = ce.prod_nitem_entrada
JOIN sitafe.sitafe_nfe_calculo_item calc_front
  ON calc_front.it_nu_chave_acesso = nfe.chave_acesso
 AND calc_front.it_nu_item = nfe.seq_nitem;
