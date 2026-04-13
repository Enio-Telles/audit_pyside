/* Gerado para integração com audit_react + C176 + Fronteira */
WITH ENTRADA_ESCOLHIDA AS (
    SELECT * FROM TMP_ENTRADA_ESCOLHIDA
),
SEFIN_ITEM_ESCOLHIDO AS (
    SELECT * FROM TMP_SEFIN_ITEM_ESCOLHIDO
),
CREDITO_CALCULADO AS (
    SELECT * FROM TMP_CREDITO_CALCULADO
),
RATEIO_FRETE_ETAPA_C AS (
    SELECT * FROM TMP_RATEIO_FRETE_ETAPA_C
)
SELECT
    e.chave_saida,
    e.num_item_saida,
    e.cod_item_saida,
    e.chave_nfe_ultima_entrada,
    e.prod_nitem_entrada,
    s.it_co_sefin_efetivo,
    s.IT_PC_INTERNA,
    s.IT_IN_ST,
    s.IT_PC_MVA,
    s.IT_IN_MVA_AJUSTADO,
    nf.CO_CRT,
    nf.PROD_QCOM,
    nf.ICMS_VICMS AS icms_proprio_nf_total,
    NVL(rf.rateio_frete_nf_item, 0) AS rateio_frete_nf_item,
    NVL(rf.rateio_icms_frete_nf_item, 0) AS rateio_icms_frete_nf_item,
    NVL(cc.cred_calc_total_item, 0) AS cred_calc_total_item,
    CASE
        WHEN s.IT_IN_ST = 'S' THEN
            CASE
                WHEN nf.CO_CRT IN ('1', '4') THEN
                    ROUND(
                        (((NVL(sn.IT_VA_PRODUTO, 0) + NVL(sn.IT_VA_FRETE, 0) + NVL(sn.IT_VA_SEGURO, 0)
                         - NVL(sn.IT_VA_DESCONTO, 0) + NVL(sn.IT_VA_OUTRO, 0) + NVL(sn.IT_VA_IPI_ITEM, 0)
                         + NVL(rf.rateio_frete_nf_item, 0)) * (100 + NVL(s.IT_PC_MVA, 0)) / 100
                         * NVL(s.IT_PC_INTERNA, 0) / 100) - NVL(cc.cred_calc_total_item, 0)
                         - NVL(rf.rateio_icms_frete_nf_item, 0), 2)
                ELSE
                    CASE
                        WHEN s.IT_IN_MVA_AJUSTADO = 'S' THEN
                            ROUND(
                                (((((NVL(sn.IT_VA_PRODUTO, 0) + NVL(sn.IT_VA_FRETE, 0) + NVL(sn.IT_VA_SEGURO, 0)
                                    - NVL(sn.IT_VA_DESCONTO, 0) + NVL(sn.IT_VA_OUTRO, 0) - NVL(nf.ICMS_VICMS, 0))
                                   / NULLIF((1 - NVL(s.IT_PC_INTERNA, 0) / 100), 0))
                                  + NVL(sn.IT_VA_IPI_ITEM, 0) + NVL(rf.rateio_frete_nf_item, 0))
                                  * (100 + NVL(s.IT_PC_MVA, 0)) / 100 * NVL(s.IT_PC_INTERNA, 0) / 100)
                                  - LEAST(NVL(nf.ICMS_VICMS, 0), NVL(cc.cred_calc_total_item, 0))
                                  - NVL(rf.rateio_icms_frete_nf_item, 0), 2)
                        ELSE
                            ROUND(
                                ((((NVL(sn.IT_VA_PRODUTO, 0) + NVL(sn.IT_VA_FRETE, 0) + NVL(sn.IT_VA_SEGURO, 0)
                                   - NVL(sn.IT_VA_DESCONTO, 0) + NVL(sn.IT_VA_OUTRO, 0) + NVL(sn.IT_VA_IPI_ITEM, 0)
                                   + NVL(rf.rateio_frete_nf_item, 0)) * (100 + NVL(s.IT_PC_MVA, 0)) / 100
                                   * NVL(s.IT_PC_INTERNA, 0)) / 100)
                                   - LEAST(NVL(nf.ICMS_VICMS, 0), NVL(cc.cred_calc_total_item, 0))
                                   - NVL(rf.rateio_icms_frete_nf_item, 0), 2)
                    END
            END
        ELSE NULL
    END AS calc_st_total_item
FROM ENTRADA_ESCOLHIDA e
JOIN bi.fato_nfe_detalhe nf
  ON nf.chave_acesso = e.chave_nfe_ultima_entrada
 AND nf.prod_nitem = e.prod_nitem_entrada
LEFT JOIN sitafe.sitafe_nfe_item sn
  ON sn.IT_NU_CHAVE_ACESSO = e.chave_nfe_ultima_entrada
 AND sn.IT_NU_ITEM = e.prod_nitem_entrada
LEFT JOIN SEFIN_ITEM_ESCOLHIDO s
  ON s.chave_saida = e.chave_saida
 AND s.num_item_saida = e.num_item_saida
 AND s.cod_item_saida = e.cod_item_saida
 AND s.chave_nfe_ultima_entrada = e.chave_nfe_ultima_entrada
 AND s.prod_nitem_entrada = e.prod_nitem_entrada
LEFT JOIN CREDITO_CALCULADO cc
  ON cc.chave_saida = e.chave_saida
 AND cc.num_item_saida = e.num_item_saida
 AND cc.cod_item_saida = e.cod_item_saida
 AND cc.chave_nfe_ultima_entrada = e.chave_nfe_ultima_entrada
 AND cc.prod_nitem_entrada = e.prod_nitem_entrada
LEFT JOIN RATEIO_FRETE_ETAPA_C rf
  ON rf.chave_acesso = e.chave_nfe_ultima_entrada
 AND rf.prod_nitem = e.prod_nitem_entrada;
