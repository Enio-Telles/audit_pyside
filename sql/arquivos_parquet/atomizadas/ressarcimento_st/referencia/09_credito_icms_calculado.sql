/* Gerado para integração com audit_react + C176 + Fronteira */
WITH ENTRADA_ESCOLHIDA AS (
    SELECT * FROM TMP_ENTRADA_ESCOLHIDA
)
SELECT
    e.chave_saida,
    e.num_item_saida,
    e.cod_item_saida,
    e.chave_nfe_ultima_entrada,
    e.prod_nitem_entrada,
    ROUND(
        (
            CASE
                WHEN nf.ICMS_ORIG IN ('1', '2', '3', '8') THEN 0.04
                ELSE (SELECT uf.ALIQ FROM qvw.tbl_aliq_ufs uf WHERE uf.UF = nf.CO_UF_EMIT)
            END
        ) * (
            NVL(nf.PROD_VPROD, 0)
          + NVL(nf.PROD_VFRETE, 0)
          + NVL(nf.PROD_VSEG, 0)
          - NVL(nf.PROD_VDESC, 0)
          + NVL(nf.PROD_VOUTRO, 0)
        ),
        2
    ) AS cred_calc_total_item
FROM ENTRADA_ESCOLHIDA e
JOIN bi.fato_nfe_detalhe nf
  ON nf.chave_acesso = e.chave_nfe_ultima_entrada
 AND nf.prod_nitem = e.prod_nitem_entrada;
