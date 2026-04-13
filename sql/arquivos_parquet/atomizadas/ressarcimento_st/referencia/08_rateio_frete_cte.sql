/* Gerado para integração com audit_react + C176 + Fronteira */
WITH ENTRADA_ESCOLHIDA AS (
    SELECT * FROM TMP_ENTRADA_ESCOLHIDA
),
CHAVES_ENTRADA_ESCOLHIDAS AS (
    SELECT DISTINCT e.chave_nfe_ultima_entrada AS chave_acesso
    FROM ENTRADA_ESCOLHIDA e
    WHERE e.chave_nfe_ultima_entrada IS NOT NULL
),
RATEIO_FRETE_ETAPA_A AS (
    SELECT ce.chave_acesso, cte_itens.IT_NU_CHAVE_CTE AS chave_cte
    FROM CHAVES_ENTRADA_ESCOLHIDAS ce
    JOIN sitafe.sitafe_cte_itens cte_itens
      ON ce.chave_acesso = cte_itens.IT_NU_CHAVE_NFE
),
RATEIO_FRETE_ETAPA_B AS (
    SELECT
        cte_itens.IT_NU_CHAVE_CTE,
        cte_itens.IT_NU_CHAVE_NFE,
        cte.IT_TP_FRETE,
        cte.IT_VA_TOTAL_FRETE AS total_frete,
        cte.IT_VA_VALOR_ICMS AS icms_frete,
        (nf.TOT_VPROD + nf.TOT_VFRETE + nf.TOT_VSEG + nf.TOT_VOUTRO - nf.TOT_VDESC + nf.TOT_VIPI + nf.TOT_VST) AS bc_rateio_fob,
        SUM(nf.TOT_VPROD + nf.TOT_VFRETE + nf.TOT_VSEG + nf.TOT_VOUTRO - nf.TOT_VDESC + nf.TOT_VIPI + nf.TOT_VST)
            OVER (PARTITION BY cte_itens.IT_NU_CHAVE_CTE) AS total_nf_cte
    FROM sitafe.sitafe_cte_itens cte_itens
    LEFT JOIN bi.fato_nfe_detalhe nf
      ON nf.chave_acesso = cte_itens.IT_NU_CHAVE_NFE
     AND nf.seq_nitem = 1
    LEFT JOIN sitafe.sitafe_cte cte
      ON cte.IT_NU_CHAVE_ACESSO = cte_itens.IT_NU_CHAVE_CTE
    WHERE cte_itens.IT_NU_CHAVE_CTE IN (SELECT chave_cte FROM RATEIO_FRETE_ETAPA_A)
      AND SUBSTR(cte.IT_NU_CNPJ_TOMADOR, 1, 8) = SUBSTR(nf.co_destinatario, 1, 8)
)
SELECT
    b.IT_NU_CHAVE_NFE AS chave_acesso,
    nf.prod_nitem,
    ROUND(
        ((nf.PROD_VPROD + nf.PROD_VFRETE + nf.PROD_VSEG + nf.PROD_VOUTRO - nf.PROD_VDESC + nf.IPI_VIPI + nf.ICMS_VICMSST)
        / NULLIF((nf.TOT_VPROD + nf.TOT_VFRETE + nf.TOT_VSEG + nf.TOT_VOUTRO - nf.TOT_VDESC + nf.TOT_VIPI + nf.TOT_VST), 0))
        * (b.total_frete * (b.bc_rateio_fob / NULLIF(b.total_nf_cte, 0))), 4
    ) AS rateio_frete_nf_item,
    ROUND(
        ((nf.PROD_VPROD + nf.PROD_VFRETE + nf.PROD_VSEG + nf.PROD_VOUTRO - nf.PROD_VDESC + nf.IPI_VIPI + nf.ICMS_VICMSST)
        / NULLIF((nf.TOT_VPROD + nf.TOT_VFRETE + nf.TOT_VSEG + nf.TOT_VOUTRO - nf.TOT_VDESC + nf.TOT_VIPI + nf.TOT_VST), 0))
        * (b.icms_frete * (b.bc_rateio_fob / NULLIF(b.total_nf_cte, 0))), 4
    ) AS rateio_icms_frete_nf_item
FROM RATEIO_FRETE_ETAPA_B b
JOIN bi.fato_nfe_detalhe nf
  ON nf.chave_acesso = b.IT_NU_CHAVE_NFE;
