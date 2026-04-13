/* Gerado para integração com audit_react + C176 + Fronteira */
WITH ENTRADA_CANDIDATOS AS (
    SELECT * FROM TMP_ENTRADA_CANDIDATOS
),
FATORES_ENTRADA AS (
    SELECT
        f.id_agrupado,
        UPPER(TRIM(f.unid)) AS unid_norm,
        UPPER(TRIM(f.unid_ref)) AS unid_ref,
        NVL(f.fator, 1) AS fator_entrada
    FROM AUDIT_REACT_FATORES_CONVERSAO f
),
BASE AS (
    SELECT
        c.*,
        NVL(fe.fator_entrada, 1) AS fator_entrada,
        CASE WHEN c.qtd_entrada_xml IS NOT NULL THEN NVL(c.qtd_entrada_xml, 0) * NVL(fe.fator_entrada, 1) END AS qtd_entrada_unid_ref,
        CASE WHEN c.id_agrupado = c.id_agrupado_entrada_xml THEN 1 ELSE 0 END AS ind_match_id_agrupado,
        CASE WHEN c.c176_num_item_ult_e_declarado = c.prod_nitem_entrada THEN 1 ELSE 0 END AS ind_match_item_declarado,
        CASE WHEN TRIM(c.cod_item_saida) = TRIM(c.cod_item_entrada_xml) THEN 1 ELSE 0 END AS ind_match_cod_item,
        CASE WHEN TRIM(c.cod_ncm_saida_agr) = TRIM(c.ncm_entrada_xml) AND c.cod_ncm_saida_agr IS NOT NULL THEN 1 ELSE 0 END AS ind_match_ncm,
        CASE WHEN TRIM(c.cest_saida_agr) = TRIM(c.cest_entrada_xml) AND c.cest_saida_agr IS NOT NULL THEN 1 ELSE 0 END AS ind_match_cest,
        ABS(NVL(c.qtd_saida_unid_ref, 0) - NVL(NVL(c.qtd_entrada_xml, 0) * NVL(fe.fator_entrada, 1), 0)) AS diff_qtd_unid_ref
    FROM ENTRADA_CANDIDATOS c
    LEFT JOIN FATORES_ENTRADA fe
      ON fe.id_agrupado = c.id_agrupado
     AND fe.unid_norm = UPPER(TRIM(c.unid_entrada_xml))
),
RANKED AS (
    SELECT
        b.*,
        (
            CASE WHEN b.ind_match_id_agrupado = 1 THEN 1000 ELSE 0 END
          + CASE WHEN b.ind_match_item_declarado = 1 THEN 300 ELSE 0 END
          + CASE WHEN b.ind_match_cod_item = 1 THEN 120 ELSE 0 END
          + CASE WHEN b.ind_match_ncm = 1 THEN 150 ELSE 0 END
          + CASE WHEN b.ind_match_cest = 1 THEN 100 ELSE 0 END
          + CASE
                WHEN ABS(NVL(b.qtd_saida_unid_ref, 0) - NVL(b.qtd_entrada_unid_ref, 0)) <= 0.000001 THEN 80
                WHEN ABS(NVL(b.qtd_saida_unid_ref, 0) - NVL(b.qtd_entrada_unid_ref, 0)) <= 1 THEN 30
                ELSE 0
            END
        ) AS score_vinculo_entrada,
        ROW_NUMBER() OVER (
            PARTITION BY b.chave_saida, b.num_item_saida, b.cod_item_saida, b.chave_nfe_ultima_entrada
            ORDER BY
                CASE WHEN b.ind_match_id_agrupado = 1 THEN 1 ELSE 0 END DESC,
                CASE WHEN b.ind_match_item_declarado = 1 THEN 1 ELSE 0 END DESC,
                (
                    CASE WHEN b.ind_match_id_agrupado = 1 THEN 1000 ELSE 0 END
                  + CASE WHEN b.ind_match_item_declarado = 1 THEN 300 ELSE 0 END
                  + CASE WHEN b.ind_match_cod_item = 1 THEN 120 ELSE 0 END
                  + CASE WHEN b.ind_match_ncm = 1 THEN 150 ELSE 0 END
                  + CASE WHEN b.ind_match_cest = 1 THEN 100 ELSE 0 END
                  + CASE
                        WHEN ABS(NVL(b.qtd_saida_unid_ref, 0) - NVL(b.qtd_entrada_unid_ref, 0)) <= 0.000001 THEN 80
                        WHEN ABS(NVL(b.qtd_saida_unid_ref, 0) - NVL(b.qtd_entrada_unid_ref, 0)) <= 1 THEN 30
                        ELSE 0
                    END
                ) DESC,
                b.diff_qtd_unid_ref ASC,
                b.prod_nitem_entrada ASC
        ) AS rn
    FROM BASE b
)
SELECT
    r.*,
    CASE
        WHEN r.ind_match_id_agrupado = 1 AND r.ind_match_item_declarado = 1 THEN 'VINCULO POR ID_AGRUPADO + ITEM DECLARADO'
        WHEN r.ind_match_id_agrupado = 1 THEN 'VINCULO POR ID_AGRUPADO'
        WHEN r.ind_match_cod_item = 1 THEN 'VINCULO POR COD_ITEM'
        ELSE 'VINCULO POR SCORE'
    END AS regra_vinculo_entrada
FROM RANKED r
WHERE r.rn = 1;
