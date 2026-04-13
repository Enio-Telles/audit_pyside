/* Gerado para integração com audit_react + C176 + Fronteira */
WITH BATIMENTO_ITEM AS (
    SELECT * FROM TMP_BATIMENTO_ITEM
),
RESUMO_MENSAL_C176_RECALC AS (
    SELECT
        b.mes_ref,
        COUNT(*) AS qtd_itens_c176,
        SUM(NVL(b.vl_st_decl_total_considerado, 0)) AS vl_st_decl_total_mes,
        SUM(NVL(b.vl_st_calc_total_considerado, 0)) AS vl_st_calc_total_mes,
        SUM(NVL(b.vl_st_fronteira_total_considerado, 0)) AS vl_st_fronteira_total_mes
    FROM BATIMENTO_ITEM b
    GROUP BY b.mes_ref
),
RESUMO_MENSAL_E111 AS (
    SELECT * FROM TMP_RESUMO_MENSAL_E111
)
SELECT
    c.mes_ref,
    c.qtd_itens_c176,
    c.vl_st_decl_total_mes,
    c.vl_st_calc_total_mes,
    c.vl_st_fronteira_total_mes,
    NVL(e.vl_e111_st_mes, 0) AS vl_e111_st_mes,
    NVL(e.vl_e111_st_extemporaneo_mes, 0) AS vl_e111_st_extemporaneo_mes,
    NVL(c.vl_st_decl_total_mes, 0) - NVL(e.vl_e111_st_mes, 0) AS dif_c176_st_x_e111,
    NVL(c.vl_st_calc_total_mes, 0) - NVL(c.vl_st_decl_total_mes, 0) AS dif_calc_st_x_c176,
    NVL(c.vl_st_fronteira_total_mes, 0) - NVL(c.vl_st_decl_total_mes, 0) AS dif_fronteira_st_x_c176,
    NVL(c.vl_st_fronteira_total_mes, 0) - NVL(c.vl_st_calc_total_mes, 0) AS dif_fronteira_st_x_calc
FROM RESUMO_MENSAL_C176_RECALC c
LEFT JOIN RESUMO_MENSAL_E111 e
  ON e.mes_ref = c.mes_ref
ORDER BY c.mes_ref;
