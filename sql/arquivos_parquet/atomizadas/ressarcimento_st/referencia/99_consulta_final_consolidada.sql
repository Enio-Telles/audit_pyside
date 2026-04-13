/* Gerado para integração com audit_react + C176 + Fronteira */
SELECT
    b.*,
    c.qtd_itens_c176,
    c.vl_st_decl_total_mes,
    c.vl_st_calc_total_mes,
    c.vl_st_fronteira_total_mes,
    c.vl_e111_st_mes,
    c.vl_e111_st_extemporaneo_mes,
    c.dif_c176_st_x_e111,
    c.dif_calc_st_x_c176,
    c.dif_fronteira_st_x_c176,
    c.dif_fronteira_st_x_calc
FROM TMP_BATIMENTO_ITEM b
LEFT JOIN TMP_CONCILIACAO_MENSAL c
  ON c.mes_ref = b.mes_ref
ORDER BY
    b.mes_ref,
    b.dt_doc_saida,
    b.chave_saida,
    b.num_item_saida;
