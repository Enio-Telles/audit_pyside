/* Gerado para integração com audit_react + C176 + Fronteira */
WITH C176_SAIDA_BASE AS (
    SELECT * FROM TMP_C176_SAIDA_BASE
)
SELECT
    b.*,
    a.id_agrupado,
    a.descr_padrao,
    a.cod_ncm AS cod_ncm_saida_agr,
    a.cest AS cest_saida_agr,
    a.unid AS unid_saida_agr,
    a.qtd AS qtd_saida_agr
FROM C176_SAIDA_BASE b
LEFT JOIN AUDIT_REACT_C170_AGR a
  ON a.chv_nfe = b.chave_saida
 AND a.num_item = b.num_item_saida
 AND a.cod_item = b.cod_item_saida;
