SELECT
    CAST(NULL AS VARCHAR2(44)) AS chave_nfe_ultima_entrada,
    CAST(NULL AS NUMBER) AS prod_nitem_entrada,
    CAST(NULL AS VARCHAR2(32)) AS it_co_sefin_efetivo,
    CAST(NULL AS NUMBER) AS it_pc_interna,
    CAST(NULL AS VARCHAR2(1)) AS it_in_st,
    CAST(NULL AS NUMBER) AS it_pc_mva,
    CAST(NULL AS VARCHAR2(1)) AS it_in_mva_ajustado
FROM dual
WHERE REGEXP_REPLACE(TRIM(:CNPJ), '[^0-9]', '') IS NOT NULL
  AND 1 = 0
