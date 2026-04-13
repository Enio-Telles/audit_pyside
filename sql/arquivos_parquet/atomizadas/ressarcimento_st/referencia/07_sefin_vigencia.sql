/* Gerado para integração com audit_react + C176 + Fronteira */
WITH ENTRADA_ESCOLHIDA AS (
    SELECT * FROM TMP_ENTRADA_ESCOLHIDA
),
TAB_AUX_CLASSIFICACAO AS (
    SELECT
        a.IT_CO_SEFIN,
        TO_DATE(a.IT_DA_INICIO, 'YYYYMMDD') AS dt_ini_vig,
        CASE WHEN TRIM(a.IT_DA_FINAL) IS NULL THEN DATE '2999-12-31' ELSE TO_DATE(a.IT_DA_FINAL, 'YYYYMMDD') END AS dt_fim_vig,
        a.IT_PC_INTERNA,
        a.IT_IN_ST,
        a.IT_PC_MVA,
        a.IT_IN_MVA_AJUSTADO,
        a.IT_IN_CONVENIO,
        a.IT_IN_ISENTO_ICMS,
        a.IT_IN_REDUCAO,
        a.IT_PC_REDUCAO,
        a.IT_IN_PGTO_SAIDA,
        a.IT_IN_COMBUSTIVEL,
        a.IT_IN_REDUCAO_CREDITO,
        a.IT_IN_PMPF
    FROM sitafe.sitafe_produto_sefin_aux a
)
SELECT
    e.chave_saida,
    e.num_item_saida,
    e.cod_item_saida,
    e.chave_nfe_ultima_entrada,
    e.prod_nitem_entrada,
    COALESCE(sn.IT_CO_SEFIN, scn.IT_CO_SEFIN) AS it_co_sefin_efetivo,
    aux.IT_PC_INTERNA,
    aux.IT_IN_ST,
    aux.IT_PC_MVA,
    aux.IT_IN_MVA_AJUSTADO,
    aux.IT_IN_CONVENIO,
    aux.IT_IN_ISENTO_ICMS,
    aux.IT_IN_REDUCAO,
    aux.IT_PC_REDUCAO,
    aux.IT_IN_PGTO_SAIDA,
    aux.IT_IN_COMBUSTIVEL,
    aux.IT_IN_REDUCAO_CREDITO,
    aux.IT_IN_PMPF
FROM ENTRADA_ESCOLHIDA e
LEFT JOIN sitafe.sitafe_nfe_item sn
  ON sn.IT_NU_CHAVE_ACESSO = e.chave_nfe_ultima_entrada
 AND sn.IT_NU_ITEM = e.prod_nitem_entrada
LEFT JOIN sitafe.sitafe_cest_ncm scn
  ON TRIM(scn.IT_NU_NCM) = TRIM(e.ncm_entrada_xml)
 AND (TRIM(scn.IT_NU_CEST) = TRIM(e.cest_entrada_xml) OR e.cest_entrada_xml IS NULL)
 AND NVL(scn.IT_IN_STATUS, 'A') <> 'C'
LEFT JOIN TAB_AUX_CLASSIFICACAO aux
  ON aux.IT_CO_SEFIN = COALESCE(sn.IT_CO_SEFIN, scn.IT_CO_SEFIN)
 AND e.dt_ultima_entrada BETWEEN aux.dt_ini_vig AND aux.dt_fim_vig;
