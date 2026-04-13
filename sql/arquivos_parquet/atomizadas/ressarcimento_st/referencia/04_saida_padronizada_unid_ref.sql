/* Gerado para integração com audit_react + C176 + Fronteira */
WITH SAIDA_AGRUPADA AS (
    SELECT * FROM TMP_SAIDA_AGRUPADA
),
FATORES_SAIDA AS (
    SELECT
        f.id_agrupado,
        UPPER(TRIM(f.unid)) AS unid_norm,
        UPPER(TRIM(f.unid_ref)) AS unid_ref,
        NVL(f.fator, 1) AS fator_saida
    FROM AUDIT_REACT_FATORES_CONVERSAO f
)
SELECT
    s.*,
    NVL(fs.unid_ref, UPPER(TRIM(COALESCE(s.unid_saida_agr, s.unid_saida_c176)))) AS unid_ref,
    NVL(fs.fator_saida, 1) AS fator_saida,
    NVL(s.qtd_saida_c176, 0) * NVL(fs.fator_saida, 1) AS qtd_saida_unid_ref,
    CASE WHEN NVL(fs.fator_saida, 1) <> 0 THEN NVL(s.vl_unit_bc_st_entrada_decl, 0) / NVL(fs.fator_saida, 1) END AS vl_unit_bc_st_decl_unid_ref,
    CASE WHEN NVL(fs.fator_saida, 1) <> 0 THEN NVL(s.vl_unit_icms_proprio_decl, 0) / NVL(fs.fator_saida, 1) END AS vl_unit_icms_proprio_decl_unid_ref,
    CASE WHEN NVL(fs.fator_saida, 1) <> 0 THEN NVL(s.vl_unit_st_decl, 0) / NVL(fs.fator_saida, 1) END AS vl_unit_st_decl_unid_ref
FROM SAIDA_AGRUPADA s
LEFT JOIN FATORES_SAIDA fs
  ON fs.id_agrupado = s.id_agrupado
 AND fs.unid_norm = UPPER(TRIM(COALESCE(s.unid_saida_agr, s.unid_saida_c176)));
