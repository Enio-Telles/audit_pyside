/* Gerado para integração com audit_react + C176 + Fronteira */
WITH SAIDA_PADRONIZADA AS (
    SELECT * FROM TMP_SAIDA_PADRONIZADA
)
SELECT
    s.comp_efd,
    s.mes_ref,
    s.reg_0000_id,
    s.cnpj,
    s.cod_fin_efd,
    s.chave_saida,
    s.num_nf_saida,
    s.dt_doc_saida,
    s.num_item_saida,
    s.cod_item_saida,
    s.descricao_item_saida,
    s.qtd_saida_c176,
    s.qtd_saida_unid_ref,
    s.unid_ref,
    s.fator_saida,
    s.vl_total_item_saida,
    s.vl_icms_saida,
    s.cod_mot_res,
    s.chave_nfe_ultima_entrada,
    s.c176_num_item_ult_e_declarado,
    s.dt_ultima_entrada,
    s.vl_unit_bc_st_entrada_decl,
    s.vl_unit_icms_proprio_decl,
    s.vl_unit_st_decl,
    s.vl_unit_bc_st_decl_unid_ref,
    s.vl_unit_icms_proprio_decl_unid_ref,
    s.vl_unit_st_decl_unid_ref,
    s.id_agrupado,
    s.descr_padrao,
    s.cod_ncm_saida_agr,
    s.cest_saida_agr,
    n.chave_acesso AS chave_entrada_xml,
    n.prod_nitem AS prod_nitem_entrada,
    n.prod_cprod AS cod_item_entrada_xml,
    n.prod_xprod AS descricao_entrada_xml,
    n.prod_ncm AS ncm_entrada_xml,
    n.prod_cest AS cest_entrada_xml,
    n.prod_ucom AS unid_entrada_xml,
    n.prod_qcom AS qtd_entrada_xml,
    n.prod_vuncom AS vl_unit_entrada_xml,
    (
        NVL(n.prod_vprod, 0)
      + NVL(n.prod_vfrete, 0)
      + NVL(n.prod_vseg, 0)
      + NVL(n.prod_voutro, 0)
      - NVL(n.prod_vdesc, 0)
    ) AS vl_total_entrada_xml,
    n.id_agrupado AS id_agrupado_entrada_xml
FROM TMP_SAIDA_PADRONIZADA s
LEFT JOIN AUDIT_REACT_NFE_AGR n
  ON n.chave_acesso = s.chave_nfe_ultima_entrada;
