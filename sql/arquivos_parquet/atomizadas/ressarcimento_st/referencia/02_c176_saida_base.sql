/* Gerado para integração com audit_react + C176 + Fronteira */
WITH
PARAMETROS AS (
    SELECT
        :CNPJ AS cnpj_filtro,
        NVL(TO_DATE(:data_inicial, 'DD/MM/YYYY'), DATE '2020-01-01') AS dt_ini_filtro,
        NVL(TO_DATE(:data_final, 'DD/MM/YYYY'), TRUNC(SYSDATE)) AS dt_fim_filtro,
        NVL(TO_DATE(:data_limite_processamento, 'DD/MM/YYYY'), TRUNC(SYSDATE)) AS dt_corte
    FROM dual
),
ARQUIVOS_ULTIMA_EFD_PERIODO AS (
    SELECT reg_0000_id, cnpj, cod_fin_efd, dt_ini, dt_fin, data_entrega
    FROM (
        SELECT
            r.id AS reg_0000_id,
            r.cnpj,
            r.cod_fin AS cod_fin_efd,
            r.dt_ini,
            r.dt_fin,
            r.data_entrega,
            ROW_NUMBER() OVER (
                PARTITION BY r.cnpj, r.dt_ini, NVL(r.dt_fin, r.dt_ini)
                ORDER BY r.data_entrega DESC, r.id DESC
            ) AS rn
        FROM sped.reg_0000 r
        JOIN PARAMETROS p
          ON r.cnpj = p.cnpj_filtro
        WHERE r.data_entrega <= p.dt_corte
    )
    WHERE rn = 1
),
ARQUIVOS_VALIDOS AS (
    SELECT a.*
    FROM ARQUIVOS_ULTIMA_EFD_PERIODO a
    JOIN PARAMETROS p
      ON a.dt_ini BETWEEN p.dt_ini_filtro AND p.dt_fim_filtro
)
SELECT
    arq.reg_0000_id,
    arq.cnpj,
    arq.dt_ini AS comp_efd,
    TRUNC(arq.dt_ini, 'MM') AS mes_ref,
    arq.cod_fin_efd,
    c100.chv_nfe AS chave_saida,
    c100.num_doc AS num_nf_saida,
    CASE
        WHEN c100.dt_doc IS NOT NULL AND REGEXP_LIKE(c100.dt_doc, '^[0-9]{8}$')
        THEN TO_DATE(c100.dt_doc, 'DDMMYYYY')
    END AS dt_doc_saida,
    c170.num_item AS num_item_saida,
    c170.cod_item AS cod_item_saida,
    c170.descr_compl AS descricao_item_saida,
    c170.qtd AS qtd_saida_c176,
    c170.unid AS unid_saida_c176,
    c170.vl_item AS vl_total_item_saida,
    c170.vl_icms AS vl_icms_saida,
    c176.cod_mot_res,
    c176.chave_nfe_ult AS chave_nfe_ultima_entrada,
    c176.num_item_ult_e AS c176_num_item_ult_e_declarado,
    CASE
        WHEN c176.dt_ult_e IS NOT NULL AND REGEXP_LIKE(c176.dt_ult_e, '^[0-9]{8}$')
        THEN TO_DATE(c176.dt_ult_e, 'DDMMYYYY')
    END AS dt_ultima_entrada,
    c176.vl_unit_ult_e AS vl_unit_bc_st_entrada_decl,
    c176.vl_unit_icms_ult_e AS vl_unit_icms_proprio_decl,
    c176.vl_unit_res AS vl_unit_st_decl
FROM sped.reg_c176 c176
JOIN ARQUIVOS_VALIDOS arq
  ON c176.reg_0000_id = arq.reg_0000_id
JOIN sped.reg_c100 c100
  ON c176.reg_c100_id = c100.id
 AND c100.reg_0000_id = arq.reg_0000_id
JOIN sped.reg_c170 c170
  ON c176.reg_c170_id = c170.id
 AND c170.reg_0000_id = arq.reg_0000_id
ORDER BY comp_efd, chave_saida, num_item_saida;
