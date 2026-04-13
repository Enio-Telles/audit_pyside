WITH PARAMETROS AS (
    SELECT
        :CNPJ AS cnpj_filtro,
        NVL(
            TO_DATE(:data_limite_processamento, 'DD/MM/YYYY'),
            TRUNC(SYSDATE)
        ) AS dt_corte
    FROM dual
),
ARQUIVOS_VALIDOS AS (
    SELECT
        r.id AS reg_0000_id,
        r.dt_ini,
        r.cnpj,
        ROW_NUMBER() OVER (
            PARTITION BY r.cnpj, r.dt_ini
            ORDER BY r.data_entrega DESC, r.id DESC
        ) AS rn
    FROM sped.reg_0000 r
    CROSS JOIN PARAMETROS p
    WHERE r.cnpj = p.cnpj_filtro
      AND r.data_entrega <= p.dt_corte
      AND r.dt_ini >= DATE '2020-01-01'
),
ARQUIVOS_FINAIS AS (
    SELECT reg_0000_id, dt_ini FROM ARQUIVOS_VALIDOS WHERE rn = 1
),
BASE_C190 AS (
    SELECT
        arq.dt_ini,
        c190.reg_0000_id,
        c190.reg_c100_id,
        c190.cst_icms,
        c190.cfop,
        c190.aliq_icms,
        c190.vl_opr,
        c190.vl_bc_icms,
        c190.vl_icms,
        c190.vl_bc_icms_st,
        c190.vl_icms_st,
        c190.vl_red_bc,
        c190.vl_ipi,
        c190.cod_obs
    FROM sped.reg_c190 c190
    JOIN ARQUIVOS_FINAIS arq ON arq.reg_0000_id = c190.reg_0000_id
)
SELECT
    TO_CHAR(dt_ini, 'YYYY/MM') AS periodo_efd,
    cst_icms,
    cfop,
    aliq_icms,
    SUM(vl_opr) AS vl_opr,
    SUM(vl_bc_icms) AS vl_bc_icms,
    SUM(vl_icms) AS vl_icms,
    SUM(vl_bc_icms_st) AS vl_bc_icms_st,
    SUM(vl_icms_st) AS vl_icms_st,
    SUM(vl_red_bc) AS vl_red_bc,
    SUM(vl_ipi) AS vl_ipi
FROM BASE_C190
GROUP BY
    dt_ini,
    cst_icms,
    cfop,
    aliq_icms
ORDER BY
    dt_ini,
    cfop,
    cst_icms;
