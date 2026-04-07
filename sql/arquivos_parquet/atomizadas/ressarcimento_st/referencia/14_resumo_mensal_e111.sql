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
)
SELECT
    TRUNC(arq.dt_ini, 'MM') AS mes_ref,
    SUM(CASE
            WHEN TRUNC(arq.dt_ini, 'MM') < DATE '2025-01-01' AND e111.cod_aj_apur = 'RO020022' THEN NVL(e111.vl_aj_apur, 0)
            WHEN TRUNC(arq.dt_ini, 'MM') >= DATE '2025-01-01' AND e111.cod_aj_apur = 'RO020047' THEN NVL(e111.vl_aj_apur, 0)
            ELSE 0
        END) AS vl_e111_st_mes,
    SUM(CASE
            WHEN TRUNC(arq.dt_ini, 'MM') < DATE '2025-01-01' AND e111.cod_aj_apur = 'RO020023' THEN NVL(e111.vl_aj_apur, 0)
            WHEN TRUNC(arq.dt_ini, 'MM') >= DATE '2025-01-01' AND e111.cod_aj_apur = 'RO020049' THEN NVL(e111.vl_aj_apur, 0)
            ELSE 0
        END) AS vl_e111_icms_proprio_mes,
    SUM(CASE WHEN e111.cod_aj_apur = 'RO020048' THEN NVL(e111.vl_aj_apur, 0) ELSE 0 END) AS vl_e111_st_extemporaneo_mes,
    SUM(CASE WHEN e111.cod_aj_apur = 'RO020050' THEN NVL(e111.vl_aj_apur, 0) ELSE 0 END) AS vl_e111_icms_proprio_extemporaneo_mes
FROM ARQUIVOS_ULTIMA_EFD_PERIODO arq
JOIN sped.reg_e111 e111
  ON e111.reg_0000_id = arq.reg_0000_id
JOIN PARAMETROS p
  ON arq.cnpj = p.cnpj_filtro
WHERE arq.dt_ini BETWEEN p.dt_ini_filtro AND p.dt_fim_filtro
GROUP BY TRUNC(arq.dt_ini, 'MM')
ORDER BY mes_ref;
