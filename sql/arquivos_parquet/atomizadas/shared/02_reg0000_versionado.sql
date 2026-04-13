WITH PARAMETROS AS (
    SELECT
        :CNPJ AS cnpj_filtro,
        NVL(
            TO_DATE(:data_limite_processamento, 'DD/MM/YYYY'),
            TRUNC(SYSDATE)
        ) AS dt_corte
    FROM dual
),
REG0000_HISTORICO AS (
    SELECT
        r.id AS reg_0000_id,
        r.reg,
        r.cod_ver,
        r.cod_fin,
        r.dt_ini,
        r.dt_fin,
        r.nome,
        r.cnpj,
        r.cpf,
        r.uf,
        r.ie,
        r.cod_mun,
        r.im,
        r.suframa,
        r.ind_perfil,
        r.ind_ativ,
        r.arquivo_nome,
        r.data_entrega,
        r.created_at,
        r.updated_at,
        r.reg_1,
        r.reg_c,
        r.reg_d,
        r.reg_e,
        r.reg_g,
        r.reg_h,
        r.reg_k,
        r.arquivo_tamanho
    FROM sped.reg_0000 r
    CROSS JOIN PARAMETROS p
    WHERE r.cnpj = p.cnpj_filtro
      AND r.dt_ini >= DATE '2020-01-01'
      AND r.data_entrega <= p.dt_corte
)
SELECT
    h.*,
    ROW_NUMBER() OVER (
        PARTITION BY h.cnpj, h.dt_ini
        ORDER BY h.data_entrega DESC, h.reg_0000_id DESC
    ) AS ordem,
    COUNT(*) OVER (
        PARTITION BY h.cnpj, h.dt_ini
    ) AS qtd_envios
FROM REG0000_HISTORICO h;
