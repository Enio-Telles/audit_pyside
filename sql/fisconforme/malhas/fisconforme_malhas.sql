WITH
    PARAMETROS AS (
        SELECT:cnpj AS cnpj_filtro
        FROM dual
    )

SELECT
    --t.cpf_cnpj AS CPF_CNPJ,
    t.malhas_id AS MALHAS_ID,
    m.titulo AS TITULO,
    t.periodo AS PERIODO,

-- Formata o perodo para DD/MM/YYYY (ex: 01/12/2023)
TO_CHAR(
    TO_DATE(
        '01/' || SUBSTR(t.periodo, 5, 2) || '/' || SUBSTR(t.periodo, 1, 4),
        'DD/MM/YYYY'
    ),
    'DD/MM/YYYY'
) AS PERIODO_FORMATADO,

-- Data da cincia formatada com hora
-- Utilizando aspas duplas e escapando o dois pontos para evitar que o Python Oracle Driver ache que é variavel bind
TO_CHAR( n.dt_ciencia, 'DD/MM/YYYY HH24":"MI":"SS' ) AS DATA_CIENCIA,

--t.status AS STATUS,
CASE t.status
    WHEN 0 THEN '0 - pendente'
    WHEN 1 THEN '1 - contestado'
    WHEN 2 THEN '2 - resolvido'
    WHEN 3 THEN '3 - acao fiscal'
    WHEN 4 THEN '4 - pendente indeferido'
    WHEN 5 THEN '5 - deferido'
    WHEN 6 THEN '6 - notificado'
    WHEN 7 THEN '7 - deferido automaticamente'
    WHEN 8 THEN '8 - aguardando autorizacao'
    WHEN 9 THEN '9 - cancelado'
    WHEN 11 THEN '11 - inapta - 5 anos'
    WHEN 12 THEN '12 - pre-fiscalizacao'
    ELSE TO_CHAR(t.status)
END AS status_descricao
FROM app_pendencia.pendencias t
    INNER JOIN app_pendencia.malhas m ON t.malhas_id = m.id
    LEFT JOIN bi.fato_det_notificacao n ON t.id = n.id_fisconforme
    CROSS JOIN PARAMETROS p -- Permite acessar os parametros na clausula WHERE
WHERE
    t.cpf_cnpj = p.cnpj_filtro
ORDER BY t.periodo DESC;