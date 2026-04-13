select 
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
END AS status_descricao,
t.*
from APP_PENDENCIA.PENDENCIAS t
where cpf_cnpj = :CNPJ
--and STATUS = 0