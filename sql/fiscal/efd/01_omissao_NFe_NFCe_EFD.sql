WITH parametros AS (
    SELECT 
        :CNPJ AS cnpj
    FROM dual
),
docs_saida AS (
    /* Passo 1: Busca NF-e e NFC-e de SA�DA emitidas pelo pr�prio contribuinte */
    SELECT 
        d.chave_acesso, d.ide_serie, d.nnf, d.tot_vnf, d.tot_vicms, 
        d.dhemi, d.co_uf_emit, d.co_uf_dest, d.co_emitente, d.co_destinatario,
        d.infprot_cstat,
        SUBSTR(d.chave_acesso, 21, 2) AS co_modelo 
    FROM bi.fato_nfe_detalhe d
    CROSS JOIN parametros p
    WHERE d.co_emitente = p.cnpj         
      AND d.co_tp_nf = 1                 
      AND SUBSTR(d.chave_acesso, 21, 2) IN ('55', '65') 
      AND d.infprot_cstat IN ('100','150') 
      AND d.seq_nitem = '1'              
),
efd_saidas AS (
    /* Passo 2: Chaves de SA�DA escrituradas na EFD (Bloco C) */
    SELECT DISTINCT c100.chv_nfe AS chave_efd
    FROM sped.reg_c100 c100 
    INNER JOIN sped.reg_0000 r0000 ON r0000.id = c100.reg_0000_id 
    INNER JOIN bi.dm_efd_arquivo_valido arqv ON c100.reg_0000_id = arqv.reg_0000_id
    CROSS JOIN parametros p
    WHERE r0000.cnpj = p.cnpj
),
notas_omissas AS (
    /* Passo 3: Identifica��o de Omiss�o via Left Join */
    SELECT d.*
    FROM docs_saida d 
    LEFT JOIN efd_saidas efd ON d.chave_acesso = efd.chave_efd 
    WHERE efd.chave_efd IS NULL
),
max_evento AS (
    /* Passo 4 e 5: Consolida o �ltimo evento da nota */
    SELECT 
        ev.chave_acesso,
        MAX(ev.evento_descevento || ' (' || TO_CHAR(ev.evento_dhevento, 'DD/MM/YY HH24:MI') || ')') 
            KEEP (DENSE_RANK LAST ORDER BY ev.nsu) AS desc_evento
    FROM bi.dm_eventos ev
    WHERE EXISTS (SELECT 1 FROM notas_omissas o WHERE o.chave_acesso = ev.chave_acesso)
    GROUP BY ev.chave_acesso
),
pendencias_fisconforme AS (
    /* Passo 6: Busca relacionamento da chave com a malha */
    SELECT 
        f.chave_acesso,
        f.malhas_id,
        f.referencia_malhas_id
    FROM app_pendencia.vw_fisconforme_chave_nota f
    CROSS JOIN parametros p
    WHERE f.cpf_cnpj = p.cnpj
),
dados_pendencias AS (
    /* Passo 7: Busca os detalhes da malha na tabela de pend�ncias */
    SELECT 
        pen.id AS id_pendencia, 
        pen.malhas_id,
        pen.referencia_malhas_id,
        pen.periodo,
        pen.status,
        pen.data_ciencia
    FROM app_pendencia.pendencias pen
    CROSS JOIN parametros p
    WHERE pen.cpf_cnpj = p.cnpj
),
dados_malha AS (
    /* Passo 8: Busca o cadastro/t�tulo da malha */
    SELECT 
        m.id, 
        m.titulo
    FROM app_pendencia.malhas m 
),
dados_notificacao AS (
    /* NOVO Passo 9: Busca os detalhes da notifica��o enviada para a pend�ncia */
    SELECT 
        notif.id_fisconforme,
        notif.id_notificacao,
        notif.tp_status,
        notif.dt_envio,
        notif.dt_ciencia,
        notif.co_cpf_cnpj_ciencia,
        notif.no_pessoa_ciencia
    FROM bi.fato_det_notificacao notif
    CROSS JOIN parametros p
    WHERE notif.co_cnpj_notif = p.cnpj
)

/* Passo Final: Resultado Consolidado */
SELECT 
    dp.id_pendencia, 
    dn.id_notificacao,
    f.malhas_id,
    m.titulo AS titulo_malha,                    
    dp.periodo,                                  
    --dp.status AS status_pendencia, 
    CASE dp.status
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
        ELSE TO_CHAR(dp.status)
    END AS status_pendencia,
    dn.tp_status AS status_notificacao,
    dn.dt_envio AS data_envio_notificacao,
    NVL(dn.dt_ciencia, dp.data_ciencia) AS data_ciencia_consolidada,
    dn.co_cpf_cnpj_ciencia AS cnpj_cpf_assinante,
    dn.no_pessoa_ciencia AS nome_assinante,
    
    CASE 
        WHEN n.co_modelo = '55' THEN 'Sa�da Omissa - NF-e'
        WHEN n.co_modelo = '65' THEN 'Sa�da Omissa - NFC-e'
        ELSE 'Sa�da Omissa'
    END AS operacao,                             
    n.chave_acesso,
    n.ide_serie AS serie,
    n.nnf,
    n.tot_vnf AS valor_nota,
    n.tot_vicms AS valor_icms,
    n.dhemi AS data_emissao,
    n.infprot_cstat AS status_nfe,
    NVL(mev.desc_evento, 'SEM EVENTO') AS ultimo_evento

  
    -- Dados de Origem/Destino
    --n.co_uf_emit AS uf_origem,
    --n.co_uf_dest AS uf_destino,
    --n.co_emitente AS emitente_auditado,          
    --n.co_destinatario AS destinatario_cliente
FROM notas_omissas n
LEFT JOIN max_evento mev ON n.chave_acesso = mev.chave_acesso
LEFT JOIN pendencias_fisconforme f ON n.chave_acesso = f.chave_acesso
LEFT JOIN dados_pendencias dp ON f.malhas_id = dp.malhas_id AND f.referencia_malhas_id = dp.referencia_malhas_id 
LEFT JOIN dados_malha m ON f.malhas_id = m.id    
LEFT JOIN dados_notificacao dn ON dp.id_pendencia = dn.id_fisconforme -- NOVO JOIN
ORDER BY n.dhemi DESC;