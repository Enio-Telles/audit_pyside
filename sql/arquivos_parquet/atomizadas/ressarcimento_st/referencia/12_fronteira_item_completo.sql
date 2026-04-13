/* Gerado para integração com audit_react + C176 + Fronteira */
WITH ENTRADA_ESCOLHIDA AS (
    SELECT * FROM TMP_ENTRADA_ESCOLHIDA
),
CHAVES_ITENS_ENTRADA_ESCOLHIDOS AS (
    SELECT DISTINCT
        e.chave_saida,
        e.num_item_saida,
        e.cod_item_saida,
        e.chave_nfe_ultima_entrada,
        e.prod_nitem_entrada
    FROM ENTRADA_ESCOLHIDA e
    WHERE e.chave_nfe_ultima_entrada IS NOT NULL
      AND e.prod_nitem_entrada IS NOT NULL
)
SELECT
    ce.chave_saida,
    ce.num_item_saida,
    ce.cod_item_saida,
    ce.chave_nfe_ultima_entrada,
    ce.prod_nitem_entrada,
    MAX(TO_DATE(nl.it_da_entrada, 'YYYYMMDD')) AS fronteira_dt_entrada,
    MAX(nl.it_co_comando) AS fronteira_comando,
    COUNT(DISTINCT lanc.it_nu_guia_lancamento) AS fronteira_qtd_guias,
    MIN(lanc.it_nu_guia_lancamento) AS fronteira_guia_exemplo,
    MAX(lanc.it_co_receita) AS fronteira_receita,
    MAX(lanc.it_va_principal_original) AS fronteira_valor_devido,
    MAX(CASE WHEN lanc.it_co_receita IS NULL THEN 0 ELSE lanc.it_va_total_pgto_efetuado END) AS fronteira_valor_pago,
    MAX(
        CASE
            WHEN lanc.it_co_situacao_lancamento IN ('00', '03') THEN 'PAGO'
            WHEN lanc.it_co_situacao_lancamento = '28' THEN 'BAIXA_DE_ACORDO_COM_DEC_11430_2004'
            WHEN lanc.it_co_situacao_lancamento = '68' THEN 'SUSPENSO'
            WHEN lanc.it_co_situacao_lancamento = '13' THEN 'CORRECAO_NO_PAGAMENTO_ORIGINAL'
            WHEN lanc.it_co_situacao_lancamento = '02' THEN 'PAGO_A_MENOR'
            WHEN lanc.it_co_situacao_lancamento = '05' THEN 'PARCELADO'
            WHEN lanc.it_co_situacao_lancamento = '08' THEN 'INSCRITO_EM_DA'
            WHEN lanc.it_co_situacao_lancamento = '10' THEN 'BAIXA_PROVISORIA'
            WHEN lanc.it_co_situacao_lancamento = '14' THEN 'LANCAMENTO_EXCLUIDO'
            WHEN lanc.it_co_situacao_lancamento = '32' THEN 'COMPENSACAO'
            WHEN lanc.it_co_situacao_lancamento = '38' THEN 'LIQUIDACAO_DESVINCULADA_CONTA_GRAFICA'
            WHEN lanc.it_co_situacao_lancamento = '46' THEN 'SUSPENSAO_JUDICIAL'
            WHEN lanc.it_co_situacao_lancamento = '50' THEN 'LANCAMENTO_INDEVIDO'
            WHEN lanc.it_co_situacao_lancamento IS NULL THEN 'SEM_SITUACAO'
            ELSE 'VERIFICAR'
        END
    ) AS fronteira_situacao,
    MAX(lanc_item.it_co_produto) AS fronteira_co_sefin_lanc,
    MAX(prod_sefin.it_no_produto) AS fronteira_nome_co_sefin,
    MAX(lanc_item.it_vl_merc_item) AS fronteira_vl_merc_item,
    MAX(lanc_item.it_vl_merc_bc_item) AS fronteira_vl_bc_merc_item,
    MAX(lanc_item.it_aliq_item) AS fronteira_aliq_item,
    MAX(lanc_item.it_vl_tot_debito_item) AS fronteira_vl_tot_debito_item,
    MAX(lanc_item.it_vl_credito_rateio) AS fronteira_vl_credito_rateio,
    MAX(lanc_item.it_vl_icms_recolher) AS fronteira_vl_icms_recolher,
    MAX(m.it_in_produto_st) AS fronteira_ind_produto_st,
    MAX(m.it_in_cest_st) AS fronteira_ind_cest_st,
    MAX(m.it_pc_interna) AS fronteira_pc_interna_merc
FROM CHAVES_ITENS_ENTRADA_ESCOLHIDOS ce
JOIN sitafe.sitafe_nota_fiscal nf
  ON nf.it_nu_identificao_nf_e = ce.chave_nfe_ultima_entrada
JOIN sitafe.sitafe_nf_lancamento nl
  ON nl.it_nu_identificacao_nf = nf.it_nu_identificacao_nf
JOIN sitafe.sitafe_nfe_item item
  ON item.it_nu_chave_acesso = ce.chave_nfe_ultima_entrada
 AND TO_NUMBER(item.it_nu_item) = ce.prod_nitem_entrada
LEFT JOIN sitafe.sitafe_lancamento lanc
  ON lanc.it_nu_guia_lancamento = nl.it_nu_guia_lancamento
LEFT JOIN sitafe.sitafe_lancamento_item lanc_item
  ON lanc_item.it_nu_identificacao_ndf = nl.it_nu_identificacao_ndf
 AND lanc_item.it_co_produto = item.it_co_sefin
LEFT JOIN sitafe.sitafe_mercadoria m
  ON m.it_co_sefin = lanc_item.it_co_produto
LEFT JOIN sitafe.sitafe_produto_sefin prod_sefin
  ON prod_sefin.it_co_sefin = lanc_item.it_co_produto
GROUP BY
    ce.chave_saida,
    ce.num_item_saida,
    ce.cod_item_saida,
    ce.chave_nfe_ultima_entrada,
    ce.prod_nitem_entrada;
