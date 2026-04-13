/*
   CONSULTA NFC-e COM COMENTÁRIOS EXPLICATIVOS
   Base conceitual: MOC 7.0 - Anexo I (Leiaute e Regras de Validação da NF-e e da NFC-e).
   Observação: os comentários foram alinhados aos campos equivalentes do MOC quando a correspondência foi clara.
   Campos auxiliares/normalizados do BI foram identificados como internos/técnicos.
*/

WITH parametros AS (
    SELECT
        :cnpj AS cnpj_filtro
    FROM
        dual
)
SELECT
    CASE
        WHEN d.co_emitente = p.cnpj_filtro
             AND d.co_tp_nf = 1 THEN
            '1 - SAIDA'
        WHEN d.co_emitente = p.cnpj_filtro
             AND d.co_tp_nf = 0 THEN
            '0 - ENTRADA'
        WHEN d.co_destinatario = p.cnpj_filtro
             AND d.co_tp_nf = 1 THEN
            '0 - ENTRADA'
        WHEN d.co_destinatario = p.cnpj_filtro
             AND d.co_tp_nf = 0 THEN
            '1 - SAIDA'
        ELSE
            'INDEFINIDO'
    END AS tipo_operacao, -- Campo derivado da consulta: classifica a operação como entrada/saída para o CNPJ filtrado
    d.co_destinatario, -- CNPJ/CPF do destinatário da NFC-e (campo BI correspondente ao grupo dest)
    d.co_emitente, -- CNPJ/CPF do emitente da NFC-e (campo BI correspondente ao grupo emit)
    p.cnpj_filtro, -- Parâmetro de entrada da consulta: CNPJ analisado
    d.nsu, -- NSU do documento/processamento no ambiente de origem (campo interno do BI)
    d.chave_acesso, -- Chave de acesso da NFC-e (44 dígitos)
    d.prod_nitem, -- nItem: número sequencial do item no detalhamento da NFC-e
    d.co_emitente || '|' || d.prod_cprod AS codigo_fonte, -- Nova chave de produto fonte
    d.ide_co_cuf, -- cUF: código da UF do emitente da NFC-e
    d.ide_co_indpag, -- indPag: indicador da forma de pagamento (campo excluído do leiaute 4.0)
    d.ide_co_mod, -- mod: código do modelo do documento fiscal (55=NF-e, 65=NFC-e)
    d.ide_serie, -- serie: série da NFC-e
    d.nnf, -- nNF: número da NFC-e
    d.dhemi, -- dhEmi: data e hora de emissão da NFC-e
    d.co_tp_nf, -- tpNF: tipo de operação da NFC-e (0=entrada, 1=saída)
    d.co_iddest, -- idDest: identificador do local de destino da operação
    d.co_cmun_fg, -- cMunFG: código do município de ocorrência do fato gerador
    d.co_tpemis, -- tpEmis: tipo de emissão da NFC-e
    d.co_finnfe, -- finNFe: finalidade de emissão da NFC-e
    d.co_indpres, -- indPres: indicador de presença do comprador no momento da operação
    d.co_indfinal, -- indFinal: indica se a operação é com consumidor final
    d.xnome_emit, -- xNome: razão social/nome do emitente
    d.xfant_emit, -- xFant: nome fantasia do emitente
    d.co_uf_emit, -- UF: sigla da UF do emitente
    d.co_cad_icms_emit, -- IE: inscrição estadual do emitente
    d.co_crt, -- CRT: código de regime tributário do emitente
    d.xlgr_emit, -- xLgr: logradouro do emitente
    d.nro_emit, -- nro: número do endereço do emitente
    d.xcpl_emit, -- xCpl: complemento do endereço do emitente
    d.xbairro_emit, -- xBairro: bairro do emitente
    d.co_cmun_emit, -- cMun: código do município do emitente
    d.xmun_emit, -- xMun: nome do município do emitente
    d.cep_emit, -- CEP: CEP do emitente
    d.cpais_emit, -- cPais: código do país do emitente
    d.xpais_emit, -- xPais: nome do país do emitente
    d.fone_emit, -- fone: telefone do emitente
    d.xnome_dest, -- xNome: razão social/nome do destinatário
    d.co_uf_dest, -- UF: sigla da UF do destinatário
    d.co_indiedest, -- indIEDest: indicador da IE do destinatário
    d.xlgr_dest, -- xLgr: logradouro do destinatário
    d.nro_dest, -- nro: número do endereço do destinatário
    d.xcpl_dest, -- xCpl: complemento do endereço do destinatário
    d.xbairro_dest, -- xBairro: bairro do destinatário
    d.co_cmun_dest, -- cMun: código do município do destinatário
    d.xmun_dest, -- xMun: nome do município do destinatário
    d.cep_dest, -- CEP: CEP do destinatário
    d.cpais_dest, -- cPais: código do país do destinatário
    d.xpais_dest, -- xPais: nome do país do destinatário
    d.fone_dest, -- fone: telefone do destinatário
    d.prod_cprod, -- cProd: código do produto ou serviço
    d.prod_cean, -- cEAN: GTIN do produto (antigo EAN/código de barras)
    d.prod_xprod, -- xProd: descrição do produto ou serviço
    d.prod_ncm, -- NCM: código NCM com 8 dígitos
    d.co_cfop, -- CFOP: código fiscal de operações e prestações do item
    d.prod_ucom, -- uCom: unidade comercial do produto
    d.prod_qcom, -- qCom: quantidade comercial
    d.prod_vuncom, -- vUnCom: valor unitário de comercialização
    d.prod_vprod, -- vProd: valor total bruto do item
    d.prod_ceantrib, -- cEANTrib: GTIN da unidade tributável
    d.prod_utrib, -- uTrib: unidade tributável
    d.prod_qtrib, -- qTrib: quantidade tributável
    d.prod_vuntrib, -- vUnTrib: valor unitário de tributação
    d.prod_vfrete, -- vFrete: valor do frete do item
    d.prod_vseg, -- vSeg: valor do seguro do item
    d.prod_vdesc, -- vDesc: valor do desconto do item
    d.prod_voutro, -- vOutro: outras despesas acessórias do item
    d.prod_indtot, -- indTot: indica se o valor do item compõe o total da NFC-e
    d.icms_csosn, -- CSOSN: código de situação da operação do Simples Nacional
    d.icms_cst, -- CST: código de situação tributária do ICMS
    d.icms_modbc, -- modBC: modalidade de determinação da base de cálculo do ICMS
    d.icms_modbcst, -- modBCST: modalidade de determinação da base de cálculo do ICMS ST
    d.icms_motdesicms, -- motDesICMS: motivo da desoneração do ICMS
    d.icms_orig, -- orig: origem da mercadoria
    d.icms_pbcop, -- pBCOp: percentual da base de cálculo da operação própria
    d.icms_pcredsn, -- pCredSN: alíquota aplicável ao crédito do Simples Nacional
    d.icms_pdif, -- pDif: percentual do diferimento do ICMS
    d.icms_picms, -- pICMS: alíquota do ICMS
    d.icms_picmsst, -- pICMSST: alíquota do ICMS ST
    d.icms_pmvast, -- pMVAST: percentual da margem de valor agregado do ICMS ST
    d.icms_predbc, -- pRedBC: percentual de redução da base de cálculo do ICMS
    d.icms_predbcst, -- pRedBCST: percentual de redução da base de cálculo do ICMS ST
    d.icms_ufst, -- UFST: UF para a qual é devido o ICMS ST
    d.icms_vbc, -- vBC: valor da base de cálculo do ICMS
    d.icms_vbcst, -- vBCST: valor da base de cálculo do ICMS ST
    d.icms_vbcstdest, -- vBCSTDest: valor da base de cálculo do ICMS ST para a UF de destino
    d.icms_vbcstret, -- vBCSTRet: valor da base de cálculo do ICMS ST retido anteriormente
    d.icms_vcredicmssn, -- vCredICMSSN: valor do crédito de ICMS do Simples Nacional
    d.icms_vicms, -- vICMS: valor do ICMS
    d.icms_vicmsdeson, -- vICMSDeson: valor do ICMS desonerado
    d.icms_vicmsdif, -- vICMSDif: valor do ICMS diferido
    d.icms_vicmsop, -- vICMSOp: valor do ICMS da operação
    d.icms_vicmsst, -- vICMSST: valor do ICMS ST
    d.icms_vicmsstdest, -- vICMSSTDest: valor do ICMS ST para a UF de destino
    d.icms_vicmsstret, -- vICMSSTRet: valor do ICMS ST retido anteriormente
    d.icms_vbcfcp, -- vBCFCP: base de cálculo do FCP
    d.icms_pfcp, -- pFCP: percentual do FCP
    d.icms_vfcp, -- vFCP: valor do FCP
    d.icms_vbcfcpst, -- vBCFCPST: base de cálculo do FCP retido por ST
    d.icms_pfcpst, -- pFCPST: percentual do FCP retido por ST
    d.icms_vfcpst, -- vFCPST: valor do FCP retido por ST
    d.icms_vbcufdest, -- vBCUFDest: base de cálculo do ICMS para a UF de destino
    d.icms_vbcfcpufdest, -- vBCFCPUFDest: base de cálculo do FCP na UF de destino
    d.icms_pfcpufdest, -- pFCPUFDest: percentual do FCP na UF de destino
    d.icms_picmsufdest, -- pICMSUFDest: alíquota interna do ICMS na UF de destino
    d.icms_picmsinter, -- pICMSInter: alíquota interestadual do ICMS
    d.icms_picmsinterpart, -- pICMSInterPart: percentual de partilha do ICMS interestadual
    d.icms_vfcpufdest, -- vFCPUFDest: valor do FCP para a UF de destino
    d.icms_vicmsufdest, -- vICMSUFDest: valor do ICMS para a UF de destino
    d.icms_vicmsufremet, -- vICMSUFRemet: valor do ICMS para a UF do remetente
    d.icms_pst, -- pST: alíquota suportada pelo consumidor final
    d.icms_vbcfcpstret, -- vBCFCPSTRet: base de cálculo do FCP retido anteriormente
    d.icms_pfcpstret, -- pFCPSTRet: percentual do FCP retido anteriormente por ST
    d.icms_vfcpstret, -- vFCPSTRet: valor do FCP retido anteriormente por ST
    d.icms_predbcefet, -- pRedBCEfet: percentual de redução da base de cálculo efetiva
    d.icms_vbcefet, -- vBCEfet: valor da base de cálculo efetiva
    d.icms_picmsefet, -- pICMSEfet: alíquota do ICMS efetiva
    d.icms_vicmsefet, -- vICMSEfet: valor do ICMS efetivo
    d.tot_vbc, -- vBC: base de cálculo total do ICMS
    d.tot_vicms, -- vICMS: valor total do ICMS
    d.tot_vicmsdeson, -- vICMSDeson: valor total do ICMS desonerado
    d.tot_vbcst, -- vBCST: base de cálculo total do ICMS ST
    d.tot_vst, -- vST: valor total do ICMS ST
    d.tot_vprod, -- vProd: valor total dos produtos e serviços
    d.tot_vfrete, -- vFrete: valor total do frete
    d.tot_vseg, -- vSeg: valor total do seguro
    d.tot_vdesc, -- vDesc: valor total do desconto
    d.tot_vii, -- vII: valor total do Imposto de Importação
    d.tot_vipi, -- vIPI: valor total do IPI
    d.tot_vpis, -- vPIS: valor total do PIS
    d.tot_vcofins, -- vCOFINS: valor total da COFINS
    d.tot_voutro, -- vOutro: total de outras despesas acessórias
    d.tot_vnf, -- vNF: valor total da NFC-e
    d.tot_vtottrib, -- vTotTrib: valor aproximado total dos tributos
    d.tot_vfcpufdest, -- vFCPUFDest: valor total do FCP para a UF de destino
    d.tot_vicmsufdest, -- vICMSUFDest: valor total do ICMS para a UF de destino
    d.tot_vicmsufremet, -- vICMSUFRemet: valor total do ICMS para a UF do remetente
    d.tot_vfcp, -- vFCP: valor total do FCP
    d.tot_vfcpst, -- vFCPST: valor total do FCP retido por ST
    d.tot_vfcpstret, -- vFCPSTRet: valor total do FCP retido anteriormente por ST
    d.tot_vipidevol, -- vIPIDevol: valor total do IPI devolvido
    d.infprot_cstat, -- cStat: código do resultado de processamento/autorização da NFC-e
    d.icms_csosn_a, -- Campo auxiliar do BI: CSOSN em formato alternativo/ajustado
    d.icms_cst_a, -- Campo auxiliar do BI: CST ICMS em formato alternativo/ajustado
    d.dt_gravacao, -- Campo interno do BI: data/hora de gravação do registro
    d.seq_nitem, -- Campo interno do BI: sequência técnica do item
    d.dhemi_hora, -- Campo derivado: componente hora da data/hora de emissão
    d.status_carga_campo_fcp, -- Campo interno do BI: status de carga dos campos de FCP
    d.prod_cest -- CEST: código especificador da substituição tributária
FROM
    bi.fato_nfce_detalhe    d,
    parametros              p
WHERE
    ( d.co_destinatario = p.cnpj_filtro OR d.co_emitente = p.cnpj_filtro )
     AND d.dhemi >= DATE '2020-01-01'