# Matriz de dependências das consultas

## Blocos-base
- `00_parametros_normalizados.sql`  
  Normaliza CNPJ/IE/NOME.
- `01_lookup_contribuinte.sql`  
  Pesquisa candidatos a contribuinte.
- `02_base_contribuinte.sql`  
  Resolve o contribuinte selecionado e vira base para os demais blocos.

## Dependências principais por domínio

### Cadastro
- `10_cadastro_principal.sql` depende de `02_base_contribuinte.sql`
- `11_endereco_cadastral.sql` depende de `02_base_contribuinte.sql`
- `12_endereco_documental_nfe.sql` depende de `02_base_contribuinte.sql`
- `13_cnae_principal.sql` depende de `02_base_contribuinte.sql`
- `14_cnae_secundario.sql` depende de `02_base_contribuinte.sql`
- `15_regime_pagamento_historico.sql` depende de `02_base_contribuinte.sql`
- `16_historico_situacao.sql` depende de `02_base_contribuinte.sql` para resolver a IE
- `17_contador_historico.sql` depende de `02_base_contribuinte.sql` para resolver a IE
- `18_historico_fac.sql` depende de `02_base_contribuinte.sql` para resolver a IE
- `19_processos_administrativos.sql` depende de `02_base_contribuinte.sql`
- `1A_veiculos_sitafe.sql` depende de `02_base_contribuinte.sql`
- `1B_veiculos_detran.sql` depende de `02_base_contribuinte.sql`

### Societário
- `20_socios_historico.sql` depende de `02_base_contribuinte.sql`
- `21_socios_atuais.sql` depende de `20_socios_historico.sql`
- `22_empresas_dos_socios_base.sql` depende de `20_socios_historico.sql`
- `23_inadimplencia_por_cnpj.sql` é independente
- `24_empresas_dos_socios_resultado.sql` depende de `22_empresas_dos_socios_base.sql` + `23_inadimplencia_por_cnpj.sql`

### Documentos fiscais
- `30_nfe_movimento_item.sql` depende de `02_base_contribuinte.sql`
- `31_nfe_entrada_itens.sql` depende de `30_nfe_movimento_item.sql`
- `32_nfe_saida_itens.sql` depende de `30_nfe_movimento_item.sql`
- `33_vaf_anual.sql` depende de `30_nfe_movimento_item.sql`
- `34_nfe_entrada_quantidade_uf.sql` depende de `02_base_contribuinte.sql`
- `35_mdfe_relacionado.sql` depende de `02_base_contribuinte.sql`
- `36_ip_transmissor_nfe.sql` depende de `02_base_contribuinte.sql`

### Arrecadação e regularidade
- `40_conta_corrente_base.sql` depende de `02_base_contribuinte.sql`
- `41_conta_corrente_agregado.sql` depende de `40_conta_corrente_base.sql`
- `42_regime_especial.sql` depende de `02_base_contribuinte.sql`
- `43_parcelamentos.sql` depende de `02_base_contribuinte.sql`
- `44_dimp_cartao_periodo.sql` depende de `02_base_contribuinte.sql`
- `45_dimp_saida_documento_periodo.sql` depende de `02_base_contribuinte.sql`
- `46_dimp_confronto_periodo.sql` depende de `44_dimp_cartao_periodo.sql` + `45_dimp_saida_documento_periodo.sql`

### Fiscalização e conformidade
- `50_vistoria_app.sql` depende de `02_base_contribuinte.sql`
- `51_vistoria_sitafe.sql` depende de `02_base_contribuinte.sql`
- `52_vistorias_agregado.sql` depende de `50_vistoria_app.sql` + `51_vistoria_sitafe.sql`
- `53_acao_fiscal_bi.sql` depende de `02_base_contribuinte.sql`
- `54_dsf_por_identificacao.sql` depende de `02_base_contribuinte.sql`
- `55_dsf_por_auto.sql` depende de `02_base_contribuinte.sql`
- `56_acoes_fiscais_agregado.sql` depende de `53_acao_fiscal_bi.sql` + `54_dsf_por_identificacao.sql` + `55_dsf_por_auto.sql`
- `57_acoes_fiscais_relacionadas.sql` depende de `02_base_contribuinte.sql`
- `58_autos_infracao.sql` depende de `57_acoes_fiscais_relacionadas.sql`
- `59_fisconforme_base.sql` depende de `02_base_contribuinte.sql`
- `5A_det_notificacoes.sql` depende de `02_base_contribuinte.sql`

## Recomendação
Na implementação em Polars, persistir cada consulta em um dataset parquet separado por domínio e consolidar somente no consumo final.
