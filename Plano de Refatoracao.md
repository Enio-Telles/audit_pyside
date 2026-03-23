Refatorar o projeto de processamento de dados fiscais para uma arquitetura modular, onde cada tabela Ã© gerada por funÃ§Ãµes especÃ­ficas em arquivos dedicados. O foco Ã© garantir a rastreabilidade dos produtos desde a unidade bÃ¡sica atÃ© o agrupamento final e cÃ¡lculo de fatores de conversÃ£o.
ðŸ“ Mapeamento de Colunas (ReferÃªncia Geral)
Utilize o seguinte mapeamento para padronizar as colunas das diferentes fontes para o formato de destino.
    â€¢ codigo: prod_cprod (NFe/NFCe) | cod_item (C170) | codigo_produto (Bloco H)
    â€¢ descricao: prod_xprod (NFe/NFCe) | descr_item (C170) | descricao_produto (Bloco H)
    â€¢ descr_compl: [Nulo] (NFe/NFCe) | descr_compl (C170) | [Nulo] (Bloco H)
    â€¢ tipo_item: [Nulo] (NFe/NFCe) | tipo_item (C170) | tipo_item (Bloco H)
    â€¢ ncm: prod_ncm (NFe/NFCe) | cod_ncm (C170) | cod_ncm (Bloco H)
    â€¢ cest: prod_cest (NFe/NFCe) | cest (C170) | cest (Bloco H)
    â€¢ gtin: prod_ceantrib (NFe/NFCe) ou, quando ausente, prod_cean (NFe/NFCe) | cod_barra (C170) | cod_barra (Bloco H)
    • Regra de separacao: cest e cod_barra/gtin sao campos distintos e nao devem ser mesclados entre si.
    â€¢ unid: prod_ucom (NFe/NFCe) | unid (C170) | unidade_medida (Bloco H)

1. MÃ³dulo: produtos_unidades.py
Objetivo: Gerar a tabela base de movimentaÃ§Ãµes por unidade.
    â€¢ Campos: codigo, descricao, descr_compl, tipo_item, ncm, cest, co_sefin_item, gtin, unid, compras, vendas.
    â€¢ Fontes: Tabelas NFe, NFCe, C170 e bloco_h.
    â€¢ LÃ³gica de Compras: Identificar no C170 quando ind_oper = 0, o cfop constar em referencias\cfop\cfop_bi.parquet com operacao_mercantil = 'X'. O preÃ§o Ã© o valor_item.
    â€¢ LÃ³gica de Vendas: Identificar em NFe e NFCe quando co_emitente = cnpj, tipo_operacao = '1 - saida', e o co_cfop constar no parquet de referÃªncia.
    â€¢ CÃ¡lculo de PreÃ§o de Venda: prod_vprod + prod_vfrete + prod_vseg + prod_voutro - prod_vdesc.
2. MÃ³dulo: produtos.py
Objetivo: Gerar a tabela de produtos normalizados e Ãºnicos.
    â€¢ Campos: chave_produto, descricao_normalizada, descricao, lista_desc_compl, lista_codigos, lista_tipo_item, lista_ncm, lista_cest, lista_gtin, lista_co_sefin, lista_unid.
    â€¢ Identificador: chave_produto deve seguir o padrÃ£o id_produto_1, id_produto_2, etc.
    â€¢ Regra de NormalizaÃ§Ã£o: A descricao_normalizada deve remover acentos, espaÃ§os extras no inÃ­cio/fim e ser convertida para MAIÃšSCULO.
    â€¢ Agrupamento: Agrupar os dados por descricao_normalizada baseando-se nas tabelas C170, bloco_h, NFe e NFCe.
3. MÃ³dulo de Agrupamento Manual e Final (produtos_agrupados)
FunÃ§Ã£o: produtos_agrupados()
    â€¢ Objetivo: Permitir a uniÃ£o manual de linhas da tabela produtos em uma nova tabela produtos_agrupados.
    â€¢ Campos: id_agrupado (id_agrupado_1...), lista_chave_produto, descr_padrao, ncm_padrao, cest_padrao, gtin_padrao,  lista_co_sefin, co_sefin_padrao, lista_unidades, co_sefin_divergentes.
    â€¢ Co_sefin_divergentes deve ser true (quando existir mais de um co_sefin diferente do outro na lista_sefin) ou false (quando sÃ³ tiver um co_sefin na lista_co_sefin)
    â€¢ LÃ³gica de Atributos "PadrÃ£o": Definir o valor que mais ocorre nas fontes (emitidas pelo CNPJ).
        â—‹ Desempate: 1Âº Maior quantidade de campos preenchidos (NCM, CEST, GTIN); 2Âº Tamanho da descriÃ§Ã£o.
    â€¢ IntegraÃ§Ã£o Final: Gerar a tabela produtos_final unindo produtos com produtos_agrupados. Esta tabela deve ser re-calculÃ¡vel caso os agrupamentos manuais sejam revisados, revisando especialmente co_sefin e valores-padrÃ£o.
4. MÃ³dulo: fatores_conversao.py
Objetivo: Calcular a relaÃ§Ã£o entre diferentes unidades de medida do mesmo produto.
    â€¢ Campos: id_produtos, descr_padrao, unid, unid_ref, fator.
    â€¢ DefiniÃ§Ã£o de unid_ref: A unidade que mais ocorre nas tabelas base (ou definida manualmente pelo usuÃ¡rio).
    â€¢ CÃ¡lculo do Fator:
        1. Calcular o preÃ§o mÃ©dio de compra por unidade (funÃ§Ã£o precos_medios_produtos_final).
        2. Fator = PreÃ§o MÃ©dio Compra da Unidade\PreÃ§o MÃ©dio Compra da Unid ou Fator = PreÃ§o MÃ©dio Venda da Unidade\PreÃ§o MÃ©dio Compra da Unid
    â€¢ Funcionalidade: A tabela deve permitir preenchimento automÃ¡tico, mas aceitar revisÃµes manuais do usuÃ¡rio.
Diretrizes de ImplementaÃ§Ã£o
    1. Utilize Pandas ou Polars para a manipulaÃ§Ã£o dos dados.
    2. Garanta que as funÃ§Ãµes de agregaÃ§Ã£o tratem valores nulos e erros de conversÃ£o de tipos.
    3. Crie logs para identificar produtos que nÃ£o possuem preÃ§o mÃ©dio de compra (o que impediria o cÃ¡lculo automÃ¡tico do fator).


## Checklist de Fechamento (Pendencias)

- [x] Incluir Bloco H no `produtos_unidades` com mapeamento de: `codigo_produto`, `descricao_produto`, `cod_ncm`, `cest`, `cod_barra`, `tipo_item`, `unidade_medida`/`unidade_media`, `quantidade`, `valor_item`.
- [ ] Validar no dado real a existencia das colunas do Bloco H e ajustar variacoes finais de schema (se houver).
- [x] Confirmar regra de mapeamento: `cest` = `cest`/`prod_cest` e `cod_barra` (gtin) = `cod_barra`/`prod_ceantrib`/`prod_cean` (fallback), mantendo campos distintos.
- [x] Criar tabela `produtos_final_{cnpj}.parquet` integrando `produtos` + `produtos_agrupados`.
- [x] Garantir recÃ¡lculo deterministico de `produtos_final` apos revisoes manuais de agrupamento.
- [x] Implementar funcao dedicada `precos_medios_produtos_final` e reutilizar em `fatores_conversao`.
- [x] Adicionar logs de itens sem preco medio de compra (arquivo de log por CNPJ + contagem por produto/unidade).
- [x] Implementar no `ServicoAgregacao` os metodos usados pela UI: `recalcular_todos_padroes` e `recalcular_valores_totais`.
- [ ] Criar testes unitarios para: leitor Bloco H, agregacao manual, recÃ¡lculo de padroes e fatores de conversao.
- [x] Atualizar README com fluxo final (`produtos_final`) e comportamento do Bloco H.




