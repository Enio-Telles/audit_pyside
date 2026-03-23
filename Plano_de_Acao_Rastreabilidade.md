# Plano de Ação: Implementação da Rastreabilidade de Produtos

Este documento traduz o [Plano de Arquitetura Revisado: Processamento de Dados Fiscais com Rastreabilidade Total] em etapas sequenciais com tarefas acionáveis (to-dos).

## Etapa 0: Preparação e Mapeamento de Chaves de Origem (SQL)
*(Pré-requisito antes do Módulo 1)*

- [ ] Ajustar a extração SQL da **NFe/NFCe** para garantir a chave única de linha: `chave_acesso + prod_nitem`.
- [ ] Validar a extração SQL do **Bloco H** para garantir que a chave única `reg_h010_id` esteja correta.
- [ ] Ajustar a extração SQL do **C170 (EFD)** para garantir a chave única de linha: `reg_0000_id + num_doc + num_item` (ou garantir a exportação do ID sequencial da tabela).
- [ ] Modificar todos os scripts de extração SQL para criar a nova **Chave de Produto Fonte (`codigo_fonte`)** com a regra: `CNPJ_Emitente + '|' + codigo_produto_original`.

## Etapa 1: Refatoração do Módulo `produtos_unidades.py`
*(Geração da tabela base de movimentações por unidade)*

- [ ] Incluir a leitura/mapeamento do novo campo `codigo_fonte` no script.
- [ ] Alterar a lógica de agregação: agrupar os dados por `codigo_fonte` (CNPJ + Código) em vez de agrupar apenas pela descrição.
- [ ] Manter intactas as lógicas de cálculo de preço médio e identificação de origem (C170/NFe).
- [ ] Validar a geração da saída contendo os campos atualizados: `cnpj_emitente`, `codigo_produto_original`, `codigo_fonte`, `descricao`, `tipo_item`, `ncm`, `cest`, `gtin`, `unid`, `compras`, `vendas`.

## Etapa 2: Refatoração do Módulo `produtos.py`
*(Geração da dimensão de produtos únicos reportados pelas fontes)*

- [ ] Configurar como identificador principal da saída a base da chave única de fontes (`chave_produto`, derivado do `codigo_fonte`).
- [ ] Remover a consolidação/agrupamento definitivo baseado na `descricao_normalizada` (usá-la apenas como metadado para sugestão de grupos futuros).
- [ ] Garantir que a tabela resultante possua 1 linha exata para cada `codigo_fonte` distinto, preservando a lista de seus atributos originais reportados (NCMs, CESTs, Unidades).

## Etapa 3: Ajuste/Criação do Módulo `produtos_agrupados.py`
*(Master Data Management e construção das Tabelas de Ligação)*

- [ ] Implementar a lógica de pré-agrupamento/sugestão (ex: registrar um mesmo `id_agrupado` para itens de mesma `descricao_normalizada` + `NCM`, ou `GTIN` idêntico).
- [ ] Prover/Preparar a rotina para revisão manual: o usuário define e consolida o grupo definitivo.
- [ ] Gerar a tabela Mestra **`produtos_agrupados`** com os campos: `id_agrupado` (Surrogate Key, ex: PROD_MSTR_001), `descr_padrao`, `ncm_padrao`, `cest_padrao`, `gtin_padrao`.
- [ ] Gerar a tabela Ponte/Bridge **`map_produto_agrupado`** contendo o relacionamento: `chave_produto` (do Mód. 2) -> `id_agrupado` (da Mestra).

## Etapa 4: Refatoração do Módulo `fatores_conversao.py`
*(Cálculos de conversão amarrados à tabela agrupada)*

- [ ] Alterar o carregamento de dados para fazer o JOIN da tabela base de movimentações (Mód. 1) com o `id_agrupado` através da ponte `map_produto_agrupado`.
- [ ] Estruturar a matriz final com as colunas: `id_agrupado`, `unid_origem`, `unid_ref` (padrão) e `fator`.
- [ ] Calcular/Manter o `fator` como a relação matemática definida (P.M. Compra da Unid. Origem / P.M. Compra da Unid. Ref.).
- [ ] Implementar rotina/log alertando quais agrupamentos/unidades não possuíam preço de compra.
- [ ] Habilitar funcionalidade para forçar/inserir fator de conversão manualmente (ex: "Caixa -> Unidade = 12").

## Etapa 5: Criação do NOVO Módulo `enriquecimento_fontes.py`
*(A Rastreabilidade e Linha Dourada na Prática)*

Desenvolver este novo script aplicando as seguintes etapas para cada fonte Parquet (NFe/NFCe, Bloco_H, C170):
- [ ] Carregar a base bruta original (`nfe_base.parquet`, por exemplo).
- [ ] Construir a coluna temporária `codigo_fonte` (ex: `co_emitente + '|' + prod_cprod`).
- [ ] Executar LEFT JOIN com `map_produto_agrupado` para obter o `id_agrupado`.
- [ ] Executar LEFT JOIN com a tabela `produtos_agrupados` para trazer os campos padronizados (`descr_padrao`, `ncm_padrao`, etc).
- [ ] Executar LEFT JOIN com a tabela `fatores_conversao` cruzando por `id_agrupado` + unidade da linha (ex: `prod_ucom`).
- [ ] Aplicar a regra de negócio para a Quantidade:
    - Se fator encontrado: `qtd_padronizada = prod_qcom * fator`
    - Se não encontrado/uni. referência: `qtd_padronizada = prod_qcom`
- [ ] Aplicar a regra de negócio para o Valor Unitário: `vuncom_padronizado = prod_vuncom / fator` (tratar casos não encontrados).
- [ ] Salvar as bases em Parquets Enriquecidos mantendo ABSOLUTAMENTE TODAS as colunas identificadoras originais e adicionando as novas colunas mapeadas.

## Etapa 6: Adaptação da Interface Gráfica (PySide6)
- [ ] Adicionar novas colunas (`codigo_fonte`, `id_agrupado`, `qtd_padronizada`, etc.) nas views/grades de dados.
- [ ] Criar/Ajustar interface de "Revisão de Agrupamentos" para permitir confirmação manual do `id_agrupado` associado a uma `chave_produto`.
- [ ] Criar/Ajustar aba de "Fatores de Conversão" guiada pela interface, com suporte a preenchimento manual de fatores desconhecidos.
- [ ] Criar visão/botão de "Auditoria de Fio de Ouro": ao clicar em um `id_agrupado` na tela principal, exibir as linhas originais correspondentes.

## Etapa 7: Testes, Auditoria e Homologação
- [ ] Executar a pipeline inteira para um cliente/CNPJ de amostra.
- [ ] Testar um cenário de "Mudança de Ideia": alterar o agrupamento de um produto no Módulo 3 (via intervenção manual) e provar que rodando o Módulo 5, o Parquet é recalculado sem erros ou dependências residuais.
- [ ] Validar um cenário de Auditoria ("Fio de Ouro"): Escolher um `id_agrupado`, filtrar nos Parquets Enriquecidos, e constatar a validade da rastreabilidade até o documento fiscal de origem.
