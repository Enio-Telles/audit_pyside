- Complementos nao devem ser incorporados em `lista_descricoes`.
- Filtros textuais da aba `Agregacao` continuam considerando as duas colunas para nao perder rastreabilidade.
- Rotinas que reconciliam grupos por descricao podem usar `lista_descricoes` e `lista_desc_compl`, mas cada campo permanece semanticamente separado.

## Identificação do CO_SEFIN (SITAFE)

A identificação do `co_sefin_item` ocorre na etapa `item_unidades` e é baseada no match de códigos fiscais contra as tabelas oficiais do SITAFE (armazenadas em `dados/referencias/CO_SEFIN/`). O sistema utiliza a seguinte ordem de precedência (`pl.coalesce`):

1. **CEST + NCM**: Match exato nos dois campos (`sitafe_cest_ncm.parquet`).
2. **Somente CEST**: Match pelo código CEST (`sitafe_cest.parquet`).
3. **Somente NCM**: Match pelo código NCM (`sitafe_ncm.parquet`).

Este código é a chave para determinar a carga tributária, alíquotas de ST e reduções de base de cálculo.

## Colunas de descrição

Na camada de agregação, as descrições principais e os complementos devem ficar separados:

- `lista_descricoes`: contém apenas descrições principais do produto/grupo.
- `lista_desc_compl`: contém apenas descrições complementares vindas de `descr_compl`.
- `lista_itens_agrupados`: mostra as descrições-base dos itens hoje vinculados ao grupo.
- `ids_origem_agrupamento`: registra quais `id_agrupado` deram origem ao grupo atual.

## Reversao de agrupamentos

- Um agrupamento manual passa a registrar no log os grupos de origem e os itens envolvidos.
- A reversao restaura os grupos de origem a partir desse snapshot.
- A reversao usa o `id_agrupado` de destino preservado no merge manual; por isso o merge nao renumera mais todos os grupos.

---

## Auditoria e Consistência de id_agrupado em C170, NFe, NFCe e Bloco H

Para garantir rastreabilidade e integridade fiscal, todas as linhas das tabelas **c170**, **nfe**, **nfce** e **bloco_h** devem possuir o campo `id_agrupado` preenchido.

- A rotina `src/transformacao/rastreabilidade_produtos/fontes_produtos.py` centraliza a geração dos arquivos:
  - `c170_agr_<cnpj>.parquet`
  - `bloco_h_agr_<cnpj>.parquet`
  - `nfe_agr_<cnpj>.parquet`
  - `nfce_agr_<cnpj>.parquet`
- Durante a geração, qualquer linha sem `id_agrupado` é:
  1. Exportada para um arquivo de auditoria (ex: `c170_agr_sem_id_agrupado_<cnpj>.parquet`)
  2. Excluída da saída final
  3. Registrada em log com aviso e caminho do arquivo de auditoria
- Todos os módulos do pipeline que consomem essas tabelas utilizam **exclusivamente** os arquivos *_agr_<cnpj>.parquet, garantindo que apenas linhas válidas (com `id_agrupado`) sejam processadas.
- Esse padrão é obrigatório para manter a rastreabilidade e a qualidade dos dados fiscais.

> **Importante:** Caso haja linhas sem `id_agrupado`, o pipeline não falha, mas gera o arquivo de auditoria e exclui essas linhas da saída principal, permitindo análise posterior.
