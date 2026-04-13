# Agregacao

## Colunas de descricao

Na camada de agregacao, as descricoes principais e os complementos devem ficar separados:

- `lista_descricoes`: contem apenas descricoes principais do produto/grupo.
- `lista_desc_compl`: contem apenas descricoes complementares vindas de `descr_compl`/`lista_desc_compl`.
- `lista_itens_agrupados`: mostra as descricoes-base dos itens hoje vinculados ao grupo.
- `ids_origem_agrupamento`: registra quais `id_agrupado` deram origem ao grupo atual.

Essa separacao vale para:

- `produtos_agrupados_<cnpj>.parquet`
- `produtos_final_<cnpj>.parquet`
- `id_agrupados_<cnpj>.parquet`
- exibicao da aba `Agregacao`

## Regra funcional

- Complementos nao devem ser incorporados em `lista_descricoes`.
- Filtros textuais da aba `Agregacao` continuam considerando as duas colunas para nao perder rastreabilidade.
- Rotinas que reconciliam grupos por descricao podem usar `lista_descricoes` e `lista_desc_compl`, mas cada campo permanece semanticamente separado.

## Reversao de agrupamentos

- Um agrupamento manual passa a registrar no log os grupos de origem e os itens envolvidos.
- A reversao restaura os grupos de origem a partir desse snapshot.
- A reversao usa o `id_agrupado` de destino preservado no merge manual; por isso o merge nao renumera mais todos os grupos.
