# Plano de melhorias do servico de similaridade

Status: implementado em feat/similaridade-particionamento.

## Sprint 1 - melhorias incrementais ao metodo composto

- NCM hierarquico de 5 niveis (item/subposicao/posicao/capitulo)
- Canonizacao de unidades antes de extrair numeros (ML/L, G/KG)
- Caps de bucket e top-k por linha
- Cap de tamanho de bloco e coesao minima

## Sprint 2 - metodologia de particionamento por chaves fiscais

- Modulo particionamento_fiscal.py com 4 camadas obrigatorias
  (GTIN, NCM+CEST+UNID, NCM+UNID, NCM4+UNID) + 1 opcional
  (descricao via inverted index).

## Sprint 3 - modo apenas descricao via inverted index

- Modulo inverted_index_descricao.py com poda por document
  frequency, comparacao apenas dentro de buckets de tokens
  compartilhados.

## Sprint 4 - integracao na UI

- Seletor de metodo na aba Agregacao.
- Checkbox para camada 5 visivel apenas no metodo de
  particionamento.
- Aviso visual quando metodo "apenas descricao" e selecionado.

## Possiveis melhorias futuras (nao implementadas)

- MinHash + LSH para corpus muito ruidoso (datasketch opcional).
- Cache parquet sidecar para vocabulario do inverted index.
- Embeddings semanticos (sentence-transformers) com cache de
  vetores - alto custo de pre-computacao, melhor recall.
- Dicionario de marcas alimentavel para sinal explicito de marca.
