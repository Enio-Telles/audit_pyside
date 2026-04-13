# tb_documentos

## Visão Geral

Tabela base do pipeline que consolida os documentos fiscais extraídos do Oracle (NFe, NFCe, SPED C170, Bloco H) em formato Parquet estruturado.

## Função de Geração

```python
def gerar_tabela_documentos(cnpj: str, pasta_cnpj: Path | None = None) -> bool
```

Módulo: `src/transformacao/tabela_documentos.py`

## Dependências

- **Depende de**: nenhuma (tabela de entrada)
- **É dependência de**: `item_unidades`

## Fontes de Entrada

- Dados brutos extraídos do Oracle (SPED, XMLs de NFe/NFCe, Bloco H)
- Arquivos Parquet gerados na fase de extração (`src/extracao/extrair_dados_cnpj.py`)

## Objetivo

Normalizar e unificar os documentos fiscais de diferentes fontes em uma estrutura comum, servindo como base para todas as etapas subsequentes do pipeline. Esta tabela é o ponto de partida para a construção da rastreabilidade fiscal.

## Principais Colunas

| Coluna | Tipo | Descrição |
|--------|------|-----------|
| `chave_acesso` | str | Chave de acesso da NFe/NFCe (44 dígitos) |
| `num_doc` | str | Número do documento fiscal |
| `cnpj_emitente` | str | CNPJ do emitente do documento |
| `cnpj_destinatario` | str | CNPJ do destinatário |
| `dt_doc` | date | Data de emissão do documento |
| `cfop` | str | Código Fiscal de Operações e Prestações |
| `fonte` | str | Origem do registro (nfe, nfce, c170, bloco_h) |

## Regras de Processamento

- Unifica documentos de diferentes fontes (Oracle, XML) em um schema comum
- Normaliza CNPJs removendo caracteres não numéricos
- Preserva a origem do documento na coluna `fonte` para rastreabilidade
- Filtra documentos relevantes para o CNPJ analisado

## Saída Gerada

```
dados/CNPJ/<cnpj>/analises/produtos/tb_documentos_<cnpj>.parquet
```

## Notas

- Esta é a primeira tabela executada no pipeline e não possui dependências upstream
- Todos os documentos fiscais passam por esta etapa antes de serem detalhados em itens individuais
