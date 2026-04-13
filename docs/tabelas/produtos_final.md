# produtos_final

## Visão Geral

Tabela mestre que consolida o agrupamento de produtos (MDM), elegendo descrições padrão, unidades de referência sugeridas e classificações fiscais consolidadas para cada produto agrupado.

## Função de Geração

```python
def gerar_produtos_final(cnpj: str, pasta_cnpj: Path | None = None) -> bool
```

Módulo: `src/transformacao/produtos_final_v2.py`

## Dependências

- **Depende de**: `descricao_produtos`
- **É dependência de**: `fontes_produtos`, `fatores_conversao`

## Fontes de Entrada

- `descricao_produtos_<cnpj>.parquet`
- `itens_<cnpj>.parquet`

## Objetivo

Realizar o Master Data Management (MDM) de produtos, agrupando descrições equivalentes em um produto mestre com atributos consolidados. Esta tabela é o coração da identificação única de produtos no sistema.

## Principais Colunas

| Coluna | Tipo | Descrição |
|--------|------|-----------|
| `id_agrupado` | str | Chave mestra do produto agrupado |
| `descr_padrao` | str | Descrição padrão eleita para o grupo |
| `ncm_padrao` | str | NCM padrão do grupo |
| `cest_padrao` | str | CEST padrão do grupo (se aplicável) |
| `unid_ref_sugerida` | str | Unidade de referência sugerida |
| `co_sefin_padrao` | str | Código SEFIN padrão |
| `codigo_fonte` | str | Identificador da fonte antes do agrupamento |
| `descricao_normalizada` | str | Descrição normalizada do grupo |

## Regras de Processamento

### Agrupamento Automático

O agrupamento considera duas trilhas principais:

1. **GTIN comum**: produtos com mesmo código de barras são agrupados
2. **Descrição normalizada + NCM**: descrições idênticas com interseção de NCM formam grupo

### Fallback Tolerado

- Descrições idênticas podem formar grupo mesmo sem NCM comum
- Produtos sem GTIN são agrupados por similaridade textual

### Eleição de Atributos

- `descr_padrao`: descrição mais frequente ou mais completa do grupo
- `ncm_padrao`: NCM mais comum entre os itens do grupo
- `cest_padrao`: CEST mais comum (quando aplicável)
- `unid_ref_sugerida`: unidade mais movimentada ou eleita manualmente

## Golden Thread

Esta tabela materializa o conceito de "fio de ouro":

```
linha original → id_linha_origem → codigo_fonte → id_agrupado → tabelas analíticas
```

## Saída Gerada

```
dados/CNPJ/<cnpj>/analises/produtos/produtos_final_<cnpj>.parquet
```

## Notas

- O `id_agrupado` é a chave primária usada em todas as tabelas analíticas subsequentes
- Esta tabela permite auditoria reversa até a linha original via `codigo_fonte`
- Ajustes manuais de agrupamento podem ser feitos via interface gráfica
- Preserva a rastreabilidade completa sem substituir linhas originais
