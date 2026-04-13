# Tabelas do Fiscal Parquet Analyzer

Este diretório contém a documentação de todas as tabelas geradas pelo pipeline de processamento fiscal.

## Visão Geral do Pipeline

O pipeline segue uma ordem topológica definida em `src/orquestrador_pipeline.py`:

```
tb_documentos
    ↓
item_unidades
    ↓
itens
    ↓
descricao_produtos
    ↓
produtos_final
    ↓
fontes_produtos
    ↓
fatores_conversao
    ↓
    ├─→ c170_xml ──┐
    └─→ c176_xml ──┤
                   ↓
        movimentacao_estoque
              ↓
        ┌─────┴─────┐
        ↓           ↓
calculos_mensais  calculos_anuais
```

## Tabelas de Base

| Tabela | Descrição | Dependências |
|--------|-----------|--------------|
| [tb_documentos](tb_documentos.md) | Consolida documentos fiscais extraídos | Nenhuma |
| [item_unidades](item_unidades.md) | Detalha itens por unidade de medida | `tb_documentos` |
| [itens](itens.md) | Detalha itens com informações enriquecidas | `item_unidades` |
| [descricao_produtos](descricao_produtos.md) | Padroniza descrições de produtos | `itens` |

## Tabelas de Agrupamento (MDM)

| Tabela | Descrição | Dependências |
|--------|-----------|--------------|
| [produtos_final](produtos_final.md) | Agrupamento mestre de produtos (MDM) | `descricao_produtos` |
| [fontes_produtos](fontes_produtos.md) | Tabela ponte: fonte → produto agrupado | `produtos_final` |
| [fatores_conversao](fatores_conversao.md) | Fatores de conversão entre unidades | `fontes_produtos`, `item_unidades` |

## Tabelas de Enriquecimento

| Tabela | Descrição | Dependências |
|--------|-----------|--------------|
| [c170_xml](c170_xml.md) | C170 enriquecido com XMLs e fatores | `fatores_conversao` |
| [c176_xml](c176_xml.md) | C176 enriquecido com XMLs e fatores | `fatores_conversao` |
| [movimentacao_estoque](movimentacao_estoque.md) | Fluxo cronológico de estoque | `c170_xml`, `c176_xml` |

## Tabelas Analíticas

| Tabela | Descrição | Dependências |
|--------|-----------|--------------|
| [calculos_mensais](calculos_mensais.md) | Resumo mensal da movimentação | `movimentacao_estoque` |
| [calculos_anuais](calculos_anuais.md) | Auditoria anual com ICMS | `movimentacao_estoque` |

## Conceitos Fundamentais

### Golden Thread (Fio de Ouro)

O sistema preserva rastreabilidade completa através do "fio de ouro":

```
linha original → id_linha_origem → codigo_fonte → id_agrupado → tabelas analíticas
```

### Chaves Centrais

| Chave | Descrição |
|-------|-----------|
| `id_linha_origem` | Chave física da linha original (ex: `chave_acesso + prod_nitem`) |
| `codigo_fonte` | `CNPJ_Emitente + "|" + codigo_produto_original` |
| `id_agrupado` | Chave mestra do produto consolidado |

### Contrato de Funções

Todas as funções de geração seguem o contrato:

```python
def gerar_<etapa>(cnpj: str, pasta_cnpj: Path | None = None) -> bool
```

- Retorna `True` em sucesso, `False` em falha
- Persiste saída em Parquet
- Não depende da camada de UI

## Localização dos Arquivos

As tabelas são geradas em:

```
dados/CNPJ/<cnpj>/analises/produtos/
```

## Notas Importantes

### Preservação de Ajustes Manuais

Ajustes manuais feitos pelo usuário (ex: `unid_ref`, `fator`) devem ser preservados em reprocessamentos.

### Separação UI vs ETL

- ETL (`extracao/`, `transformacao/`, `utilitarios/`): não manipula widgets
- Interface: usa `QThread` para trabalho pesado, comunica por sinais

### Invariantes de Negócio

- `cest` e `gtin` não são equivalentes
- Estoque final não altera saldo físico, apenas audita
- Custo médio é calculado cronologicamente, linha a linha
