# fontes_produtos

## Visão Geral

Tabela ponte que relaciona cada `codigo_fonte` ao respectivo `id_agrupado`, preservando a capacidade de voltar da análise ao item bruto e permitindo agregação/desagregação manual.

## Função de Geração

```python
def gerar_fontes_produtos(cnpj: str, pasta_cnpj: Path | None = None) -> bool
```

Módulo: `src/transformacao/fontes_produtos.py`

## Dependências

- **Depende de**: `produtos_final`
- **É dependência de**: `fatores_conversao`

## Fontes de Entrada

- `produtos_final_<cnpj>.parquet`

## Objetivo

Criar uma tabela de mapeamento entre as fontes originais dos produtos (`codigo_fonte`) e os produtos agrupados (`id_agrupado`). Esta tabela é essencial para:

1. **Agregação**: permitir que múltiplas fontes apontem para o mesmo produto mestre
2. **Desagregação**: permitir voltar do produto mestre às fontes originais
3. **Rastreabilidade**: manter o vínculo entre análise e dados brutos

## Principais Colunas

| Coluna | Tipo | Descrição |
|--------|------|-----------|
| `codigo_fonte` | str | Identificador da fonte: `CNPJ_Emitente + "|" + codigo_produto_original` |
| `id_agrupado` | str | Chave do produto agrupado associado |
| `descr_padrao` | str | Descrição padrão do grupo |
| `ncm_padrao` | str | NCM padrão do grupo |
| `cest_padrao` | str | CEST padrão do grupo |
| `unid_ref_sugerida` | str | Unidade de referência sugerida |

## Regras de Processamento

### Agregação Manual

Quando a heurística automática não é suficiente:

- Vários grupos mestre podem ser fundidos em um novo `id_agrupado`
- Os vínculos da tabela ponte passam a apontar para o novo grupo

### Desagregação Manual

- O grupo consolidado é particionado
- A tabela ponte restaura a associação autônoma dos itens de origem
- A rastreabilidade é preservada, nunca substituída

## Uso Posterior

Esta tabela é consumida por:

- **fatores_conversao**: para vincular unidades ao produto agrupado
- **c170_xml**, **c176_xml**: para enriquecer fontes com `id_agrupado`
- **movimentacao_estoque**: para JOINs entre fontes e tabelas de/para

## Saída Gerada

```
dados/CNPJ/<cnpj>/analises/produtos/fontes_produtos_<cnpj>.parquet
```

## Notas

- Esta tabela é a peça central da agregação e desagregação
- Permite intervenção manual quando o agrupamento automático falha
- Essencial para auditoria: permite navegar do analítico ao bruto
