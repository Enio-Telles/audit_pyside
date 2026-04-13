# Arquitetura do Sistema

O **Fiscal Parquet Analyzer** adota uma arquitetura modular, orientada a domínios e desacoplada da interface gráfica.

## 1. Estrutura de Diretórios

O código-fonte reside em `src/`, organizado da seguinte forma:

- **`src/extracao/`**: Módulos para leitura de fontes externas (Oracle, XMLs brutos) e conversão inicial para Parquet.
- **`src/transformacao/`**: O coração do sistema, subdividido por domínios fiscais:
    - `tabelas_base/`: Processamento inicial de itens e documentos.
    - `rastreabilidade_produtos/`: Agrupamento (MDM) e fatores de conversão.
    - `movimentacao_estoque_pkg/`: Geração do fluxo cronológico de estoque.
    - `calculos_mensais_pkg/` e `calculos_anuais_pkg/`: Resumos e auditorias tributárias.
- **`src/utilitarios/`**: Funções transversais (I/O Parquet, texto, performance, validações).
- **`src/interface_grafica/`**: Camada PySide6, isolada da lógica de negócio.

## 2. Orquestrador de Pipeline (Registry)

O processamento é gerenciado de forma declarativa em `src/orquestrador_pipeline.py`.

### O Padrão Registry
Cada tabela do sistema é registrada com um ID, um caminho de importação e suas dependências:

```python
_registar("movimentacao_estoque", 
          "transformacao.movimentacao_estoque:gerar_movimentacao_estoque", 
          deps=["c170_xml", "c176_xml"])
```

### Grafo de Dependências
O orquestrador resolve a ordem de execução automaticamente através de um algoritmo de **ordenação topológica**, garantindo que as tabelas base sejam geradas antes das tabelas de auditoria.

**Fluxo Principal:**
`tb_documentos` → `item_unidades` → `itens` → `descricao_produtos` → `produtos_final` → `fatores_conversao` → `movimentacao_estoque` → `calculos_mensais` → `calculos_anuais`.

## 3. Contrato de Funções

Para garantir a interoperabilidade, todas as funções principais de geração de tabela seguem o mesmo contrato:

```python
def gerar_<tabela>(cnpj: str, pasta_cnpj: Path | None = None) -> bool:
```
- **Retorno:** Booleano indicando o sucesso da operação.
- **Isolamento:** Nenhuma função de transformação pode importar nada de `interface_grafica`.

## 4. Proxy Modules (Compatibilidade)

Para manter a compatibilidade com scripts legados e importações dinâmicas, a raiz de `src/transformacao/` contém "Proxy Modules" que apenas redirecionam as chamadas para os novos subpacotes.

Exemplo em `src/transformacao/movimentacao_estoque.py`:
```python
from transformacao.movimentacao_estoque_pkg.movimentacao_estoque import *
```
