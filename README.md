# Fiscal Parquet Analyzer (Refatorado)

Solução de alta performance para análise, extração e transformação de dados fiscais (Oracle -> Parquet) com interface gráfica moderna em **PySide6**.

## 🛠️ Requisitos do Sistema

- **Python 3.9+**
- **Oracle Instant Client** (necessário para extração via `oracledb`)
- Espaço em disco para armazenamento de arquivos Parquet (otimizados com compressão Snappy)

## 📦 Instalação

Recomenda-se o uso de um ambiente virtual (venv). Instale as dependências básicas:

```bash
pip install polars PySide6 openpyxl python-docx python-dotenv rich oracledb
```

## 🚀 Como Inicializar

Para abrir a interface gráfica, execute o lançador na raiz do projeto:

```bash
python app.py
```

O `app.py` configura automaticamente o ambiente as pastas necessárias (`workspace/`, `CNPJ/`, etc.) e garante que os módulos internos sejam carregados corretamente.

## 🏗️ Nova Arquitetura de Transformação (5 Módulos)

O projeto foi refatorado para seguir um fluxo lógico de "Plano Físico", garantindo total rastreabilidade dos dados:

1.  **Movimentações por Unidade (`produtos_unidades`)**: Consolida C170, NFe e NFCe com granularidade de unidade de medida.
2.  **Registro de Produtos (`produtos`)**: Deduplicação por descrição normalizada e atribuição de IDs únicos.
3.  **Agrupamento e Padrões (`produtos_agrupados`)**: Interface para agregação manual e definição automática de NCM/CEST padrão (via moda estatística).
4.  **Produtos Final (`produtos_final`)**: Integra `produtos` + `produtos_agrupados` em uma visão final recalculável.
5.  **Fatores de Conversão (`fatores_conversao`)**: Cálculo de multiplicadores entre unidades baseado em preço médio consolidado.

### Regras de Mapeamento (Produtos)

- `cest` é um campo próprio (`cest` / `prod_cest`) e não deve ser misturado com código de barras.
- `gtin` (código de barras) vem de `cod_barra` (SPED) ou `prod_ceantrib` / `prod_cean` (NFe/NFCe, com fallback).
- Quando não há preço médio de compra por unidade, o cálculo usa fallback de venda e registra logs em:
  `log_sem_preco_medio_compra_<cnpj>.parquet` e `log_sem_preco_medio_compra_<cnpj>.json`.

## 📁 Estrutura de Pastas

- `src/interface_grafica`: Código da UI (PySide6), modelos e serviços.
- `src/transformacao`: Os 4 módulos do "Plano Físico".
- `src/extracao`: Lógica de interface com Oracle e geração de tabelas brutas.
- `src/utilitarios`: Funções compartilhadas de texto, data e exportação.
- `CNPJ/`: Onde residem os dados processados organizados por contribuinte.

## 🧪 Testes

Execute os testes unitários via `pytest`:

```bash
python -m pytest
```

## Funcionalidades Principais

- **Visualização de Parquet e Integração Banco de Dados**: Consulta arquivos Parquet localizados ou no banco de dados, lidando com NFe, NFCe, C170, e outros dados fiscais.
- **Parametrização Dinâmica de SQL**: Extração e identificação automática de parâmetros (`:parametro`) a partir de arquivos e textos de consultas SQL, permitindo preenchimento através da interface gráfica com validação.
- **Exportação para Excel**: Geração de relatórios através de Pandas e Polars para arquivos Excel formatados.
- **Análise de Fator de Conversão e Códigos Mercadoria**: Mecanismos customizados de hashing (`MD5`) e tratativas de deduplicação usando as funções otimizadas do pacote `polars`.

## Instalação e Execução

O ambiente e as dependências estão gerenciados e documentados (via `.env.example`).
Execute a aplicação via terminal usando:

```bash
python3 app.py
```

## Execução dos Testes

O projeto utiliza o `pytest` para rodar os testes unitários.
Certifique-se de executar no diretório raiz do projeto definindo a variável `PYTHONPATH`:

```bash
PYTHONPATH=./funcoes_auxiliares:. python3 -m pytest
```

Isso garante que todos os módulos (incluindo testes que fazem mock e testam funções auxiliares de extração e tabelas) sejam corretamente resolvidos.
