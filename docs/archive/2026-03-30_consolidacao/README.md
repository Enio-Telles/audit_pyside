# Documentação do Projeto: Fiscal Parquet Analyzer

Bem-vindo à documentação consolidada do **Fiscal Parquet Analyzer**. Este conjunto de documentos descreve a arquitetura, as regras de negócio fiscais, as estratégias de performance e o método de rastreabilidade de dados.

## 📚 Sumário

1.  **[Arquitetura do Sistema](arquitetura.md)**  
    Estrutura modular, padrão Registry, grafo de dependências e organização de pacotes.
2.  **[Processamento e Cálculos Fiscais](processamento_fiscal.md)**  
    Detalhamento dos algoritmos de Movimentação de Estoque, Fatores de Conversão, Cálculos Mensais e Anuais.
3.  **[Rastreabilidade e Agregação (Fio de Ouro)](rastreabilidade.md)**  
    Metodologia de Master Data Management (MDM) e como garantimos a auditoria até o registro original.
4.  **[Performance e Otimização](performance.md)**  
    Diagnósticos de desempenho, uso de Polars e otimizações com NumPy.

---

## 🚀 Visão Geral

O **Fiscal Parquet Analyzer** é uma ferramenta de auditoria e análise de dados fiscais (SPED, XMLs) construída sobre o motor **Polars** para processamento massivo de dados em memória, com interface em **PySide6**.

O projeto utiliza uma arquitetura modular "1 Tabela = 1 Função", onde cada etapa do processamento é isolada em subpacotes dentro de `src/transformacao/`.

## 🛠️ Tecnologias Principais

- **Linguagem:** Python 3.10+
- **Processamento:** Polars (DataFrame library de alta performance)
- **Cálculos Intensivos:** NumPy (vetorização de saldos sequenciais)
- **Interface:** PySide6 (Qt para Python)
- **Armazenamento:** Apache Parquet (colunar e comprimido)
