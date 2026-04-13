# Performance e Otimização

O **Fiscal Parquet Analyzer** utiliza tecnologias modernas para garantir processamento veloz de milhões de linhas.

## 1. Motor de Dados: Polars

O sistema utiliza o **Polars** em substituição ao Pandas tradicional.
- **Lazy Evaluation:** O pipeline de dados é montado como um plano de execução (`LazyFrame`) e otimizado pelo Polars antes do processamento real.
- **Escrita em Parquet:** O formato colunar Parquet permite leitura seletiva de colunas, economizando I/O.

## 2. Otimização NumPy (Saldos Sequenciais)

Cálculos de saldo cronológico (onde a linha N depende do resultado da linha N-1) são gargalos naturais para vetores puros.
O sistema resolve isso extraindo as colunas críticas para **arrays NumPy** e executando a iteração em arrays de baixo nível.
- **Resultado:** Ganho de performance de **3x a 5x** em comparação com o uso de dicionários Python (`to_dicts()`).

## 3. Estratégia de Cache e I/O

- **Cache de Schema e Metadados:** `ParquetService` mantém em memória o schema e a contagem de linhas dos arquivos mais acessados.
- **Filtragem Cedo:** Filtros são aplicados o mais cedo possível no `LazyFrame` para reduzir o volume de dados processados em etapas pesadas (joins/sorts).

## 4. Assincronismo na UI

Nenhum processamento pesado (ETL ou Consulta SQL) ocorre na Thread Principal (Main Thread) da aplicação.
- O sistema utiliza `QThread` (através de Workers como `PipelineWorker` e `QueryWorker`) para manter a interface responsiva durante o processamento.
- A comunicação entre a UI e o motor de dados é feita via Sinais e Objetos de Resultado.

## 5. Logs de Performance e Instrumentação

O sistema gera logs automáticos de execução em `logs/performance/perf_events.jsonl`, permitindo diagnosticar gargalos em tempo real. Cada etapa do orquestrador registra seu tempo de início, fim e duração.
