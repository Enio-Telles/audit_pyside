# Checklist Executavel de Otimizacao

Este checklist transforma o plano de desempenho em uma sequencia de execucao pratica.

## Estado atual

- [x] Plano macro consolidado em `docs/plano_otimizacao_desempenho.md`
- [x] Fase 0 iniciada com instrumentacao basica no codigo
- [x] Fase 1 iniciada com debounce em filtros principais da interface
- [x] Fase 2 iniciada com cache curto de datasets no `ParquetService`
- [x] Diagnostico inicial de performance consolidado em `docs/diagnostico_performance_atual.md`
- [ ] Linha de base registrada com medicoes reais
- [ ] Priorizacao final dos gargalos a partir dos logs

## Fase 0 - Observabilidade e Linha de Base

### 0.1. Instrumentacao

- [x] Criar infraestrutura simples de log de performance
- [x] Instrumentar `src/interface_grafica/services/parquet_service.py`
- [x] Instrumentar `src/interface_grafica/services/aggregation_service.py`
- [x] Instrumentar `src/transformacao/movimentacao_estoque.py`
- [x] Instrumentar `src/transformacao/calculos_mensais.py`
- [x] Instrumentar `src/transformacao/calculos_anuais.py`
- [x] Instrumentar `src/interface_grafica/services/query_worker.py`

Observacao:

- Os eventos da Fase 0 agora sao gravados em `logs/performance/perf_events.jsonl`.
- O resumo agregado pode ser gerado com `python scripts/resumir_performance.py`.

### 0.2. Coleta de linha de base

- [ ] Abrir `mov_estoque` de um CNPJ grande e registrar o tempo total
- [ ] Abrir `aba_mensal` do mesmo CNPJ e registrar o tempo total
- [ ] Abrir `aba_anual` do mesmo CNPJ e registrar o tempo total
- [ ] Aplicar filtros de texto em tabela grande e observar a quantidade de recargas
- [ ] Editar fatores na aba Conversao e disparar o recalc
- [ ] Executar uma consulta Oracle representativa e medir conexao, execute, fetch e montagem do DataFrame
- [ ] Repetir cada medicao pelo menos 3 vezes
- [ ] Preencher `docs/baseline_performance.md` com os resultados consolidados

### 0.3. Consolidacao da linha de base

- [ ] Separar os eventos por fluxo
- [ ] Calcular media, minimo e maximo
- [ ] Identificar os 3 gargalos mais lentos por fluxo
- [ ] Classificar cada gargalo em:
- [ ] UI
- [ ] I/O
- [ ] join/agregacao
- [ ] ordenacao
- [ ] recalc encadeado
- [ ] memoria

### 0.4. Criterio de saida da Fase 0

- [ ] Conseguimos responder com dados reais onde o sistema perde mais tempo
- [ ] Conseguimos dizer se o maior problema esta na UI, no parquet ou no pipeline derivado
- [ ] Existe uma lista ordenada de 5 otimizações candidatas com impacto estimado

## Fase 1 - UX e Responsividade

- [x] Aplicar debounce nos filtros de texto das telas principais
- [ ] Remover recargas redundantes disparadas a cada tecla
- [ ] Reduzir `resizeColumnsToContents()` automatico
- [ ] Garantir worker para recargas pesadas
- [ ] Medir antes/depois da UX apos cada ajuste

## Fase 2 - Reducao de I/O

- [x] Cache de schema por arquivo
- [x] Cache de contagem por arquivo + filtro
- [x] Cache curto de pagina por arquivo + filtro + pagina
- [x] Cache curto de `load_dataset` para releituras iguais
- [x] Reuso do schema cacheado ao aplicar filtros no `LazyFrame`
- [ ] Reuso de `LazyFrame` no mesmo ciclo
- [x] Invalidacao por `mtime` via assinatura do arquivo
- [ ] Selecionar apenas colunas necessarias em leituras grandes

## Fase 3 - Pipeline Derivado

- [ ] Mapear dependencias reais entre artefatos
- [ ] Separar full recalculation de recalc parcial
- [x] Reduzir materializacoes repetidas em `_process_source` de `movimentacao_estoque.py`
- [x] Simplificar aplicacao do saldo anual com `group_by(...).map_groups(...)`
- [x] Reduzir custo de montagem de referencias NSU e lookup de descricoes em `movimentacao_estoque.py`
- [x] Precalcular flags usadas em `calcular_saldo_anual` antes do agrupamento
- [ ] Revisar custo de `movimentacao_estoque.py`
- [x] Medir melhor etapas de normalizacao, eventos, enriquecimento e ordenacao em `movimentacao_estoque.py`
- [ ] Medir impacto detalhado de `sort` e `join` com base nos novos logs
- [ ] Estudar recalc parcial em mensal/anual por periodo afetado

## Fase 4 - Refatoracao da Interface

- [ ] Separar controladores por aba
- [ ] Extrair rotina comum de filtro/recarga/status
- [ ] Reduzir acoplamento da `MainWindow`
- [ ] Padronizar fluxo de tabela destacada, filtros e presets

## Fase 5 - Confiabilidade

- [ ] Padronizar erros de arquivo e schema
- [ ] Criar verificacoes de sanidade apos gerar parquet derivado
- [ ] Validar colunas minimas antes de recalcular
- [ ] Melhorar mensagens tecnicas e mensagens de usuario

## Fase 6 - Testes e Regressao

- [ ] Cobrir mais regras em `movimentacao_estoque`
- [ ] Cobrir pipeline completo de Conversao -> Estoque
- [ ] Criar testes de regressao para bugs fiscais recentes
- [ ] Criar benchmark basico repetivel

## Fase 7 - Limpeza Tecnica

- [ ] Remover codigo morto
- [ ] Consolidar helpers repetidos
- [ ] Revisar nomes e responsabilidades
- [ ] Atualizar documentacao tecnica apos cada refatoracao relevante

## Proxima acao recomendada

- [ ] Rodar a aplicacao com um CNPJ grande, executar os fluxos principais e analisar `logs/performance/perf_events.jsonl`
