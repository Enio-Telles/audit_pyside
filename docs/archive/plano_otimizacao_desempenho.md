# Plano de Otimizacao de Desempenho e Confiabilidade

## Objetivo

Tornar o sistema:

- mais rapido na abertura, filtragem e recalculo das tabelas
- mais estavel durante operacoes pesadas
- mais simples de manter e evoluir
- menos propenso a regressao e erros silenciosos

Este plano foi montado com base na estrutura atual do projeto, especialmente nas areas:

- `src/interface_grafica/ui/main_window.py`
- `src/interface_grafica/services/parquet_service.py`
- `src/interface_grafica/services/aggregation_service.py`
- `src/interface_grafica/services/query_worker.py`
- `src/transformacao/movimentacao_estoque.py`
- `src/transformacao/calculos_mensais.py`
- `src/transformacao/calculos_anuais.py`
- `src/transformacao/fatores_conversao.py`

## Resultado esperado

Ao final do plano, o sistema deve apresentar:

- menor latencia para abrir e filtrar tabelas grandes
- menor tempo para recalculo de `mov_estoque`, `aba_mensal` e `aba_anual`
- menos leituras repetidas de parquet
- menor uso de memoria em consultas e cargas grandes
- menor acoplamento da interface com a regra de negocio
- testes cobrindo os calculos fiscais mais sensiveis
- logs e metricas suficientes para localizar gargalos e erros com rapidez

## Diagnostico atual

### 1. Interface com responsabilidades demais

Arquivo principal:

- `src/interface_grafica/ui/main_window.py`

Problemas:

- concentra UI, filtros, persistencia, recarga de tabelas, operacoes de arquivo e parte da coordenacao de negocio
- alto acoplamento entre abas
- maior risco de regressao ao alterar uma funcionalidade localizada
- dificil identificar o que roda na thread principal e o que deveria ir para worker

Impacto:

- manutencao lenta
- maior chance de travamento ou comportamento inconsistente
- dificuldade para evoluir sem quebrar fluxos antigos

### 2. Leitura repetitiva e custosa de parquet

Arquivo principal:

- `src/interface_grafica/services/parquet_service.py`

Problemas:

- repeticao de scans para schema, total de linhas e pagina atual
- baixa reutilizacao de resultados ja conhecidos
- pouca diferenciacao entre cenarios de preview, filtro local e carga completa

Impacto:

- tempo de resposta alto em tabelas grandes
- custo desnecessario de CPU e I/O

### 3. Pipeline derivado ainda muito "full recalculation"

Arquivos principais:

- `src/interface_grafica/services/aggregation_service.py`
- `src/transformacao/movimentacao_estoque.py`
- `src/transformacao/calculos_mensais.py`
- `src/transformacao/calculos_anuais.py`

Problemas:

- mudancas pequenas ainda escalam para recalculos amplos
- ausencia de cache intermediario claro
- dependencia forte de leitura integral e agregacao full
- pontos de custo elevado em `join`, `sort`, `group_by(...).map_groups(...)`

Impacto:

- lentidao perceptivel apos alteracoes de agregacao ou fatores
- maior risco de inconsistencias em fluxos longos

### 4. Uso de memoria acima do ideal

Arquivo principal:

- `src/interface_grafica/services/query_worker.py`

Problemas:

- consultas grandes tendem a acumular tudo antes da montagem final
- falta estrategia de preview limitado e exportacao em partes

Impacto:

- risco de degradacao geral da aplicacao
- consumo de memoria desnecessario

### 5. Cobertura de testes ainda desigual

Estado observado:

- ha testes bons nas regras mais recentes de mensal, anual e `mov_estoque`
- ainda faltam testes de regressao para UI, integracao do pipeline e cenarios de carga

Impacto:

- mudancas de performance podem introduzir bugs sem sinalizacao imediata
- dificuldade para refatorar trechos centrais com seguranca

## Principios de execucao

Antes de otimizar, o time deve seguir estes principios:

1. Medir antes e depois.
2. Corrigir gargalos reais, nao apenas suspeitas.
3. Reduzir trabalho repetido antes de trocar algoritmo.
4. Tirar processamento pesado da UI antes de refatorar arquitetura ampla.
5. Garantir testes dos calculos fiscais antes de mexer em pontos criticos.
6. Dividir o trabalho em fases curtas e verificaveis.

## Metricas-base obrigatorias

Antes de iniciar a execucao, registrar uma linha de base com os tempos reais:

- tempo para abrir `mov_estoque`
- tempo para abrir `aba_mensal`
- tempo para abrir `aba_anual`
- tempo para filtrar tabela grande digitando texto
- tempo para recalcular apos editar fatores na aba Conversao
- tempo para recalcular apos agregacao manual
- uso de memoria ao abrir tabela grande
- uso de memoria em consulta Oracle grande

Sugestao de padrao:

- 3 medicoes por fluxo
- anotar media, minimo e maximo
- registrar tamanho do parquet e quantidade de linhas
- guardar os resultados em um arquivo de benchmark simples em `docs/` ou `tests/perf/`

## Fase 0 - Observabilidade e linha de base

Objetivo:

- descobrir com precisao onde o sistema gasta tempo e memoria

Etapas:

1. Instrumentar tempos em `parquet_service.py`.
   Medir separadamente:
   - leitura de schema
   - contagem total
   - construcao da consulta lazy
   - coleta da pagina

2. Instrumentar tempos em `aggregation_service.py`.
   Medir:
   - leitura das bases
   - recalc de referencias
   - recalc de `mov_estoque`
   - recalc de `aba_mensal`
   - recalc de `aba_anual`

3. Instrumentar `movimentacao_estoque.py`.
   Medir:
   - leituras
   - joins
   - ordenacoes
   - agrupamentos
   - `map_groups`

4. Registrar logs com contexto de CNPJ, arquivo e quantidade de linhas.

Entregaveis:

- logs legiveis de performance por etapa
- tabela baseline com tempos reais

Criterio de aceite:

- ser possivel identificar, em um fluxo real, as 3 etapas mais lentas sem usar debug manual

## Fase 1 - Ganhos rapidos de UX e responsividade

Objetivo:

- melhorar a percepcao de rapidez sem alterar profundamente a arquitetura

Etapas:

1. Aplicar debounce nos filtros de texto.
   Prioridade:
   - Consulta
   - Consulta SQL
   - Agregacao
   - Conversao
   - Estoque

2. Reduzir chamadas automaticas a `resizeColumnsToContents()`.
   Regra:
   - apenas na primeira carga
   - ou por acao explicita do usuario

3. Revisar sinais reativos que disparam recarga a cada evento.
   Exemplos:
   - `textChanged`
   - `currentTextChanged`
   - eventos em cascata ao limpar filtros

4. Garantir que recargas pesadas usem worker sempre que houver risco de travar a tela.

5. Revisar atualizacao de labels, status e titulos para nao recalcular dados desnecessariamente.

Entregaveis:

- filtros com atraso controlado
- menos recargas redundantes
- UI mais fluida em digitacao e troca de aba

Criterio de aceite:

- digitacao em filtros nao deve disparar recarga a cada tecla
- tabela grande deve continuar responsiva durante filtragem

## Fase 2 - Reducao de I/O e cache inteligente

Objetivo:

- evitar releitura do mesmo parquet e recalculo do mesmo resultado

Etapas:

1. Criar cache de schema por arquivo em `parquet_service.py`.

2. Criar cache de total de linhas por combinacao:
   - arquivo
   - filtros
   - colunas visiveis quando isso afetar a leitura

3. Estudar reuso de `LazyFrame` montado no mesmo ciclo de requisicao.

4. Adotar invalidacao de cache por:
   - `mtime` do arquivo
   - troca de CNPJ
   - regravacao de parquet derivado

5. Criar cache curto para datasets muito usados na interface:
   - `mov_estoque`
   - `aba_mensal`
   - `aba_anual`
   - `fatores_conversao`

6. Padronizar leitura com selecao minima de colunas.
   Regra:
   - nunca carregar todas as colunas se a tela usa apenas um subconjunto

Entregaveis:

- camada de cache simples e previsivel
- invalidacao segura
- reducao do numero de scans por interacao

Criterio de aceite:

- reabrir a mesma tabela sem alteracao de arquivo deve ser significativamente mais rapido que a primeira abertura

## Fase 3 - Otimizacao do pipeline derivado

Objetivo:

- reduzir o custo do caminho `agregacao -> fatores -> mov_estoque -> mensal -> anual`

Etapas:

1. Mapear dependencias reais entre artefatos.
   Exemplo de matriz:
   - alteracao visual: nao recalcula nada
   - alteracao de filtro: nao reler base inteira
   - alteracao de fator: recalcular `mov_estoque`, `mensal`, `anual`
   - alteracao estrutural de agregacao: recalculo completo

2. Transformar o pipeline em etapas explicitamente encadeadas e reutilizaveis.

3. Materializar artefatos intermediarios quando isso economizar tempo de CPU.
   Exemplos:
   - base padronizada para `mov_estoque`
   - base auxiliar mensal com colunas tributarias prontas

4. Reavaliar `movimentacao_estoque.py`.
   Prioridades:
   - reduzir `map_groups` quando houver alternativa em Polars puro
   - eliminar sorts desnecessarios
   - minimizar colunas carregadas antes de joins
   - evitar materializacao intermediaria quando a expressao lazy resolver

5. Estudar recalc parcial por ano afetado em `calculos_mensais.py` e `calculos_anuais.py`.

6. Garantir que cada etapa grave artefato consistente antes de disparar a seguinte.

Entregaveis:

- grafo de dependencias do pipeline
- menor custo medio de recalculo
- menos full scans em alteracoes pequenas

Criterio de aceite:

- editar fator na Conversao deve recalcular mais rapido e com escopo previsivel
- alterar agregacao deve produzir resultado consistente sem etapas redundantes

## Fase 4 - Refatoracao estrutural da interface

Objetivo:

- deixar a UI mais limpa, modular e menos sujeita a regressao

Etapas:

1. Quebrar `main_window.py` em controladores por dominio.
   Sugestao inicial:
   - `consulta_controller.py`
   - `consulta_sql_controller.py`
   - `agregacao_controller.py`
   - `conversao_controller.py`
   - `estoque_controller.py`

2. Extrair helpers compartilhados:
   - filtros
   - presets de colunas
   - persistencia de layout
   - destaque de janelas
   - exportacao de tabelas

3. Padronizar o fluxo de tabela:
   - carregar
   - filtrar
   - aplicar preferencia
   - atualizar status
   - destacar

4. Criar camada clara entre UI e servicos.
   Regra:
   - controller coordena
   - service calcula/carrega
   - model representa

5. Eliminar duplicacao de logica de filtros e recarga.

Entregaveis:

- `main_window.py` menor
- responsabilidades mais claras
- menor risco de efeito colateral entre abas

Criterio de aceite:

- ser possivel alterar uma aba com impacto minimo nas demais
- menor complexidade ciclomática nas funcoes centrais

## Fase 5 - Confiabilidade e prevencao de erros

Objetivo:

- diminuir falhas de regra, estado inconsistente e bugs silenciosos

Etapas:

1. Revisar tratamento de erro em operacoes de arquivo.
   Cobrir:
   - parquet ausente
   - schema inesperado
   - coluna faltante
   - arquivo corrompido

2. Padronizar mensagens de erro para o usuario e para log tecnico.

3. Garantir validacoes antes de recalculo.
   Exemplos:
   - CNPJ selecionado
   - arquivos de entrada existentes
   - colunas minimas disponiveis

4. Criar verificacoes de sanidade apos gerar:
   - `mov_estoque`
   - `aba_mensal`
   - `aba_anual`
   - `fatores_conversao`

5. Registrar contagens e estatisticas minimas apos cada etapa.
   Exemplos:
   - linhas geradas
   - anos encontrados
   - nulos em colunas criticas
   - ids sem chave fiscal esperada

6. Revisar pontos com maior chance de erro fiscal:
   - ST
   - MVA
   - PMS/PME
   - ultima movimentacao do periodo
   - fallback de aliquota

Entregaveis:

- fluxo mais resistente a dados imperfeitos
- logs mais uteis para suporte e depuracao

Criterio de aceite:

- falhas operacionais comuns devem produzir erro explicito e rastreavel

## Fase 6 - Testes e protecao contra regressao

Objetivo:

- permitir refatoracao e otimizacao com seguranca

Etapas:

1. Expandir testes unitarios dos calculos fiscais.
   Prioridades:
   - `movimentacao_estoque`
   - `calculos_mensais`
   - `calculos_anuais`
   - `fatores_conversao`

2. Criar testes de integracao para o pipeline principal.
   Cenarios:
   - agregacao manual
   - alteracao de `unid_ref`
   - alteracao de fator
   - recalculo derivado completo

3. Criar testes de regressao para bugs corrigidos recentemente.
   Exemplos:
   - bloqueio de ST apenas onde cabivel
   - formula de MVA ajustado
   - recalculo pendente na aba Conversao

4. Criar smoke tests da interface para fluxos de alto risco.
   Mesmo que sejam poucos, devem cobrir:
   - abertura das abas principais
   - aplicacao de filtros
   - destaque de tabela
   - recalc via botao

5. Criar benchmarks simples para cenarios criticos.

Entregaveis:

- suite de testes mais confiavel
- benchmark basico para comparar desempenho entre versoes

Criterio de aceite:

- nenhuma refatoracao relevante deve entrar sem testes de regra e sem comparacao minima de desempenho

## Fase 7 - Limpeza tecnica e padronizacao

Objetivo:

- reduzir complexidade acumulada e melhorar legibilidade

Etapas:

1. Remover codigo morto, helpers obsoletos e caminhos antigos nao usados.

2. Padronizar nomes de funcoes, metodos e estados internos.

3. Revisar comentarios e documentacao para manter aderencia ao codigo real.

4. Consolidar utilitarios repetidos em modulos especificos.

5. Revisar imports, dependencias circulares e pontos de acoplamento desnecessario.

Entregaveis:

- codigo mais legivel
- menor custo de manutencao

Criterio de aceite:

- menor duplicacao e maior previsibilidade estrutural

## Backlog tecnico por arquivo

### `src/interface_grafica/ui/main_window.py`

Backlog:

- aplicar debounce em todos os filtros relevantes
- mover logica de negocio e de recalc para servicos/controladores
- reduzir metodos longos e multi-responsabilidade
- unificar rotina de recarga de tabela
- unificar rotina de status, titulo e preferencias

### `src/interface_grafica/services/parquet_service.py`

Backlog:

- cache de schema
- cache de contagem
- reuso de consultas lazy
- leitura orientada a colunas
- invalidacao por alteracao de arquivo

### `src/interface_grafica/services/aggregation_service.py`

Backlog:

- cronometro por etapa
- grafo de dependencias
- recalc incremental quando possivel
- menos releitura de parquet no mesmo fluxo

### `src/interface_grafica/services/query_worker.py`

Backlog:

- leitura em partes
- preview com limite configuravel
- exportacao chunked
- feedback progressivo de progresso

### `src/transformacao/movimentacao_estoque.py`

Backlog:

- medir `map_groups`
- estudar substituicao por expressao vetorizada onde viavel
- reduzir joins e sorts
- selecionar apenas colunas necessarias
- revisar custo de materializacao intermediaria

### `src/transformacao/calculos_mensais.py`

Backlog:

- avaliar recalc por ano/mes afetado
- reaproveitar auxiliares tributarios
- evitar full scan quando so parte do periodo mudou

### `src/transformacao/calculos_anuais.py`

Backlog:

- avaliar recalc por ano afetado
- reaproveitar base consolidada da `mov_estoque`
- revisar agregacoes duplicadas

### `src/transformacao/fatores_conversao.py`

Backlog:

- medir custo de geracao completa
- evitar releituras repetidas das mesmas bases
- separar claramente calculo bruto, escolha de `unid_ref` e persistencia

## Ordem recomendada de execucao

### Sprint 1

- Fase 0 completa
- Fase 1 parcial
- cache de schema
- cronometros nas etapas principais

### Sprint 2

- Fase 1 completa
- Fase 2 principal
- primeiros caches de parquet
- reducao de scans redundantes

### Sprint 3

- Fase 3 principal
- otimizacao de `movimentacao_estoque`
- recalc mais granular

### Sprint 4

- Fase 4 principal
- decomposicao da `MainWindow`
- consolidacao de controladores

### Sprint 5

- Fase 5 e Fase 6
- reforco de testes
- validacoes e checks de sanidade

### Sprint 6

- Fase 7
- limpeza final
- revisao de documentacao

## Prioridade pratica imediata

Se for comecar agora, a ordem de maior retorno tende a ser:

1. Medir tempos reais e registrar baseline.
2. Colocar debounce e cortar recargas redundantes de tabela.
3. Criar cache de schema e contagem em `ParquetService`.
4. Instrumentar e otimizar o recalc de `mov_estoque`.
5. Reduzir full recalculation no fluxo da Conversao e Agregacao.
6. Refatorar `main_window.py` por abas.
7. Expandir testes de regressao e benchmark.

## Definicao de sucesso

O plano sera considerado bem-sucedido quando:

- a interface estiver claramente mais fluida em uso diario
- os recalculos mais comuns forem perceptivelmente mais curtos
- os gargalos estiverem medidos e documentados
- as principais regras fiscais estiverem protegidas por testes
- o codigo estiver mais modular e previsivel para manutencao futura
