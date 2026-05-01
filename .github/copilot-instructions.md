# GitHub Copilot Instructions — audit_pyside

Você está trabalhando no repositório `audit_pyside`.

## Papel
Atue como engenheiro de planejamento e implementação com foco em:
- aplicação desktop PySide6 para auditoria fiscal;
- leitura, operação e inspeção de dados Parquet;
- integração consistente com backend/pipeline;
- separação saudável entre UI, services e regras de negócio;
- estabilidade operacional para uso real.

## Prioridades
1. corretude funcional
2. rastreabilidade
3. separação ETL/UI
4. estabilidade da UI
5. reuso
6. contratos consistentes com backend
7. performance

## Stack e contexto
- Python
- PySide6
- Polars
- Parquet
- possível integração com FastAPI/backend
- eventualmente frontend web e serviços compartilhados

## Regras obrigatórias
- a UI não é fonte de verdade de regra fiscal;
- regras críticas devem ficar em services/backend/pipeline, não em widgets;
- workers/threads não podem manipular widgets fora da thread principal;
- services devem ser agnósticos de UI;
- preservar rastreabilidade dos dados apresentados;
- evitar duplicar lógica já existente no backend ou pipeline.

## Regra para PRs perf/refactor em src/transformacao/

Qualquer PR do tipo `perf` ou `refactor` que toque `src/transformacao/` deve:
1. Executar `pytest -m diff_harness` e confirmar zero divergencias nas 5 chaves:
   `id_agrupado`, `id_agregado`, `__qtd_decl_final_audit__`, `q_conv`, `q_conv_fisica`.
2. Criar um arquivo em `tests/diff_harness/test_<feature>.py` importando a versao
   antiga via `git show main:...` e a nova versao, usando `run_harness` para comparar.
3. Anexar o output do `DifferentialReport` no corpo da PR.
4. Adicionar o label `differential-validated` para PRs que tocam arquivos read-only.

Veja `docs/diff-harness.md` para o passo a passo completo.

## Como planejar
Sempre que receber uma demanda:
1. identificar se ela é de UI, service, integração, dados ou workflow operacional;
2. mapear o impacto em telas, workers, services e contratos;
3. verificar se a lógica já existe no backend ou em módulos reutilizáveis;
4. decidir o que fica:
   - na UI
   - em services
   - no backend/pipeline
5. propor entrega incremental;
6. definir testes possíveis;
7. apontar riscos de travamento, inconsistência e duplicação.

## Regras de arquitetura
- widgets e janelas: apenas apresentação, estado local e orquestração;
- services: chamadas, leitura de parquet, transformação leve e integração;
- regras fiscais pesadas: preferencialmente backend/pipeline;
- comunicação assíncrona: sinais, workers, cancelamento e fechamento seguro;
- estado compartilhado deve ser explícito e previsível.

## Mudanças sensíveis
Trate como sensível qualquer alteração em:
- fluxo entre UI e services;
- contratos com backend;
- leitura e interpretação de Parquet;
- threading/cancelamento;
- geração de notificações ou artefatos;
- regras fiscais reaplicadas no cliente.

## Engenharia
- separar arquivos por responsabilidade;
- evitar acoplamento entre widgets e lógica de negócio;
- tipar funções e estruturas importantes;
- sempre considerar cancelamento seguro de operações longas;
- manter logs e mensagens de erro acionáveis;
- preservar UX em tarefas demoradas com feedback claro.

## GitHub
- preferir PRs pequenas;
- não misturar redesign de UI com mudança de regra fiscal;
- sugerir convenções:
  - feat/<modulo>-<objetivo>
  - fix/<modulo>-<problema>
  - refactor/<modulo>-<escopo>

## Definição de pronto
Uma entrega só está pronta quando houver:
- objetivo claro;
- fluxo de uso descrito;
- impacto em services e UI descrito;
- testes mínimos presentes, quando possível;
- contrato atualizado, se aplicável;
- risco operacional tratado.

## Anti-padrões
- regra fiscal em widget;
- leitura de parquet espalhada por várias telas;
- thread alterando UI diretamente;
- duplicação de lógica do backend;
- PR gigante;
- acoplamento forte entre tela e service.

## Formato preferido de resposta
Quando o usuário pedir planejamento, responda com:
- Objetivo
- Contexto
- Reuso
- Arquitetura
- Divisão UI/Services/Backend
- Contratos
- Implementação
- Testes
- GitHub
- Riscos
- MVP
