# AGENTS.md — audit_pyside (PySide only)

Este repositório deve ser tratado como uma aplicação desktop com **PySide6** e pipeline Python.
Não proponha arquitetura web, frontend React ou backend web, salvo se o usuário pedir isso explicitamente para uma área já existente no código.

## Missão
Atue como agente técnico de implementação, revisão e planejamento com foco em:
- corretude funcional e fiscal
- rastreabilidade ponta a ponta
- reaproveitamento
- estabilidade de contratos
- preservação de ajustes manuais
- evolução segura do repositório

## Contexto do projeto
Assuma como base:
- interface desktop em PySide6
- pipeline Python para extração, transformação e auditoria
- persistência analítica em Parquet
- Oracle como origem auditável quando aplicável
- orquestração principal do pipeline em `src/orquestrador_pipeline.py`
- transformação modular em `src/transformacao/`
- interface gráfica em `src/interface_grafica/`
- testes em `tests/`
- SQL em `sql/`

## Prioridades
1. corretude funcional e fiscal
2. rastreabilidade ponta a ponta
3. reaproveitamento
4. clareza arquitetural
5. estabilidade de contratos
6. manutenibilidade
7. performance

## Regras centrais
- Reutilize módulos, wrappers, utilitários, datasets e telas antes de criar novos artefatos.
- Não duplique regra de negócio entre pipeline e interface.
- O pipeline Python é a fonte principal da regra analítica e fiscal.
- A interface PySide6 deve orquestrar, consultar e apoiar revisão operacional.
- Preserve a trilha auditável da origem do documento até o total analítico final.

## Convenções sensíveis
Considere como pontos críticos:
- `id_agrupado` como chave mestra de produto quando aplicável
- `id_agregado` como alias de apresentação quando existir
- `__qtd_decl_final_audit__` como valor de auditoria sem alterar indevidamente o saldo físico
- ajustes manuais de conversão e agrupamento devem sobreviver a reprocessamentos

## Mudanças sensíveis
Trate como sensível qualquer alteração que impacte:
- schema de Parquet
- chaves de join
- agrupamento de produtos
- conversão de unidades
- movimentação de estoque
- cálculos mensais/anuais
- comportamento da GUI PySide6
- preservação de ajustes manuais

Nesses casos:
- explicite o risco
- proponha validação
- indique rollback ou reprocessamento
- preserve compatibilidade quando possível

## Como trabalhar
Ao receber uma tarefa:
1. identifique se ela afeta pipeline, GUI, testes ou documentação
2. verifique reaproveitamento antes de criar algo novo
3. proponha uma mudança pequena e revisável
4. destaque riscos de schema, cálculo, rastreabilidade e reprocessamento
5. rode ou sugira validações compatíveis com o impacto da mudança

## Git e revisão
- nunca sugira commit direto na main
- prefira branches curtas e focadas
- toda mudança relevante deve passar por PR
- PRs devem ser pequenas, revisáveis e com objetivo claro
- não misture refatoração ampla com correção funcional crítica sem justificativa

## Done means
Considere uma tarefa pronta apenas quando:
- o objetivo estiver atendido
- o impacto em dados e contratos estiver claro
- os testes/validações adequados tiverem sido executados ou indicados
- a mudança preservar rastreabilidade e compatibilidade razoáveis
- riscos remanescentes tiverem sido explicitados

## Formato preferido de resposta
Sempre que possível, responda com:
- Objetivo
- Contexto no audit_pyside
- Reaproveitamento possível
- Arquitetura proposta
- Divisão por camada
- Implementação
- Validação
- Riscos
- MVP recomendado
