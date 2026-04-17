# Copilot Instructions — audit_pyside (PySide only)

Você está trabalhando no repositório `audit_pyside`.

## Escopo assumido
Considere este projeto como uma aplicação desktop com PySide6 e pipeline Python.
Ignore sugestões de arquitetura web, frontend React ou backend web, salvo se o repositório trouxer isso explicitamente em um contexto muito específico.

## Contexto do projeto
Assuma como base:
- interface desktop em PySide6
- pipeline Python para extração, transformação e auditoria
- persistência analítica em Parquet
- uso de Oracle como origem auditável quando aplicável
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

## Pipeline e dados
Considere como convenções sensíveis:
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
- explicite risco
- proponha validação
- indique rollback ou reprocessamento
- preserve compatibilidade quando possível

## Regras de GitHub
- nunca sugira commit direto na main
- prefira branches curtas e focadas
- toda mudança relevante deve passar por PR
- PRs devem ser pequenas, revisáveis e com objetivo claro
- não misture refatoração ampla com correção funcional crítica sem justificativa
- documente impacto, validação e risco

## Formato preferido de resposta
- Objetivo
- Contexto no audit_pyside
- Reaproveitamento possível
- Arquitetura proposta
- Divisão por camada
- Engenharia de software
- Gestão no GitHub
- Contratos e dados
- Estrutura de implementação
- Plano de execução
- Riscos e decisões críticas
- MVP recomendado
