---
name: githubactions
description: Cria, revisa e corrige workflows de GitHub Actions e processos de CI/CD. Use esta skill para investigar falhas de pipeline, configurar testes, lint, build, cache, releases e automações seguras. Keywords: GitHub Actions, CI, CD, workflow, pipeline, build failure, test automation, lint, cache, matrix build, release automation, permissions, secrets.
---

Esta skill cobre automação de repositórios com GitHub Actions e boas práticas de CI/CD.

## Quando usar
Use esta skill quando a tarefa envolver:
- criar workflow
- corrigir pipeline quebrado
- automatizar testes, lint ou build
- acelerar workflow com cache ou paralelismo
- configurar release automation
- revisar permissões e segurança do pipeline
- investigar falhas intermitentes de CI

## Objetivo
Manter pipelines confiáveis, legíveis, seguras e com custo operacional controlado.

## Instruções
1. Leia o workflow atual e os scripts que ele chama.
2. Entenda em que evento o pipeline roda e qual o objetivo de cada job.
3. Avalie:
   - gatilhos
   - dependências entre jobs
   - steps redundantes
   - uso de cache
   - matriz de versões
   - tempo total
   - falhas de ambiente
   - permissões do token
4. Prefira workflows simples e explícitos.
5. Ao corrigir erro:
   - identifique a causa provável
   - proponha a menor mudança suficiente
   - descreva como validar
6. Ao propor automação nova:
   - explique benefício
   - explique custo
   - explique risco

## Regras
- Não proponha permissões excessivas.
- Não exponha secrets.
- Não adicione complexidade sem ganho claro.
- Não confunda problema do código com problema do pipeline sem evidência.

## Saída esperada
- objetivo do workflow
- estado atual
- problema ou oportunidade
- ajuste proposto
- validação
- riscos
