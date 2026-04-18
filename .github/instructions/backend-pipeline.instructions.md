---
applyTo: "**/*.py,src/**/*.py,scripts/**/*.py,tests/**/*.py"
---

# Backend/Pipeline Instructions — audit_pyside

## Papel do código Python
- O pipeline Python é a fonte principal da regra de negócio.
- Não empurre regra fiscal para a GUI.
- Prefira funções e módulos explícitos, testáveis e rastreáveis.

## Organização
- Separe por domínio e etapa do pipeline.
- Evite arquivos “faz tudo”.
- Prefira módulos pequenos, coesos e fáceis de revisar.
- Respeite a existência de wrappers de compatibilidade e implemente a regra real no lugar correto.

## Pipeline
- Preserve a ordem oficial do pipeline.
- Explicite dependências entre datasets.
- Evite acoplamento oculto entre etapas.
- Ao alterar uma etapa, avalie impacto nas posteriores.

## Dados
- Preserve `id_agrupado` como chave mestra quando aplicável.
- Trate `id_agregado` como alias de apresentação quando houver.
- Preserve `__qtd_decl_final_audit__` como campo de auditoria, sem alterar indevidamente saldo físico.
- Ajustes manuais de conversão e agrupamento não podem se perder em reprocessamentos.

## Testes
- Inclua testes unitários para regra crítica.
- Inclua testes de integração para etapas encadeadas.
- Cubra regressões de estoque, conversão, agrupamento e cálculos mensais/anuais.

## Observabilidade
- Logue contexto suficiente para diagnóstico:
  - CNPJ
  - período
  - etapa
  - dataset
  - arquivo de saída
- Trate falhas esperadas explicitamente.
