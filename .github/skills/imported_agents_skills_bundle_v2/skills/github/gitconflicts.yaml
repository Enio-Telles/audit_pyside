---
name: gitconflicts
description: Resolve conflitos de Git com segurança e clareza. Use esta skill para analisar conflitos de merge ou rebase, identificar intenção das mudanças, preservar comportamento correto e orientar validação pós-resolução. Keywords: git conflict, merge conflict, rebase conflict, conflict resolution, merge markers, cherry-pick conflict, stash conflict, resolve conflicts.
---

Esta skill ajuda a resolver conflitos de Git de forma segura, sem se limitar a “fazer o conflito sumir”. O foco é preservar o comportamento correto do sistema e evitar regressões silenciosas.

## Quando usar
Use esta skill quando a tarefa envolver:
- conflitos em merge
- conflitos em rebase
- conflitos em cherry-pick
- conflitos após sincronizar branch com main
- arquivos com marcadores de conflito
- dúvidas sobre qual lado manter
- validação após resolução de conflito

## Objetivo
Resolver conflitos com base na intenção funcional das mudanças, não apenas na versão mais recente do arquivo.

## Instruções
1. Identifique o tipo de operação que gerou o conflito:
   - merge
   - rebase
   - cherry-pick
   - stash apply/pop

2. Leia o contexto do código ao redor do conflito.
   - Não resolva olhando apenas os blocos marcados.
   - Entenda o que cada lado da mudança pretendia fazer.

3. Classifique o conflito:
   - conflito textual simples
   - conflito estrutural
   - conflito semântico
   - conflito de comportamento
   - conflito em configuração ou lockfile

4. Ao resolver:
   - preserve a lógica necessária dos dois lados quando apropriado
   - elimine duplicações introduzidas pela fusão
   - mantenha nomes, imports e interfaces consistentes
   - revise chamadas afetadas por mudanças de assinatura

5. Depois da resolução:
   - rode testes relevantes
   - valide lint e build quando aplicável
   - revise manualmente os pontos integrados
   - confirme que o arquivo final faz sentido como código completo

## Regras
- Não escolha automaticamente “ours” ou “theirs” sem entender o impacto.
- Não trate conflito textual como resolvido semanticamente.
- Não ignore mudanças de API, schema ou contrato escondidas no conflito.
- Não conclua a resolução sem validação mínima.
