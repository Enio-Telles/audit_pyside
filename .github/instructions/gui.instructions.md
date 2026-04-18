---
applyTo: "app.py,**/*pyside*.py,**/*gui*.py,src/**/*ui*.py,src/**/*view*.py,src/**/*widget*.py"
---

# GUI Instructions — audit_pyside

## Papel da interface
- A GUI em PySide6 deve orquestrar, consultar e apoiar revisão operacional.
- Não duplique lógica fiscal ou analítica na interface.
- A tela não deve virar fonte de verdade de regra de negócio.

## UX esperada
Ao sugerir fluxos:
- priorize clareza operacional
- mostre progresso de execução
- exponha erros com contexto útil
- preserve navegação simples
- destaque o que é auditável e rastreável

## Boas práticas
- Evite handlers longos.
- Extraia lógica para serviços/controladores quando necessário.
- Não esconda transformação de dados em eventos de botão.
- Mantenha nomes de ações e telas claros.

## Performance
- Não carregar datasets pesados na interface sem necessidade.
- Evite bloquear a UI em tarefas longas.
- Considere execução assíncrona, worker ou fila quando necessário.

## Contratos
- A GUI deve consumir saídas estáveis do pipeline.
- Em caso de inconsistência, corrija na fonte certa em vez de mascarar na tela.
