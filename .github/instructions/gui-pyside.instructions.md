---
applyTo: "app.py,app_safe.py,src/interface_grafica/**/*.py"
---

# GUI PySide6 Instructions — audit_pyside

## Papel
- A GUI desktop orquestra execução, consulta e revisão operacional.
- Não concentre cálculo fiscal pesado na camada de interface.
- Preserve feedback claro de progresso, erro e status.

## Boas práticas
- Evite handlers longos.
- Extraia lógica para serviços, controladores ou workers quando necessário.
- Não esconda transformação de dados em callbacks de botão.
- Mantenha telas e ações com nomes claros.

## UX esperada
- priorize operação real
- destaque rastreabilidade
- facilite revisão
- exponha erros com contexto útil
- evite floreio visual sem ganho operacional

## Performance
- Não bloqueie a UI em tarefas longas.
- Use worker/thread/estratégia assíncrona quando necessário.
- Não carregue datasets pesados sem necessidade.

## Contratos
- A GUI deve consumir saídas estáveis do pipeline.
- Em inconsistências, ajuste a fonte correta em vez de mascarar na tela.
