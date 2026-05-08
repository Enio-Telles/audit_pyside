# AGENTS.md — src/interface_grafica

Estas instruções valem para toda a árvore `src/interface_grafica/`.

## Papel desta área
A GUI desktop em PySide6 deve orquestrar execução, consulta e revisão operacional.
Ela não deve virar a fonte principal da regra fiscal ou analítica.

## Regras específicas
- Não concentre cálculo fiscal pesado na camada de interface.
- Evite handlers longos.
- Extraia lógica para serviços, controladores ou workers quando necessário.
- Não esconda transformação de dados em callbacks de botão.
- Mantenha telas, ações e mensagens com nomes claros.
- Preserve feedback claro de progresso, erro e status.

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
