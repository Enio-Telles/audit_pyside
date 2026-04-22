# AGENTS.md — src/interface_grafica

Estas instruções valem para toda a árvore `src/interface_grafica/`.
Para regras transversais (chaves invariantes, anti-padrões gerais, formato de resposta),
veja `AGENTS.md` na raiz.

---

## Papel desta área

A GUI desktop em PySide6 deve **orquestrar execução, consultar resultados e apoiar revisão operacional**.
Ela não é e nunca deve se tornar a fonte principal da regra fiscal ou analítica.

---

## Estado atual — atenção P3

| Arquivo | Linhas | Status |
|---|---|---|
| `src/interface_grafica/ui/main_window.py` | ~10 366 | **Marcado para decomposição em P3** |
| `src/interface_grafica/ui/main_window_safe.py` | — | Herda `BaseMainWindow`; adiciona shutdown seguro de workers |

**P3 decompõe `main_window.py` em:**
- `ui/windows/` — janelas e diálogos
- `ui/controllers/` — lógica de coordenação
- `ui/widgets/` — componentes reutilizáveis

Até a decomposição: **não adicione novas lógicas de negócio a `main_window.py`**.
Extraia qualquer nova lógica para um serviço ou controller antes de integrar à janela.

---

## Regras específicas

### Separação de responsabilidades
- **Não concentre cálculo fiscal pesado na camada de interface.**
- Nenhuma regra fiscal nova deve ser implementada na GUI — sempre delegue a `src/transformacao/`.
- Evite handlers longos (`on_click_*`); extraia lógica para serviços ou workers.
- Não esconda transformação de dados em callbacks de botão.
- Mantenha telas, ações e mensagens com nomes claros em português.

### Workers e performance
- Não bloqueie a UI em tarefas longas.
- Use `QThread` / worker pattern para qualquer operação de I/O ou pipeline demorado.
- Não carregue datasets pesados sem necessidade (lazy load via serviços).
- Sinalize progresso, erro e status de forma clara ao usuário.

### Tema visual (QSS inline)
A aplicação usa paleta escura aplicada via `setStyleSheet` inline (estilo "High-Contrast Noir"):

| Uso | Cor |
|---|---|
| Aviso / destaque amarelo | `#ccaa00` |
| Sucesso / positivo | `#4caf50` |
| Erro / negativo | `#e57373` |
| Destaque azul (seleção) | `#1e40af` |

Ao adicionar novos elementos visuais, respeite esta paleta para manter consistência.
Não introduza estilos que contradizem o tema escuro atual.

### Contratos com o pipeline
- A GUI deve consumir saídas **estáveis** do pipeline (`src/transformacao/`).
- Em inconsistências entre dados e tela, ajuste a fonte correta no pipeline; não mascare na GUI.
- Preserve as 5 chaves invariantes (`id_agrupado`, `id_agregado`, `__qtd_decl_final_audit__`,
  `q_conv`, `q_conv_fisica`) ao exibir ou editar dados.

### UX esperada
- Priorize operação real sobre visual.
- Destaque rastreabilidade (origem do dado, período, CNPJ).
- Facilite revisão de ajustes manuais.
- Exponha erros com contexto útil (evite mensagens genéricas).
- Não exponha stack traces brutos ao usuário (risco de Information Disclosure).

---

## Serviços disponíveis

| Serviço | Localização | Responsabilidade |
|---|---|---|
| `ServicoAgregacao` | `services/aggregation_service.py` | Merge/unmerge de produtos agrupados |
| `SqlService` | `services/sql_service.py` | Execução de SQL via catálogo |
| `ParquetService` | `services/parquet_service.py` | Leitura de Parquets por CNPJ |

---

## Anti-padrões

- Implementar regra fiscal diretamente na GUI.
- Bloquear a thread principal com I/O ou processamento pesado.
- Expor exceções brutas ao usuário (Information Disclosure).
- Criar novos cálculos de `q_conv` ou `id_agrupado` na camada de interface.
- Adicionar mais lógica a `main_window.py` sem extrair para serviço/controller primeiro.

---

## Formato de resposta

Use o formato padrão definido em `AGENTS.md` da raiz:
**Objetivo / Contexto / Reaproveitamento / Arquitetura / Implementação / Validação / Riscos / MVP**
