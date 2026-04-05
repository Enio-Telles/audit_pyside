
## 2024-04-05 - Melhora acessibilidade na barra de filtros
**Learning:** Itens de formulário e botões sem labels explícitos diminuem a acessibilidade do app.
**Action:** Sempre usar `<label>` associado a inputs via `htmlFor` e fornecer `aria-label` para botões que contenham apenas ícones.
## 2024-04-05 - Use explicitly associated <label> for main UI inputs
**Learning:** Found that layout components (like LeftPanel) were using generic `<div>` or `<span>` elements as "labels" for critical inputs (e.g., CPF/CNPJ, Data limite). This means screen readers will not announce the purpose of the input.
**Action:** Always replace purely visual text labels next to inputs with a proper `<label>` element containing an `htmlFor` attribute that matches the input's `id`. This ensures both visual and assistive technology users understand the input's purpose without affecting layout styles.
## 2024-05-24 - Improve DataTable pagination and selection accessibility
**Learning:** Found missing aria labels for checkboxes and pagination buttons. Added `aria-label` and `title` to them, and improved visual feedback with `disabled:cursor-not-allowed` for disabled buttons.
**Action:** Always provide `aria-label` or `title` for icon-only elements like `«` and `»` pagination buttons or checkboxes to ensure screen readers can announce them. Also add visual cues like `disabled:cursor-not-allowed` for disabled states.
