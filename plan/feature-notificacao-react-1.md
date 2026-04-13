---
goal: Estender React "Análise em Lote" – tabela completa, link REDESIM, seletor PDF da DSF e geração de notificação TXT
version: 1.0
date_created: 2025-07-10
last_updated: 2025-07-10
owner: Equipe Fiscal
status: 'Planned'
tags: [feature, react, fastapi, fisconforme, notificacao]
---

# Introduction

![Status: Planned](https://img.shields.io/badge/status-Planned-blue)

Esta feature estende o tab "Análise em Lote" (React) com três melhorias principais e a capacidade de gerar o arquivo TXT de notificação fiscal:

1. **Tabela de pendências completa** — exibir todos os 8 campos retornados por `sql/Fisconforme_malha_cnpj.sql` (atualmente apenas 5 são mostrados).
2. **REDESIM como link clicável** — campo REDESIM/URL na seção de Dados Cadastrais renderizado como `<a href target="_blank">`.
3. **Seletor PDF da DSF** — input de arquivo para selecionar o PDF da DSF associado a um CNPJ dentro do card de resultados.
4. **Geração da notificação TXT** — botão "Gerar Notificação" que envia os dados ao backend e faz download do arquivo `notificacao_det_<CNPJ>.txt` preenchido com tabela de pendências + imagens da DSF, no mesmo formato de `modelo/notificacao_det_63614176000153.txt`.

O objetivo final é substituir o fluxo manual do PySide6 pela interface React para geração de notificações em lote.

---

## 1. Requirements & Constraints

- **REQ-001**: A tabela de pendências deve exibir as colunas `ID Pendência`, `ID Notif.`, `Malha ID`, `Título`, `Período`, `Status Pend.`, `Status Notif.`, `Ciência`.
- **REQ-002**: O field `REDESIM` (ou qualquer campo que comece com `http`) nos dados cadastrais deve ser renderizado como `<a href="..." target="_blank" rel="noopener noreferrer">` em vez de texto plano.
- **REQ-003**: Cada card de resultado deve ter uma área para selecionar o arquivo PDF da DSF correspondente (file input), com botão "Selecionar PDF DSF" estilizado com Tailwind.
- **REQ-004**: O botão "Gerar Notificação" deve aparecer somente quando há malhas ou sempre (para possibilitar notificações mesmo sem pendências). Implementar aparecimento condicional: visível sempre após expansão.
- **REQ-005**: O endpoint de geração deve receber: `cnpj`, `dados_cadastrais`, `malhas`, `auditor`, `cargo_titulo`, `matricula`, `contato`, `dsf` (número), `pdf_base64` (opcional), e campos opcionais de auditoria.
- **REQ-006**: O backend deve usar `preencher_modelo()` e `converter_pdf_para_base64_html()` já existentes em `src/interface_grafica/fisconforme/preenchimento.py`.
- **REQ-007**: A resposta do endpoint deve incluir o conteúdo HTML/TXT da notificação em base64 ou como string para download via Blob API no frontend.
- **REQ-008**: O SQL `sql/Fisconforme_malha_cnpj.sql` tem bug: vírgula faltando entre `status_pendencia` e `status_notificacao` na cláusula SELECT final — deve ser corrigido.
- **CON-001**: Sem Pandas no fluxo principal backend; gerar a `{{TABELA}}` como HTML puro em Python.
- **CON-002**: Upload de PDF via multipart ou base64 — usar base64 para simplicidade (PDF de DSF é pequeno, usualmente < 5MB).
- **CON-003**: TypeScript com `verbatimModuleSyntax` — imports de tipo devem usar `import type`.
- **GUD-001**: Manter `FisconformeTab.tsx` sob 550 linhas; extrair `NotificacaoForm` como componente inline se necessário.
- **PAT-001**: Padrão `v = (lo, up) => String(m[lo] ?? m[up] ?? "-")` já presente na tabela de malhas — estender para os novos campos.

---

## 2. Implementation Steps

### Implementation Phase 1 — Corrigir bug SQL e mostrar todos os campos na tabela React

- GOAL-001: Corrigir o SQL bugado e expandir a tabela de malhas no ResultCard para 8 colunas completas.

| Task     | Description                                                                                                                                            | Completed | Date |
| -------- | ------------------------------------------------------------------------------------------------------------------------------------------------------ | --------- | ---- |
| TASK-001 | **[SQL]** Em `sql/Fisconforme_malha_cnpj.sql` linha 37, adicionar vírgula faltando após `status_pendencia` (antes de `status_notificacao`) na cláusula SELECT final | | |
| TASK-002 | **[React – types.ts]** O tipo `MalhaRecord` em `frontend/src/api/types.ts` já inclui todos os 8 campos opcionais — verificar e documentar sem alteração necessária | | |
| TASK-003 | **[React – FisconformeTab.tsx]** No `ResultCard`, substituir o `<thead>` da tabela de malhas (5 colunas) por 8 colunas: `ID Pend.`, `ID Notif.`, `Malha ID`, `Título`, `Período`, `Status Pend.`, `Status Notif.`, `Ciência` | | |
| TASK-004 | **[React – FisconformeTab.tsx]** No `<tbody>` da tabela de malhas, adicionar as células para `id_notificacao`/`ID_NOTIFICACAO`, `malhas_id`/`MALHAS_ID`, `status_notificacao`/`STATUS_NOTIFICACAO` usando o helper `v(lo, up)` | | |

### Implementation Phase 2 — REDESIM como link clicável

- GOAL-002: Renderizar URLs no painel de Dados Cadastrais como hiperlinks clicáveis.

| Task     | Description                                                                                                                                                                                                           | Completed | Date |
| -------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------- | ---- |
| TASK-005 | **[React – FisconformeTab.tsx]** No mapeamento `Object.entries(dc)` dentro do bloco "Dados Cadastrais", criar helper `isUrl(s: string): boolean` que testa se a string começa com `http://` ou `https://` | | |
| TASK-006 | **[React – FisconformeTab.tsx]** Substituir `<span className="text-slate-200 truncate">{String(v ?? "")}</span>` por renderização condicional: se `isUrl(String(v))` → `<a href={String(v)} target="_blank" rel="noopener noreferrer" className="text-blue-400 underline truncate">{String(v)}</a>`, senão manter o span atual | | |

### Implementation Phase 3 — Seletor PDF DSF e formulário do auditor

- GOAL-003: Adicionar estado local por CNPJ para arquivo PDF e campos do auditor no ResultCard expandido.

| Task     | Description                                                                                                                                                                                                            | Completed | Date |
| -------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------- | ---- |
| TASK-007 | **[React – FisconformeTab.tsx]** No `ResultCard`, adicionar estado local: `const [pdfFile, setPdfFile] = useState<File \| null>(null)` | | |
| TASK-008 | **[React – FisconformeTab.tsx]** Adicionar estado local para campos do auditor: `const [auditor, setAuditor] = useState("")`, `const [cargo, setCargo] = useState("Auditor")`, `const [matricula, setMatricula] = useState("")`, `const [contato, setContato] = useState("")`, `const [dsf, setDsf] = useState("")` | | |
| TASK-009 | **[React – FisconformeTab.tsx]** No bloco expandido do `ResultCard`, adicionar após a tabela de malhas uma seção "Gerar Notificação" com: (a) input `type="file" accept=".pdf"` oculto + botão estilizado mostrando nome do arquivo selecionado, (b) inputs para DSF número, Auditor, Cargo, Matrícula, Contato | | |
| TASK-010 | **[React – FisconformeTab.tsx]** Adicionar `<input type="file" accept=".pdf" ref={fileInputRef} onChange={handlePdfChange} className="hidden" />` e botão estilizado `"📎 Selecionar PDF DSF"` que chama `fileInputRef.current?.click()` | | |

### Implementation Phase 4 — Backend: endpoint de geração de notificação

- GOAL-004: Criar endpoint FastAPI `POST /api/fisconforme/gerar-notificacao` que usa a lógica de `preenchimento.py`.

| Task     | Description                                                                                                                                                                                                                   | Completed | Date |
| -------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------- | ---- |
| TASK-011 | **[Backend – fisconforme.py]** Adicionar modelo Pydantic `GerarNotificacaoRequest` com campos: `cnpj: str`, `razao_social: str`, `ie: str`, `dsf: str`, `auditor: str`, `cargo_titulo: str`, `matricula: str`, `contato: str`, `malhas: List[Dict[str, Any]]`, `pdf_base64: Optional[str] = None` | | |
| TASK-012 | **[Backend – fisconforme.py]** Criar função `_gerar_tabela_html(malhas: List[Dict]) -> str` que gera HTML `<table>` com 8 colunas formatadas (ID Pend, ID Notif, Malha ID, Título, Período, Status Pend, Status Notif, Ciência) com estilos inline compatíveis com o modelo HTML existente | | |
| TASK-013 | **[Backend – fisconforme.py]** Criar função `_converter_pdf_base64_para_html(pdf_base64: str, dsf_numero: str) -> str` que: decodifica o base64 → bytes, abre com `fitz.open(stream=bytes, filetype="pdf")`, renderiza páginas com DPI=170, retorna HTML com imagens base64 idêntico ao de `preenchimento.converter_pdf_para_base64_html()` | | |
| TASK-014 | **[Backend – fisconforme.py]** Criar endpoint `@router.post("/gerar-notificacao")` que: (1) lê `modelo/modelo_notificacao_fisconforme_n_atendido.txt`, (2) monta `dados` dict com todos os placeholders, (3) chama `_gerar_tabela_html()` para `{{TABELA}}`, (4) chama `_converter_pdf_base64_para_html()` se `pdf_base64` presente para `{{DSF_IMAGENS}}`, (5) substitui todos os placeholders, (6) retorna `{"conteudo": "<html string>", "nome_arquivo": "notificacao_det_<cnpj>.txt"}` | | |
| TASK-015 | **[Backend – fisconforme.py]** O endpoint deve usar `MODELO_PATH = Path("c:/Sistema_pysisde/modelo/modelo_notificacao_fisconforme_n_atendido.txt")` — verificar se o caminho deve ser relativo ao `__file__` ou absoluto, usar `Path(__file__).parent.parent.parent / "modelo" / "modelo_notificacao_fisconforme_n_atendido.txt"` para portabilidade | | |

### Implementation Phase 5 — Frontend: integração do botão "Gerar Notificação"

- GOAL-005: Conectar o formulário do auditor ao endpoint e oferecer download do arquivo TXT gerado.

| Task     | Description                                                                                                                                                                                                             | Completed | Date |
| -------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------- | ---- |
| TASK-016 | **[API types – types.ts]** Adicionar interface `GerarNotificacaoRequest` com os campos: `cnpj`, `razao_social`, `ie`, `dsf`, `auditor`, `cargo_titulo`, `matricula`, `contato`, `malhas`, `pdf_base64?: string` | | |
| TASK-017 | **[API client – client.ts]** Adicionar método `gerarNotificacao(payload: GerarNotificacaoRequest)` em `fisconformeApi` que faz `api.post<{ conteudo: string; nome_arquivo: string }>('/fisconforme/gerar-notificacao', payload).then(r => r.data)` | | |
| TASK-018 | **[React – FisconformeTab.tsx]** Adicionar estado `const [gerando, setGerando] = useState(false)` e `const [erroGeracao, setErroGeracao] = useState("")` no `ResultCard` | | |
| TASK-019 | **[React – FisconformeTab.tsx]** Implementar `handleGerarNotificacao()`: (1) converte `pdfFile` para base64 se presente (usando `FileReader`), (2) chama `fisconformeApi.gerarNotificacao(payload)`, (3) cria `Blob` com o conteúdo retornado, (4) cria URL temporária + `<a>` com `download={nome_arquivo}` e faz `.click()` para trigger do download | | |
| TASK-020 | **[React – FisconformeTab.tsx]** Adicionar botão `"Gerar Notificação"` com estado de loading (`gerando`) e exibição de erro (`erroGeracao`) abaixo do formulário de auditoria | | |

---

## 3. Alternatives

- **ALT-001**: Usar `multipart/form-data` para upload do PDF em vez de base64 — descartado por simplicidade; PDFs de DSF são pequenos e base64 evita complexidade de `FormData` no frontend.
- **ALT-002**: Reutilizar diretamente `preenchimento.preencher_modelo()` em vez de duplicar a lógica no endpoint — possível, mas requereria refatorar o path_resolver e as importações relativas do módulo PySide6; optou-se por reimplementar os helpers direto no router para desacoplamento.
- **ALT-003**: Salvar o arquivo no servidor e retornar uma URL de download — descartado; preferível retornar o conteúdo diretamente para evitar gestão de arquivos temporários no servidor.
- **ALT-004**: Gerar o download via `StreamingResponse` do FastAPI — mais limpo mas requer mudança no axios; base64 + Blob API no frontend é suficiente.

---

## 4. Dependencies

- **DEP-001**: `PyMuPDF` (`fitz`) — já em `requirements.txt`; confirmar disponibilidade no início do TASK-013.
- **DEP-002**: `modelo/modelo_notificacao_fisconforme_n_atendido.txt` — arquivo template já existe.
- **DEP-003**: `frontend/src/api/types.ts` — tipo `MalhaRecord` com 8 campos já existe (sem alteração necessária).
- **DEP-004**: `backend/routers/fisconforme.py` — router já registrado em `backend/main.py`.

---

## 5. Files

- **FILE-001**: `sql/Fisconforme_malha_cnpj.sql` — correção do bug de vírgula faltante (TASK-001)
- **FILE-002**: `frontend/src/components/tabs/FisconformeTab.tsx` — expansão da tabela + links + seletor PDF + formulário auditor + botão de geração (TASK-003 a TASK-010, TASK-018 a TASK-020)
- **FILE-003**: `frontend/src/api/types.ts` — novo tipo `GerarNotificacaoRequest` (TASK-016)
- **FILE-004**: `frontend/src/api/client.ts` — novo método `fisconformeApi.gerarNotificacao` (TASK-017)
- **FILE-005**: `backend/routers/fisconforme.py` — modelo Pydantic, funções helper e endpoint de geração (TASK-011 a TASK-015)

---

## 6. Testing

- **TEST-001**: Verificar que o SQL corrigido não retorna erro sintático ao ser executado contra o Oracle (o campo `status_notificacao` deve aparecer como coluna separada).
- **TEST-002**: Abrir o tab "2. Resultados" e verificar que a tabela de malhas exibe 8 colunas para um CNPJ com dados no cache.
- **TEST-003**: Verificar que campos com URLs no painel de Dados Cadastrais são renderizados como links azuis clicáveis (testar com campo que contenha `https://` no valor).
- **TEST-004**: Selecionar um arquivo PDF no seletor de DSF — verificar que o nome do arquivo é exibido no botão.
- **TEST-005**: Clicar em "Gerar Notificação" sem PDF — verificar que o arquivo TXT é gerado com `{{DSF_IMAGENS}}` vazio mas os demais campos preenchidos.
- **TEST-006**: Clicar em "Gerar Notificação" com PDF DSF selecionado — verificar que o TXT contém as imagens base64 das páginas do PDF.
- **TEST-007**: Verificar que o arquivo gerado tem a mesma estrutura de `modelo/notificacao_det_63614176000153.txt` (header SEFIN, corpo, tabela, página quebra, imagens DSF).
- **TEST-008**: Executar `cd frontend && pnpm lint && pnpm exec tsc --noEmit` e verificar ausência de erros.

---

## 7. Risks & Assumptions

- **RISK-001**: PDF de DSF > 5MB pode causar lentidão na serialização base64 no browser — mitigar alertando o usuário se `pdfFile.size > 5_000_000`.
- **RISK-002**: O campo com a URL do REDESIM no Oracle pode ter nome variado (ex: `REDESIM`, `URL_REDESIM`, `LINK`) — a detecção por valor (`startsWith("http")`) é mais robusta que detecção por nome de campo.
- **RISK-003**: Portabilidade do path do modelo TXT — usar `Path(__file__).resolve().parent.parent.parent / "modelo"` para garantir resolução correta independente do CWD do processo uvicorn.
- **ASSUMPTION-001**: O campo `IE` (Inscrição Estadual) em `dados_cadastrais` retorna como `INSCRICAO_ESTADUAL` ou `IE` do Oracle — o código deve tentar ambos: `dc.get("IE") or dc.get("INSCRICAO_ESTADUAL") or ""`.
- **ASSUMPTION-002**: `[[DSF_IMAGENS]]` deve ser deixado como string vazia quando não há PDF selecionado (conforme comportamento atual de `preenchimento.py`).
- **ASSUMPTION-003**: Os campos do auditor (nome, cargo, matrícula, contato) são preenchidos manualmente pelo usuário na interface — não existe fonte Oracle para esses dados no escopo atual.

---

## 8. Related Specifications / Further Reading

- [plan/feature-auditor-dsf-preview-1.md](feature-auditor-dsf-preview-1.md) — Plan para a versão PySide6 da mesma feature
- [modelo/modelo_notificacao_fisconforme_n_atendido.txt](../modelo/modelo_notificacao_fisconforme_n_atendido.txt) — Template com todos os placeholders
- [modelo/notificacao_det_63614176000153.txt](../modelo/notificacao_det_63614176000153.txt) — Exemplo de saída esperada
- [src/interface_grafica/fisconforme/preenchimento.py](../src/interface_grafica/fisconforme/preenchimento.py) — Lógica de preenchimento do modelo (referência para o backend)
- [backend/routers/fisconforme.py](../backend/routers/fisconforme.py) — Router FastAPI onde o endpoint será adicionado
