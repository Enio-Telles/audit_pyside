# Fiscal Parquet Analyzer — Web Frontend

Interface web completa espelhando a aplicação PySide6 existente, com backend FastAPI e frontend React 19.

## Arquitetura

```
backend/        FastAPI (Python) — expõe serviços existentes via REST
  main.py
  routers/
    cnpj.py           GET/POST/DELETE /api/cnpj, /files, /schema
    parquet.py        POST /api/parquet/query
    pipeline.py       POST /api/pipeline/run, GET /status
    estoque.py        GET /api/estoque/{cnpj}/mov_estoque|mensal|anual|...
    aggregation.py    GET /api/aggregation/{cnpj}/tabela_agrupada
    sql_query.py      GET/POST /api/sql/...

frontend/       React 19 + Vite + TypeScript + Tailwind CSS
  src/
    api/          axios client + tipos TypeScript
    store/        Zustand (estado global: CNPJ, arquivo, filtros, tabs)
    components/
      layout/     LeftPanel (lista CNPJs, arquivos, pipeline)
      table/      DataTable (TanStack Table) + FilterBar
      tabs/       ConsultaTab, ConsultaSqlTab, AgregacaoTab,
                  ConversaoTab, EstoqueTab, LogsTab
```

## Como rodar

### 1. Backend (porta 8000)

```powershell
conda activate audit
cd backend
uvicorn main:app --host 0.0.0.0 --port 8000 --reload
```

Ou execute `backend/start.ps1`.

### 2. Frontend (porta 5173)

```powershell
cd frontend
pnpm install
pnpm dev
```

Ou execute `frontend/start.ps1`.

Acesse **http://localhost:5173** — o Vite proxeia `/api` para o backend automaticamente.

### ObservaÃ§Ãµes sobre dependÃªncias

- O frontend prioriza `pnpm` quando `frontend/pnpm-lock.yaml` estiver presente.
- O `app_react.py` valida se o binÃ¡rio real do Vite existe antes de iniciar o frontend.
- Se `node_modules` estiver inconsistente, por exemplo apÃ³s mover a pasta do projeto, o launcher reinstala automaticamente as dependÃªncias.

## Funcionalidades implementadas

| Aba          | Funcionalidade                                              |
|--------------|-------------------------------------------------------------|
| Consulta     | Filtros dinâmicos, paginação, seleção de colunas, export CSV |
| Consulta SQL | Carregar arquivos .sql, editar e executar contra Oracle     |
| Agregação    | Tabela agrupada com busca por desc/NCM/CEST                 |
| Conversão    | Fatores de conversão de unidades com filtro                 |
| Estoque      | Subtabs: mov_estoque, tabela mensal, tabela anual, id_agrupados |
| Logs         | Status e progresso do pipeline em tempo real                |

Na barra lateral, a lista `CNPJs registrados` exibe também a razão social a partir do parquet `dados_cadastrais_<cnpj>.parquet` em `dados/CNPJ/<cnpj>/arquivos_parquet/`.

Se a razão social não estiver disponível no parquet local, o backend refaz a consulta `sql/dados_cadastrais.sql` usando as credenciais Oracle do `.env`, recria o parquet cadastral do CNPJ e tenta preencher a informação automaticamente. Se a consulta falhar, a listagem continua operando e mantém o fallback visual de razão social indisponível.

O fallback respeita o placeholder real definido na SQL cadastral, como `:CO_CNPJ_CPF`, evitando acoplamento com nomes fixos de bind no backend.

Na aba `Estoque`, quando um parquet analítico ainda não existir para o CNPJ selecionado, as rotas retornam paginação vazia em vez de `404`. Isso preserva o contrato da UI, evita erro de consulta no frontend e mantém a distinção entre ausência de dados e falha de processamento.

No enriquecimento fiscal da `mov_estoque`, quando não existir vigência compatível no `sitafe_produto_sefin_aux.parquet`, o campo `it_pc_interna` permanece vazio. Isso evita projetar a alíquota mais recente sobre movimentos históricos sem cobertura temporal válida.

## Tema visual

Dark navy matching a UI PySide6 original (`#0a1628` base, `#0f1b33` cards, azul accent).

## Preferencias visuais das tabelas

Nas abas `Consulta`, `Agregacao`, `Conversao` e `Estoque`, o menu `Colunas` permite controlar tres aspectos visuais das tabelas:

- mostrar ou ocultar colunas;
- mudar a ordem de exibicao;
- ajustar a largura em pixels de cada coluna.

Essas preferencias sao persistidas no `localStorage` do navegador para o usuario atual, de forma independente por aba e por subtabela quando aplicavel. O botao `Padrao` restaura a ordem original recebida do backend, redefine as larguras base de cada tela e volta a exibir todas as colunas.

Nas tabelas que usam o componente compartilhado (`Consulta`, `Agregacao` e `Estoque`), a ordem pode ser alterada arrastando o cabecalho da coluna e a largura pode ser ajustada arrastando a borda direita do cabecalho. Na aba `Conversao`, a tabela customizada recebeu a mesma interacao direta no cabecalho sem alterar a logica de edicao inline do fator e da unidade de referencia.
