<!-- markdownlint-disable-file -->

# Task Details: Audit React - Otimizacao P2-P5

## Research Reference

**Source Research**: #file:../research/20260406-audit-react-otimizacao-research.md

---

## Fase P2: Performance de renderizacao e backend essencial

### Tarefa P2.1: Instalar e integrar @tanstack/react-virtual no DataTable

Substituir o render direto de todas as linhas por um virtualizador de linhas,
de modo que apenas as linhas visiveis no viewport sao montadas no DOM.

- **Arquivos**:
  - `frontend/src/components/table/DataTable.tsx` - adicionar useVirtualizer; substituir map de linhas
  - `frontend/package.json` - nova dependencia @tanstack/react-virtual

- **Implementacao**:
  ```
  pnpm add @tanstack/react-virtual
  ```
  No DataTable.tsx, importar useVirtualizer de @tanstack/react-virtual.
  Criar ref para o container scrollavel da tabela.
  Configurar virtualizer com estimateSize (altura da linha por density) e count=rows.length.
  Substituir `rows.map(...)` por `virtualizer.getVirtualItems().map(vi => rows[vi.index])`.
  Adicionar `paddingTop` e `paddingBottom` no tbody para manter scroll correto.

- **Success**:
  - Tabela com 5000 linhas rola sem lag perceptivel
  - Apenas ~30 linhas no DOM ao mesmo tempo
  - tsc --noEmit sem erros

- **Research References**:
  - #file:../research/20260406-audit-react-otimizacao-research.md (Secao 2.2) - virtualizacao ausente
  - #fetch:https://tanstack.com/virtual/latest/docs/introduction

- **Dependencies**:
  - Instalar @tanstack/react-virtual antes de editar DataTable.tsx

---

### Tarefa P2.2: Criar endpoint /parquet/metadata no backend

Endpoint GET que retorna metadados de um arquivo parquet sem carregar dados.

- **Arquivos**:
  - `backend/routers/parquet.py` - adicionar endpoint get_metadata
  - `frontend/src/api/client.ts` - adicionar parquetApi.metadata()
  - `frontend/src/api/types.ts` - adicionar ParquetMetadata interface

- **Implementacao backend** (parquet.py):
  ```python
  class MetadataResponse(BaseModel):
      path: str
      total_rows: int
      columns: list[str]
      dtypes: dict[str, str]
      sample: list[dict]  # 5 linhas
      numeric_stats: dict  # {col: {min, max, mean, null_count}}

  @router.get("/metadata")
  def get_metadata(path: str):
      svc = _parquet_service
      lf = pl.scan_parquet(path)
      schema = lf.schema
      total = lf.select(pl.len()).collect()[0,0]
      sample = lf.head(5).collect().to_dicts()
      stats = {}
      for col, dtype in schema.items():
          if dtype in (pl.Float64, pl.Float32, pl.Int64, pl.Int32, pl.Int16):
              s = lf.select([
                  pl.col(col).min().alias("min"),
                  pl.col(col).max().alias("max"),
                  pl.col(col).mean().alias("mean"),
                  pl.col(col).null_count().alias("null_count"),
              ]).collect().to_dicts()[0]
              stats[col] = s
      return MetadataResponse(
          path=path, total_rows=total,
          columns=list(schema.keys()),
          dtypes={k: str(v) for k,v in schema.items()},
          sample=[{k: _safe_value(v) for k,v in r.items()} for r in sample],
          numeric_stats=stats
      )
  ```

- **Success**:
  - GET /parquet/metadata?path=... retorna JSON com total_rows, columns, dtypes, sample, numeric_stats
  - Backend testa sem 500

- **Research References**:
  - #file:../research/20260406-audit-react-otimizacao-research.md (Secao 2.1) - metadata endpoint ausente

- **Dependencies**:
  - P2.1 concluida (nao bloqueante, pode rodar em paralelo)

---

### Tarefa P2.3: Criar endpoint /parquet/export-csv no backend

Exportar resultado filtrado completo (sem limite de pagina) como CSV streaming.

- **Arquivos**:
  - `backend/routers/parquet.py` - adicionar endpoint export_csv
  - `frontend/src/api/client.ts` - adicionar parquetApi.exportCsv()
  - `frontend/src/components/tabs/ConsultaTab.tsx` - substituir export client-side por chamada ao endpoint

- **Implementacao backend**:
  ```python
  from fastapi.responses import StreamingResponse
  import io

  @router.post("/export-csv")
  def export_csv(req: QueryRequest):
      svc = _parquet_service
      # reutiliza logica de query mas sem paginar
      df = svc.build_query(req).collect()
      buf = io.StringIO()
      df.write_csv(buf)
      buf.seek(0)
      return StreamingResponse(
          iter([buf.getvalue()]),
          media_type="text/csv",
          headers={"Content-Disposition": "attachment; filename=export.csv"}
      )
  ```

- **Success**:
  - POST /parquet/export-csv retorna CSV com todas as linhas filtradas
  - Botao "Exportar CSV" em ConsultaTab baixa arquivo completo, nao apenas pagina

- **Research References**:
  - #file:../research/20260406-audit-react-otimizacao-research.md (Secao 2.1) - export endpoint ausente

- **Dependencies**:
  - Singleton parquet (P0.1 concluida)

---

### Tarefa P2.4: Summary cards no ConsultaTab

Adicionar faixa de cartoes acima da tabela mostrando totais e indicadores chave.

- **Arquivos**:
  - `frontend/src/components/ui/SummaryCards.tsx` - novo componente
  - `frontend/src/components/tabs/ConsultaTab.tsx` - integrar SummaryCards

- **Interface do componente**:
  ```tsx
  interface SummaryCardsProps {
    totalRows: number;
    filteredRows: number;
    selectedCount?: number;
    loading?: boolean;
  }
  ```

- **Visual**: faixa horizontal com 3-4 cartoes dark compactos acima da DataTable.
  Cada cartao: label pequeno em slate-400 + numero em destaque branco.
  Exemplo: "Total  12.847" | "Filtrado  342" | "Selecionados  5"

- **Dados**: `data?.total_rows` (servidor) e `data?.rows?.length` (pagina atual).
  Filtrado real requer query sem paginar - usar `total_rows` do backend como proxy.

- **Success**:
  - Cartoes aparecem com valores corretos
  - Atualizam ao filtrar e paginar
  - tsc sem erros

- **Research References**:
  - #file:../research/20260406-audit-react-otimizacao-research.md (Secao 2.4) - summary cards ausentes

- **Dependencies**:
  - P2.1 (nao bloqueante)

---

### Tarefa P2.5: placeholderData no ConsultaSqlTab

Evitar flash de conteudo vazio ao navegar entre paginas SQL.

- **Arquivo**: `frontend/src/components/tabs/ConsultaSqlTab.tsx`

- **Mudanca**:
  A ConsultaSqlTab usa `useMutation` (nao `useQuery`), portanto `placeholderData`
  nao se aplica diretamente. A melhoria equivalente e manter o resultado anterior
  visivel com opacidade reduzida enquanto a nova pagina carrega.
  Adicionar `style={{ opacity: execMutation.isPending ? 0.5 : 1 }}` no container
  do DataTable para feedback visual sem apagar os dados anteriores.

- **Success**:
  - Ao mudar de pagina SQL, tabela anterior permanece visivel ate nova chegar
  - isPending mostra opacidade reduzida

- **Dependencies**:
  - P2.1 (nao bloqueante)

---

## Fase P3: DataTable evolution e persistencia

### Tarefa P3.1: Density configuration no DataTable

Adicionar prop `density` com tres modos, alterando padding e tamanho de fonte.

- **Arquivos**:
  - `frontend/src/components/table/DataTable.tsx` - adicionar prop density
  - `frontend/src/components/ui/DensityToggle.tsx` - botao seletor de densidade

- **Implementacao**:
  ```tsx
  type TableDensity = 'compact' | 'normal' | 'comfortable';

  // Em DataTableProps adicionar:
  density?: TableDensity;

  // Mapeamento de classes:
  const densityCls: Record<TableDensity, string> = {
    compact:      'py-0.5 px-1 text-[10px]',
    normal:       'py-1   px-2 text-xs',
    comfortable:  'py-2   px-3 text-sm',
  };
  ```

- **estimateSize no virtualizer** (P2.1): retornar 20/28/36 conforme density.

- **Success**:
  - Tres modos visuais distintos e funcionais
  - DensityToggle aparece na toolbar de ConsultaTab

- **Research References**:
  - #file:../research/20260406-audit-react-otimizacao-research.md (Secao 2.3)

- **Dependencies**:
  - P2.1 (virtualizer precisa de estimateSize por density)

---

### Tarefa P3.2: Row detail drawer no DataTable

Slide-in lateral que exibe todos os campos de uma linha selecionada.

- **Arquivos**:
  - `frontend/src/components/ui/RowDetailDrawer.tsx` - novo componente drawer
  - `frontend/src/components/table/DataTable.tsx` - adicionar prop onRowClick e drawer

- **Interface**:
  ```tsx
  interface RowDetailDrawerProps {
    row: Record<string, unknown> | null;
    columns: string[];
    onClose: () => void;
  }
  ```

- **Visual**: painel direito de largura fixa (360px), position fixed, slide-in com
  transicao CSS. Header com nome da linha (primeira col ou indice), lista de
  label:valor para todos os campos. Fechar com X ou Escape.

- **No DataTable**: adicionar `onRowClick?: (row: Record<string, unknown>) => void`
  e estado `detailRow`. Clicar na linha chama `onRowClick(row)`.

- **Success**:
  - Clicar em qualquer linha abre drawer com todos os campos
  - Fechar funciona com X e Escape
  - Nao interfere com selecao de linhas existente

- **Dependencies**:
  - P3.1 pode ser paralela

---

### Tarefa P3.3: URL sync com URLSearchParams

Sincronizar tab ativa, CNPJ e arquivo selecionado com a URL do navegador.

- **Arquivos**:
  - `frontend/src/hooks/useUrlSync.ts` - novo hook
  - `frontend/src/App.tsx` - usar hook no componente raiz

- **Implementacao**:
  ```tsx
  export function useUrlSync() {
    const { activeTab, setActiveTab, selectedCnpj, setSelectedCnpj } = useAppStore();

    // Write to URL on change
    useEffect(() => {
      const params = new URLSearchParams();
      if (selectedCnpj) params.set('cnpj', selectedCnpj);
      if (activeTab) params.set('tab', activeTab);
      const newUrl = params.toString() ? `?${params}` : '/';
      window.history.replaceState(null, '', newUrl);
    }, [activeTab, selectedCnpj]);

    // Read from URL on mount
    useEffect(() => {
      const params = new URLSearchParams(window.location.search);
      const cnpj = params.get('cnpj');
      const tab  = params.get('tab');
      if (cnpj) setSelectedCnpj(cnpj);
      if (tab)  setActiveTab(tab);
    }, []);
  }
  ```

- **Success**:
  - URL atualiza ao trocar tab ou CNPJ
  - Colar URL em nova aba restaura estado
  - Sem dependencia de react-router (usa apenas window.history)

- **Dependencies**:
  - Nenhuma (independente das outras tarefas P3)

---

### Tarefa P3.4: Persistencia inter-sessao com localStorage

Salvar e restaurar automaticamente o ultimo CNPJ e aba usados.

- **Arquivo**: `frontend/src/store/appStore.ts`

- **Implementacao**:
  Adicionar `subscribeWithSelector` ou usar `zustand/middleware` `persist`:
  ```tsx
  import { persist } from 'zustand/middleware';

  // Envolver apenas os campos a persistir:
  // selectedCnpj, activeTab
  // (nao persistir selectedFile pois o path pode mudar)
  ```

  Alternativa sem middleware: no `setSelectedCnpj` e `setActiveTab`, chamar
  `localStorage.setItem(...)`, e no `create()` ler os valores salvos como
  estado inicial.

- **Success**:
  - Apos fechar e reabrir o app, CNPJ anterior esta selecionado
  - Ultima aba visitada esta ativa
  - setSelecioneFile ainda reseta ao trocar CNPJ

- **Dependencies**:
  - P3.3 (URL sync) deve ser implementada primeiro para evitar conflito de leitura inicial

---

### Tarefa P3.5: XLSX export

Exportar a tabela filtrada completa como arquivo Excel com formatacao.

- **Arquivos**:
  - `frontend/package.json` - adicionar xlsx
  - `frontend/src/components/tabs/ConsultaTab.tsx` - botao "Exportar XLSX"
  - Reutilizar endpoint /parquet/export-csv ou criar /parquet/export-xlsx no backend

- **Instalacao**:
  ```
  pnpm add xlsx
  ```

- **Implementacao frontend** (client-side a partir dos dados da pagina atual ou
  via fetch do export endpoint):
  ```tsx
  import * as XLSX from 'xlsx';

  function exportXlsx(rows, columns, filename) {
    const ws = XLSX.utils.json_to_sheet(rows.map(r =>
      Object.fromEntries(columns.map(c => [c, r[c]]))
    ));
    const wb = XLSX.utils.book_new();
    XLSX.utils.book_append_sheet(wb, ws, 'Dados');
    XLSX.writeFile(wb, filename + '.xlsx');
  }
  ```

- **Success**:
  - Botao "Exportar XLSX" aparece na toolbar de ConsultaTab
  - Arquivo baixado abre corretamente no Excel
  - Cabecalhos em portugues corretos

- **Dependencies**:
  - P2.3 (export-csv backend) para poder exportar resultado completo filtrado

---

## Fase P4: Valor analitico

### Tarefa P4.1: Column statistics panel

Painel lateral que aparece ao clicar em um cabecalho de coluna, mostrando stats.

- **Arquivos**:
  - `frontend/src/components/ui/ColumnStatsPanel.tsx` - novo componente
  - `frontend/src/api/client.ts` - parquetApi.metadata() (criado em P2.2)
  - `frontend/src/components/table/DataTable.tsx` - callback onHeaderClick

- **Dados**: usar o endpoint /parquet/metadata (P2.2) que ja retorna numeric_stats.
  Para colunas categoricas, exibir contagem de valores unicos e top 5 valores.

- **Visual**: drawer lateral (reutilizar base de RowDetailDrawer) com:
  - Nome da coluna + tipo
  - Para numericas: min, max, media, nulos, distribuicao em barra simples
  - Para texto: contagem unicos, top 5 valores com frequencia

- **Success**:
  - Clicar no cabecalho de coluna abre painel com estatisticas corretas
  - Funciona para colunas numericas e categoricas

- **Dependencies**:
  - P2.2 (metadata endpoint) e P3.2 (drawer base)

---

### Tarefa P4.2: ConversaoTab migracao para DataTable base

Substituir a tabela customizada de ~700 linhas na ConversaoTab pelo DataTable
compartilhado, preservando edited inline e logica de fallback de preco.

- **Arquivo**: `frontend/src/components/tabs/ConversaoTab.tsx`

- **Estrategia**:
  1. Identificar a interface de edicao inline (updateFator, batchUpdateUnidRef)
  2. Usar a prop `onCellEdit` (a ser criada em DataTable) ou manter o render
     de celulas especiais via `cellRenderer` callback
  3. Migrar secao de tabela (~linhas 450-700) para usar `<DataTable>` com
     props columns, rows, loading, page, totalPages, onPageChange
  4. Preservar a logica de highlight de fallback de preco via highlightRules

- **Success**:
  - ConversaoTab usa DataTable compartilhado
  - Edicao inline de fator e unid_ref funciona identicamente ao atual
  - Paginacao de 150 linhas mantida
  - Nenhuma regressao visual

- **Dependencies**:
  - P2.1 (virtualizacao), P3.1 (density)

---

### Tarefa P4.3: Zustand store split por dominio

Dividir o AppStore monolitico em stores menores e mais coesos.

- **Arquivos a criar**:
  - `frontend/src/store/appShellStore.ts` - appMode, selectedCnpj, selectedFile, activeTab, leftPanelVisible
  - `frontend/src/store/consultaStore.ts` - todos os campos consultaXxx
  - `frontend/src/store/pipelineStore.ts` - pipelineWatchCnpj, pipelineStatus, pipelinePolling, acoes

- **Arquivo a deprecar**:
  - `frontend/src/store/appStore.ts` - reexportar de cada sub-store para backward compat

- **Estrategia de migracao**:
  1. Criar sub-stores
  2. Re-exportar `useAppStore` como composicao dos tres
  3. Migrar componentes para usar sub-stores diretamente onde fizer sentido
  4. Remover appStore.ts apos validacao

- **Success**:
  - Cada store tem responsabilidade clara
  - Sem quebra de funcionalidade existente
  - tsc sem erros

- **Dependencies**:
  - Completar P3.3 e P3.4 antes (dependem de appStore)

---

### Tarefa P4.4: Multi-sort e filter presets na ConsultaTab

Adicionar ordenacao por multiplas colunas e presets de filtro rapido.

- **Arquivos**:
  - `frontend/src/store/appStore.ts` (ou consultaStore) - consultaSort vira array
  - `frontend/src/components/tabs/ConsultaTab.tsx` - UI de multi-sort
  - `frontend/src/api/client.ts` + `backend/routers/parquet.py` - sort_by vira lista
  - `frontend/src/components/ui/FilterPresets.tsx` - novo componente com presets

- **Presets sugeridos**:
  - "Somente nulos" - filterItem {column: colAtiva, operator: 'nulo'}
  - "Somente divergentes" - filterItem com operador especifico
  - "Ultimos N modificados" - sort por col de data desc

- **Success**:
  - Usuario pode ordenar por 2+ colunas simultaneamente
  - Presets aparecem como botoes rapidos acima da tabela
  - Backend aceita sort_by como lista

- **Dependencies**:
  - P4.3 (store split) recomendado antes

---

## Fase P5: Features avancadas e qualidade

### Tarefa P5.1: Auto-highlight de nulos, outliers e duplicados

Detecao automatica e destaque visual de anomalias nos dados.

- **Arquivos**:
  - `frontend/src/hooks/useAutoHighlight.ts` - hook que gera HighlightRules automaticamente
  - `frontend/src/components/tabs/ConsultaTab.tsx` - opcao "Auto-highlight"

- **Logica**:
  - Nulos: celula vazia/null => fundo vermelho suave
  - Duplicados: valor repetido na coluna chave => fundo amarelo suave
  - Outliers numericos: valor > media+3*desvpad => fundo laranja suave
  - Deteccao client-side sobre os dados da pagina atual para performance

- **Success**:
  - Toggle "Auto-highlight" ativa/desativa destacamento automatico
  - Performance: sem lag perceptivel com 200 linhas

- **Dependencies**:
  - P4.1 (column stats panel) para ter desvpad/media disponivel

---

### Tarefa P5.2: Mini graficos por coluna (recharts)

Adicionar sparkline ou histograma compacto no painel de estatisticas.

- **Arquivos**:
  - `frontend/package.json` - adicionar recharts
  - `frontend/src/components/ui/ColumnStatsPanel.tsx` - integrar BarChart/LineChart

- **Instalacao**: `pnpm add recharts`

- **Graficos por tipo de coluna**:
  - Numerica: histograma de distribuicao (BarChart) com 10 buckets
  - Data: linha de frequencia por mes
  - Categorica: barras horizontais top 10 valores

- **Success**:
  - Grafico aparece no ColumnStatsPanel para colunas com dados suficientes
  - Bundle size nao excede +150KB gzipped

- **Dependencies**:
  - P4.1 (ColumnStatsPanel) deve existir primeiro

---

### Tarefa P5.3: CI basico (.github/workflows/ci.yml)

Criar pipeline de CI que roda lint, typecheck e testes a cada push.

- **Arquivo a criar**: `.github/workflows/ci.yml`

- **Conteudo**:
  ```yaml
  name: CI
  on: [push, pull_request]
  jobs:
    frontend:
      runs-on: ubuntu-latest
      defaults:
        run:
          working-directory: frontend
      steps:
        - uses: actions/checkout@v4
        - uses: pnpm/action-setup@v4
          with: { version: latest }
        - uses: actions/setup-node@v4
          with: { node-version: 20, cache: pnpm }
        - run: pnpm install --frozen-lockfile
        - run: pnpm lint
        - run: pnpm exec tsc --noEmit
        - run: pnpm test --run
    backend:
      runs-on: ubuntu-latest
      steps:
        - uses: actions/checkout@v4
        - uses: actions/setup-python@v5
          with: { python-version: '3.11' }
        - run: pip install -r requirements.txt
        - run: PYTHONPATH=src python -m pytest tests/ -q
  ```

- **Success**:
  - CI passa no branch master
  - Badge de status aparece no README

- **Dependencies**:
  - P5.4 (tests) pode rodar em paralelo

---

### Tarefa P5.4: Test suite minimo

Criar testes para componentes e rotas principals.

- **Frontend** (Vitest + Testing Library):
  - `frontend/src/components/table/DataTable.test.tsx`
    - renderiza sem dados, renderiza com dados, skeleton aparece com loading=true
  - `frontend/src/components/table/ColumnToggle.test.tsx`
    - ocultar coluna, mostrar coluna, reset
  - `frontend/src/hooks/usePipelineStatus.test.ts`
    - hook limpa intervalo ao desmontar

- **Backend** (pytest):
  - `tests/test_parquet_router.py`
    - GET /parquet/metadata retorna 200 com campos corretos
    - POST /parquet/export-csv retorna CSV valido
  - `tests/test_sql_router.py`
    - POST /sql/execute com page=1 retorna total_pages

- **Success**:
  - `pnpm test --run` passa sem falhas
  - `pytest tests/` passa sem falhas
  - Cobertura minima dos fluxos criticos

- **Dependencies**:
  - P2.2 e P2.3 (endpoints metadata e export-csv) devem existir para teste de backend

---

## Dependencies Gerais

- @tanstack/react-virtual (P2.1)
- xlsx (P3.5)
- recharts (P5.2)

## Success Criteria Globais

- DataTable com 5000 linhas rola suavemente
- ConsultaTab mostra summary cards corretos
- Export CSV baixa resultado completo (nao apenas pagina atual)
- URL contem cnpj e tab; estado restaura apos reload
- tsc --noEmit e pnpm lint passam sem erros apos cada fase
- CI verde no branch master
