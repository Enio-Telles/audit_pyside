# Plano de Implementação — GerenciarCnpjModal + Reorganização do Projeto

> **Data:** 07/04/2026  
> **Componente:** `frontend/src/components/modals/GerenciarCnpjModal.tsx`  
> **Backend:** `backend/routers/cnpj.py`  
> **Objetivo:** Refatorar, otimizar, expandir o modal E reorganizar a estrutura do projeto

---

## 📋 Resumo do Componente Atual

O `GerenciarCnpjModal` permite gerenciar arquivos de um CNPJ específico:

| Funcionalidade | Status |
|---|---|
| Listar arquivos Parquet | ✅ Funcional |
| Apagar arquivo individual | ✅ Funcional |
| Apagar todos os Parquet | ✅ Funcional |
| Apagar dados de agregação | ✅ Funcional |
| Apagar fatores de conversão | ✅ Funcional |
| Confirmação antes de apagar | ✅ Funcional |
| Feedback visual | ✅ Básico |

**Problemas identificados:**
- Acoplamento ao `ParquetService` da UI desktop no backend
- Sem feedback de erro (apenas sucesso)
- Sem loading states nas ações de deletar
- Sem paginação na lista de arquivos (>100 arquivos travam)
- Sem busca/filtro na lista de arquivos
- Sem resumo de espaço usado
- `isBusy` global impede ações paralelas seguras
- Sem acessibilidade (aria-labels, roles)
- Sem animações de transição

---

## 📋 Master Todo List

### P0 — Crítico (Correções de Bugs)

- [x] **T01:** Adicionar tratamento de erros nas mutations
- [x] **T02:** Adicionar loading states por ação (não global `isBusy`)
- [x] **T03:** Adicionar paginação/infinite scroll na lista de arquivos
- [x] **T04:** Adicionar resumo de espaço usado por seção
- [x] **T05:** Corrigir `isBusy` para permitir ações independentes

### P1 — Melhorias de UX

- [x] **T06:** Adicionar busca/filtro na lista de arquivos
- [ ] **T07:** Adicionar animações de entrada/saída (Framer Motion)
- [x] **T08:** Adicionar acessibilidade (aria-labels, role="dialog")
- [x] **T09:** Adicionar ícones por tipo de arquivo
- [x] **T10:** Adicionar ordenação na lista (nome, tamanho, data)
- [x] **T11:** Adicionar confirmação com input para "Apagar todos" (digite "APAGAR")

### P2 — Novas Funcionalidades

- [ ] **T12:** Adicionar seção de dados cadastrais (limpar cache Oracle)
- [x] **T13:** Adicionar export de lista de arquivos (JSON/CSV)
- [ ] **T14:** Adicionar histórico de execuções do pipeline
- [ ] **T15:** Adicionar botão "Reprocessar" (re-run pipeline)
- [ ] **T16:** Adicionar seção de notificações Fisconforme (limpar cache)
- [ ] **T17:** Adicionar botão de download de arquivo individual

### P3 — Backend

- [ ] **T18:** Remover acoplamento `interface_grafica` do `cnpj.py`
- [ ] **T19:** Adicionar validação de CNPJ com Pydantic
- [ ] **T20:** Adicionar endpoint de resumo de espaço (`GET /{cnpj}/storage`)
- [ ] **T21:** Adicionar endpoint de histórico de pipeline (`GET /{cnpj}/history`)
- [ ] **T22:** Adicionar rate limiting nas rotas de delete
- [ ] **T23:** Adicionar soft delete (trash com expiração)
- [x] **T24:** Adicionar log de auditoria de deleções

### P4 — Refatoração e Qualidade

- [ ] **T25:** Extrair sub-componentes (FileList, StorageSection, ConfirmDialog)
- [ ] **T26:** Adicionar testes unitários (Vitest + MSW)
- [ ] **T27:** Adicionar type hints completos no backend
- [ ] **T28:** Criar custom hook `useCnpjManager(cnpj)`
- [ ] **T29:** Adicionar error boundary para falhas de API
- [ ] **T30:** Otimizar re-renders com `useMemo` e `useCallback`

---

## 🔍 Detalhamento por Item

---

### T01: Adicionar tratamento de erros nas mutations

**Status:** 🔴 Crítico  
**Impacto:** Usuário não sabe quando falha

**Problema atual:**
```typescript
const deleteFileMutation = useMutation({
  mutationFn: (file: ParquetFile) => cnpjApi.deleteParquetFile(cnpj, file.path),
  onSuccess: (_, file) => {
    invalidateFiles();
    showFeedback(`Arquivo "${file.name}" apagado.`);
  },
  // ❌ Sem onError — falhas silenciosas
});
```

**Solução:**
```typescript
const deleteFileMutation = useMutation({
  mutationFn: (file: ParquetFile) => cnpjApi.deleteParquetFile(cnpj, file.path),
  onSuccess: (_, file) => {
    invalidateFiles();
    showFeedback(`✅ Arquivo "${file.name}" apagado.`, "success");
  },
  onError: (error: AxiosError, file) => {
    showFeedback(
      `❌ Erro ao apagar "${file.name}": ${error.response?.data?.detail || error.message}`,
      "error"
    );
  },
});
```

**Refatorar `showFeedback`:**
```typescript
type FeedbackType = "success" | "error" | "warning";

const [feedback, setFeedback] = useState<{
  msg: string;
  type: FeedbackType;
} | null>(null);

const showFeedback = (msg: string, type: FeedbackType = "success") => {
  setFeedback({ msg, type });
  setTimeout(() => setFeedback(null), 4000);
};
```

**Estimativa:** 1 hora

---

### T02: Loading states por ação

**Status:** 🔴 Crítico  
**Impacto:** UX confusa, múltiplas ações simultâneas

**Problema atual:**
```typescript
const isBusy =
  deleteFileMutation.isPending ||
  deleteAllMutation.isPending ||
  deleteAgregacaoMutation.isPending ||
  deleteConversaoMutation.isPending;
```
Um único `isBusy` global bloqueia tudo.

**Solução:** Estado por arquivo/ação:

```typescript
const [deletingFiles, setDeletingFiles] = useState<Set<string>>(new Set());

const handleDeleteFile = (file: ParquetFile) => {
  setDeletingFiles(prev => new Set(prev).add(file.path));
  deleteFileMutation.mutate(file, {
    onSettled: () => {
      setDeletingFiles(prev => {
        const next = new Set(prev);
        next.delete(file.path);
        return next;
      });
    },
  });
};
```

**UI:**
```tsx
<button disabled={deletingFiles.has(f.path)}>
  {deletingFiles.has(f.path) ? (
    <Spinner className="w-3 h-3" />
  ) : "🗑"}
</button>
```

**Estimativa:** 2 horas

---

### T03: Paginação na lista de arquivos

**Status:** 🔴 Crítico  
**Impacto:** Performance com >100 arquivos

**Problema:** Todos os arquivos renderizados de uma vez.

**Solução:**
```typescript
const FILES_PER_PAGE = 50;
const [filePage, setFilePage] = useState(1);

const paginatedFiles = useMemo(() => {
  const start = (filePage - 1) * FILES_PER_PAGE;
  return files.slice(start, start + FILES_PER_PAGE);
}, [files, filePage]);

const totalPages = Math.ceil(files.length / FILES_PER_PAGE);
```

**UI com paginação:**
```tsx
<div className="flex items-center justify-between mt-2 text-xs text-slate-400">
  <span>{files.length} arquivos</span>
  {totalPages > 1 && (
    <div className="flex gap-1">
      <button
        disabled={filePage === 1}
        onClick={() => setFilePage(p => p - 1)}
        className="px-2 py-0.5 rounded bg-slate-700 disabled:opacity-30"
      >
        ←
      </button>
      <span>{filePage}/{totalPages}</span>
      <button
        disabled={filePage === totalPages}
        onClick={() => setFilePage(p => p + 1)}
        className="px-2 py-0.5 rounded bg-slate-700 disabled:opacity-30"
      >
        →
      </button>
    </div>
  )}
</div>
```

**Estimativa:** 2 horas

---

### T04: Resumo de espaço usado

**Status:** 🔴 Crítico  
**Impacto:** Visibilidade de storage

**Solução:**
```typescript
const storageSummary = useMemo(() => {
  const parquetSize = files.reduce((sum, f) => sum + f.size, 0);
  return {
    parquet: parquetSize,
    parquetFormatted: formatBytes(parquetSize),
  };
}, [files]);
```

**UI no header da seção:**
```tsx
<span className="text-[10px] text-slate-500 ml-2">
  ({storageSummary.parquetFormatted} no total)
</span>
```

**Estimativa:** 1 hora

---

### T05: Corrigir `isBusy` para ações independentes

**Status:** 🔴 Crítico  
**Impacto:** UX limitada

**Problema:** Deletar agregação bloqueia deletar arquivo individual.

**Solução:** Usar loading states individuais (ver T02) + validar por seção:

```typescript
const isSectionBusy = {
  parquet: deleteFileMutation.isPending || deleteAllMutation.isPending,
  agregacao: deleteAgregacaoMutation.isPending,
  conversao: deleteConversaoMutation.isPending,
};
```

**Estimativa:** 1 hora (aproveita T02)

---

### T06: Busca/filtro na lista de arquivos

**Status:** 🟡 Alto Impacto  
**Impacto:** Encontrar arquivos rapidamente

**Solução:**
```typescript
const [searchQuery, setSearchQuery] = useState("");

const filteredFiles = useMemo(() => {
  if (!searchQuery.trim()) return files;
  const q = searchQuery.toLowerCase();
  return files.filter(f => f.name.toLowerCase().includes(q));
}, [files, searchQuery]);
```

**UI:**
```tsx
<input
  type="text"
  placeholder="Buscar arquivos..."
  value={searchQuery}
  onChange={e => setSearchQuery(e.target.value)}
  className="w-full px-2 py-1 text-xs bg-slate-800 border border-slate-700 rounded text-slate-300 placeholder-slate-500 mb-2"
/>
```

**Estimativa:** 1 hora

---

### T07: Animações com Framer Motion

**Status:** 🟡 Alto Impacto  
**Impacto:** UX polida

**Instalação:**
```bash
cd frontend && pnpm add framer-motion
```

**Solução:**
```tsx
import { AnimatePresence, motion } from "framer-motion";

<AnimatePresence>
  {isOpen && (
    <motion.div
      initial={{ opacity: 0 }}
      animate={{ opacity: 1 }}
      exit={{ opacity: 0 }}
      transition={{ duration: 0.2 }}
      className="fixed inset-0 z-50 ..."
    >
      <motion.div
        initial={{ scale: 0.95, y: 20 }}
        animate={{ scale: 1, y: 0 }}
        exit={{ scale: 0.95, y: 20 }}
        className="flex flex-col rounded-lg ..."
      >
        {/* conteúdo */}
      </motion.div>
    </motion.div>
  )}
</AnimatePresence>
```

**Estimativa:** 2 horas

---

### T08: Acessibilidade

**Status:** 🟡 Alto Impacto  
**Impacto:** WCAG compliance

**Solução:**
```tsx
<div
  role="dialog"
  aria-modal="true"
  aria-labelledby="modal-title"
  aria-describedby="modal-description"
  className="fixed inset-0 z-50 ..."
>
  <div className="flex flex-col ...">
    <div id="modal-title" className="text-sm font-bold ...">
      Gerenciar CNPJ
    </div>
    <div id="modal-description" className="text-xs text-slate-400 ...">
      {cnpj} — {razaoSocial}
    </div>

    {/* Fechar com ESC */}
    <button
      onClick={onClose}
      aria-label="Fechar modal"
      onKeyDown={e => { if (e.key === "Escape") onClose(); }}
    >
      ✕
    </button>
  </div>
</div>
```

**Estimativa:** 1 hora

---

### T09: Ícones por tipo de arquivo

**Status:** 🔵 Baixo  
**Impacto:** Scanabilidade visual

**Solução:**
```typescript
function getFileIcon(fileName: string): string {
  if (fileName.includes("produtos")) return "📦";
  if (fileName.includes("itens")) return "📋";
  if (fileName.includes("documentos")) return "📄";
  if (fileName.includes("fatores_conversao")) return "🔄";
  if (fileName.includes("agregacao")) return "📊";
  if (fileName.includes("estoque")) return "🏭";
  return "📁";
}
```

**Estimativa:** 30 minutos

---

### T10: Ordenação na lista

**Status:** 🔵 Baixo  
**Impacto:** Organização

**Solução:**
```typescript
type SortKey = "name" | "size" | "date";
const [sortBy, setSortBy] = useState<SortKey>("name");
const [sortDesc, setSortDesc] = useState(false);

const sortedFiles = useMemo(() => {
  return [...filteredFiles].sort((a, b) => {
    let cmp = 0;
    if (sortBy === "name") cmp = a.name.localeCompare(b.name);
    else if (sortBy === "size") cmp = a.size - b.size;
    if (sortDesc) cmp = -cmp;
    return cmp;
  });
}, [filteredFiles, sortBy, sortDesc]);
```

**Estimativa:** 1 hora

---

### T11: Confirmação reforçada para "Apagar todos"

**Status:** 🟡 Alto Impacto  
**Impacto:** Prevenir deleções acidentais

**Solução:**
```typescript
const [confirmDeleteAll, setConfirmDeleteAll] = useState(false);
const [deleteAllInput, setDeleteAllInput] = useState("");

const isDeleteAllConfirmed = deleteAllInput === "APAGAR";
```

**UI:**
```tsx
{confirmDeleteAll && (
  <div className="bg-rose-950/60 border border-rose-700 rounded p-3">
    <div className="text-xs text-rose-200 font-semibold mb-2">
      ⚠️ Ação destrutiva irreversível
    </div>
    <div className="text-xs text-rose-300 mb-2">
      Digite <code className="bg-rose-900 px-1 rounded">APAGAR</code> para confirmar:
    </div>
    <input
      value={deleteAllInput}
      onChange={e => setDeleteAllInput(e.target.value)}
      className="w-full px-2 py-1 text-xs bg-slate-800 border border-rose-700 rounded text-slate-300"
      placeholder="APAGAR"
    />
    <div className="flex gap-2 mt-2">
      <button
        onClick={() => {
          if (isDeleteAllConfirmed) {
            deleteAllMutation.mutate();
            setConfirmDeleteAll(false);
            setDeleteAllInput("");
          }
        }}
        disabled={!isDeleteAllConfirmed || isBusy}
        className="..."
      >
        Confirmar exclusão permanente
      </button>
      <button onClick={() => setConfirmDeleteAll(false)}>Cancelar</button>
    </div>
  </div>
)}
```

**Estimativa:** 2 horas

---

### T12: Seção de dados cadastrais (Oracle cache)

**Status:** 🟢 Médio  
**Impacto:** Controle de cache

**Backend necessário:**
```python
@router.delete("/{cnpj}/cache/cadastral")
def delete_cadastral_cache(cnpj: str):
    """Apaga o cache de dados cadastrais do CNPJ."""
    cnpj = _sanitize(cnpj)
    cache_file = _caminho_parquet_cadastral(ParquetService(CNPJ_ROOT), cnpj)
    if cache_file.exists():
        cache_file.unlink()
        return {"deleted": True}
    return {"deleted": False}
```

**UI:**
```tsx
<div className="border border-slate-700 rounded p-3">
  <div className="flex items-center justify-between">
    <div>
      <span className="text-xs font-semibold text-slate-300">
        Cache de Dados Cadastrais
      </span>
      <div className="text-[11px] text-slate-500">
        dados_cadastrais_{cnpj}.parquet
      </div>
    </div>
    <button
      onClick={() => /* confirmar e deletar */}
      className="text-[11px] px-2 py-1 rounded bg-slate-700 hover:bg-slate-600"
    >
      Limpar cache
    </button>
  </div>
</div>
```

**Estimativa:** 2 horas

---

### T13: Export de lista de arquivos

**Status:** 🟢 Médio  
**Impacto:** Auditoria

**Solução:**
```typescript
const exportFileList = () => {
  const data = files.map(f => ({
    name: f.name,
    size: f.size,
    sizeFormatted: formatBytes(f.size),
    path: f.path,
  }));
  const blob = new Blob([JSON.stringify(data, null, 2)], { type: "application/json" });
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = `cnpj_${cnpj}_files.json`;
  a.click();
  URL.revokeObjectURL(url);
};
```

**UI:** Botão no header:
```tsx
<button
  onClick={exportFileList}
  className="text-[11px] px-2 py-0.5 rounded bg-slate-700 hover:bg-slate-600 text-slate-200"
>
  📥 Exportar lista
</button>
```

**Estimativa:** 1 hora

---

### T14: Histórico de execuções do pipeline

**Status:** 🟢 Médio  
**Impacto:** Visibilidade de operações

**Backend necessário:** Endpoint `GET /{cnpj}/history`

**UI:**
```tsx
<div className="border border-slate-700 rounded p-3">
  <span className="text-xs font-semibold text-slate-300">
    Histórico de Execuções
  </span>
  {historico.map(h => (
    <div key={h.id} className="flex items-center justify-between text-xs mt-1">
      <span>{h.data}</span>
      <span className={h.sucesso ? "text-emerald-400" : "text-rose-400"}>
        {h.sucesso ? "✅ Sucesso" : "❌ Falha"}
      </span>
      <span className="text-slate-500">{h.duracao}</span>
    </div>
  ))}
</div>
```

**Estimativa:** 4 horas (com backend)

---

### T15: Botão "Reprocessar" pipeline

**Status:** 🟢 Médio  
**Impacto:** Conveniência

**Solução:**
```typescript
const reprocessMutation = useMutation({
  mutationFn: () => pipelineApi.run({
    cnpj,
    incluir_extracao: false,
    incluir_processamento: true,
  }),
  onSuccess: () => {
    showFeedback("Pipeline iniciado para reprocessamento.");
    onClose();
  },
});
```

**UI:** Botão no footer:
```tsx
<button
  onClick={() => reprocessMutation.mutate()}
  disabled={reprocessMutation.isPending}
  className="px-4 py-1.5 rounded text-xs bg-blue-700 hover:bg-blue-600 text-white"
>
  🔄 Reprocessar
</button>
```

**Estimativa:** 2 horas

---

### T16: Seção de notificações Fisconforme

**Status:** 🔵 Baixo  
**Impacto:** Controle de cache

**Backend necessário:** Endpoint para limpar cache Fisconforme do CNPJ.

**Estimativa:** 2 horas

---

### T17: Download de arquivo individual

**Status:** 🔵 Baixo  
**Impacto:** Backup

**Backend necessário:** Endpoint `GET /{cnpj}/download` com query param `path`.

**Estimativa:** 2 horas

---

### T18: Remover acoplamento backend → interface_grafica

**Status:** 🔴 Crítico  
**Local:** `backend/routers/cnpj.py`  
**Impacto:** Arquitetura limpa

**Problema:**
```python
from interface_grafica.services.registry_service import RegistryService
from interface_grafica.services.parquet_service import ParquetService
from interface_grafica.config import CNPJ_ROOT
```

**Solução:**
1. Criar `backend/services/registry_service.py`
2. Criar `backend/services/parquet_service.py`
3. Usar `project_paths.py` para `CNPJ_ROOT`

**Estimativa:** 4 horas

---

### T19: Validação de CNPJ com Pydantic

**Status:** 🟡 Alto Impacto  
**Impacto:** Validação automática

**Solução:**
```python
from pydantic import BaseModel, field_validator

class CNPJAdd(BaseModel):
    cnpj: str

    @field_validator("cnpj")
    @classmethod
    def validate_cnpj(cls, v: str) -> str:
        cleaned = re.sub(r"\D", "", v)
        if len(cleaned) != 14:
            raise ValueError("CNPJ deve ter 14 dígitos")
        return cleaned
```

**Estimativa:** 1 hora

---

### T20: Endpoint de resumo de espaço

**Status:** 🟢 Médio  
**Impacto:** Visibilidade

**Backend:**
```python
@router.get("/{cnpj}/storage")
def get_storage_summary(cnpj: str):
    cnpj = _sanitize(cnpj)
    cnpj_dir = CNPJ_ROOT / cnpj
    if not cnpj_dir.exists():
        return {"total_size": 0, "file_count": 0, "sections": {}}

    files = list(cnpj_dir.rglob("*.parquet"))
    total_size = sum(f.stat().st_size for f in files if f.exists())

    sections = {}
    for pattern, name in [
        ("arquivos_parquet/*.parquet", "raw"),
        ("analises/produtos/*.parquet", "analises"),
    ]:
        section_files = list(cnpj_dir.glob(pattern))
        sections[name] = {
            "count": len(section_files),
            "size": sum(f.stat().st_size for f in section_files if f.exists()),
        }

    return {
        "total_size": total_size,
        "total_size_formatted": format_bytes(total_size),
        "file_count": len(files),
        "sections": sections,
    }
```

**Estimativa:** 2 horas

---

### T21: Endpoint de histórico de pipeline

**Status:** 🟢 Médio  
**Impacto:** Visibilidade

**Solução:** Ler logs do pipeline ou criar tabela de auditoria.

**Estimativa:** 4 horas

---

### T22: Rate limiting em deletes

**Status:** 🟡 Alto Impacto  
**Impacto:** Segurança

**Instalação:**
```bash
pip install slowapi
```

**Backend:**
```python
from slowapi import Limiter
from slowapi.util import get_remote_address

limiter = Limiter(key_func=get_remote_address)

@router.delete("/{cnpj}/parquets/all")
@limiter.limit("2/minute")
def delete_all_parquets(cnpj: str, request: Request):
    # ...
```

**Estimativa:** 2 horas

---

### T23: Soft delete com trash

**Status:** 🔵 Baixo  
**Impacto:** Recuperação de dados

**Solução:** Mover para `.trash/{cnpj}/{timestamp}/` em vez de unlink.

**Estimativa:** 6 horas

---

### T24: Log de auditoria de deleções

**Status:** 🟡 Alto Impacto  
**Impacto:** Compliance

**Solução:**
```python
import logging

audit_logger = logging.getLogger("audit.cnpj")

@router.delete("/{cnpj}/parquets/all")
def delete_all_parquets(cnpj: str):
    # ... deletar ...
    audit_logger.info(f"CNPJ {cnpj}: {len(deleted)} arquivos apagados por {request.client.host}")
    return {"deleted": deleted, "count": len(deleted)}
```

**Estimativa:** 2 horas

---

### T25: Extrair sub-componentes

**Status:** 🟢 Médio  
**Impacto:** Manutenibilidade

**Estrutura proposta:**
```
modals/GerenciarCnpjModal/
├── index.tsx              # Componente principal
├── FileListSection.tsx    # Seção de arquivos Parquet
├── StorageSection.tsx     # Seção de agregação/conversão
├── ConfirmDialog.tsx      # Diálogo de confirmação
├── FeedbackToast.tsx      # Feedback visual
└── hooks/
    ├── useFileActions.ts  # Lógica de delete
    └── useStorageSummary.ts
```

**Estimativa:** 4 horas

---

### T26: Testes unitários

**Status:** 🟢 Médio  
**Impacto:** Confiabilidade

**Testes propostos:**
```typescript
describe("GerenciarCnpjModal", () => {
  it("renderiza com lista vazia", () => { ... });
  it("mostra confirmação antes de deletar", () => { ... });
  it("exibe feedback de sucesso", () => { ... });
  it("exibe erro de API corretamente", () => { ... });
  it("filtra arquivos por busca", () => { ... });
  it("pagina lista com >50 arquivos", () => { ... });
});
```

**Estimativa:** 6 horas

---

### T27: Type hints no backend

**Status:** 🟢 Médio  
**Impacto:** Qualidade

**Arquivos:**
- `backend/routers/cnpj.py` — adicionar tipos em todas as funções

**Estimativa:** 2 horas

---

### T28: Custom hook `useCnpjManager`

**Status:** 🟢 Médio  
**Impacto:** Separação de concerns

**Solução:**
```typescript
function useCnpjManager(cnpj: string) {
  const queryClient = useQueryClient();

  const { data: files, isLoading } = useQuery({
    queryKey: ["files", cnpj],
    queryFn: () => cnpjApi.listFiles(cnpj),
  });

  const deleteFile = useMutation({ ... });
  const deleteAll = useMutation({ ... });
  const deleteAgregacao = useMutation({ ... });
  const deleteConversao = useMutation({ ... });

  const invalidateFiles = () => {
    void queryClient.invalidateQueries({ queryKey: ["files", cnpj] });
  };

  return {
    files,
    isLoading,
    deleteFile,
    deleteAll,
    deleteAgregacao,
    deleteConversao,
    invalidateFiles,
  };
}
```

**Estimativa:** 3 horas

---

### T29: Error boundary

**Status:** 🔵 Baixo  
**Impacto:** Resiliência

**Solução:**
```tsx
import { ErrorBoundary } from "react-error-boundary";

<ErrorBoundary
  fallback={
    <div className="text-xs text-rose-400 bg-rose-900/30 p-3 rounded">
      Erro ao carregar dados do CNPJ. Tente novamente.
    </div>
  }
>
  {/* conteúdo do modal */}
</ErrorBoundary>
```

**Estimativa:** 1 hora

---

### T30: Otimizar re-renders

**Status:** 🔵 Baixo  
**Impacto:** Performance

**Solução:**
```typescript
// Memoizar listas
const FileItem = React.memo(({ file, onDelete }: FileItemProps) => { ... });

// Callbacks estáveis
const handleDelete = useCallback((file: ParquetFile) => {
  deleteFileMutation.mutate(file);
}, [deleteFileMutation]);

// Memoizar cálculos
const storageSummary = useMemo(() => { ... }, [files]);
```

**Estimativa:** 2 horas

---

## 📊 Matriz de Prioridade vs Esforço

| Item | Impacto | Esforço | Prioridade |
|---|---|---|---|
| T01: Erros nas mutations | 🔴 Alto | 1h | **P0** |
| T02: Loading states | 🔴 Alto | 2h | **P0** |
| T03: Paginação | 🔴 Alto | 2h | **P0** |
| T04: Resumo espaço | 🟡 Médio | 1h | **P0** |
| T05: isBusy independente | 🔴 Alto | 1h | **P0** |
| T06: Busca | 🟡 Médio | 1h | **P1** |
| T07: Animações | 🟡 Médio | 2h | **P1** |
| T08: Acessibilidade | 🟡 Médio | 1h | **P1** |
| T09: Ícones | 🔵 Baixo | 0.5h | **P1** |
| T10: Ordenação | 🔵 Baixo | 1h | **P1** |
| T11: Confirmação reforçada | 🟡 Médio | 2h | **P1** |
| T12: Cache cadastral | 🟢 Médio | 2h | **P2** |
| T13: Export lista | 🟢 Médio | 1h | **P2** |
| T14: Histórico | 🟢 Médio | 4h | **P2** |
| T15: Reprocessar | 🟢 Médio | 2h | **P2** |
| T18: Desacoplar backend | 🔴 Alto | 4h | **P3** |
| T19: Validação Pydantic | 🟡 Médio | 1h | **P3** |
| T20: Endpoint storage | 🟢 Médio | 2h | **P3** |
| T22: Rate limiting | 🟡 Médio | 2h | **P3** |
| T24: Log auditoria | 🟡 Médio | 2h | **P3** |
| T25: Sub-componentes | 🟢 Médio | 4h | **P4** |
| T26: Testes | 🟢 Médio | 6h | **P4** |
| T28: Custom hook | 🟢 Médio | 3h | **P4** |
| T30: Otimizar renders | 🔵 Baixo | 2h | **P4** |

---

## 🗺️ Roadmap Sugerido

### Sprint 1 — Estabilidade (3-4 dias)
- T01, T02, T03, T04, T05
- **Objetivo:** Modal funcional, sem bugs críticos

### Sprint 2 — UX (2-3 dias)
- T06, T07, T08, T09, T10, T11
- **Objetivo:** UX polida e acessível

### Sprint 3 — Backend (3-4 dias)
- T18, T19, T20, T22, T24
- **Objetivo:** Backend limpo e seguro

### Sprint 4 — Features (3-4 dias)
- T12, T13, T14, T15, T16, T17
- **Objetivo:** Funcionalidades completas

### Sprint 5 — Qualidade (3-4 dias)
- T25, T26, T27, T28, T29, T30
- **Objetivo:** Código testado e otimizado

---

## 📐 Arquitetura Proposta (Pós-Refatoração)

```
frontend/src/
├── components/
│   └── modals/
│       └── GerenciarCnpjModal/
│           ├── index.tsx              # Orquestrador visual
│           ├── FileListSection.tsx    # Lista + busca + paginação
│           ├── StorageSection.tsx     # Agregação/Conversão/Cadastral
│           ├── ConfirmDialog.tsx      # Confirmação de ações
│           ├── FeedbackToast.tsx      # Notificações
│           └── hooks/
│               ├── useCnpjManager.ts  # Lógica de mutations/queries
│               ├── useFileActions.ts  # Delete com loading state
│               └── useStorageSummary.ts # Cálculos de espaço
│
backend/
├── routers/
│   └── cnpj.py                        # Sem dependência de interface_grafica
├── services/
│   ├── registry_service.py            # Registro de CNPJs
│   └── parquet_service.py             # Manipulação de Parquet
└── middleware/
    └── rate_limiter.py                # Rate limiting
```

---

## 📁 Referências

- [AGENTS.md](../../AGENTS.md) — Guia operacional
- [plano_otimizacao_q.md](../../plano_otimizacao_q.md) — Plano geral do projeto
- [frontend/src/api/client.ts](../../frontend/src/api/client.ts) — API client
- [frontend/src/api/types.ts](../../frontend/src/api/types.ts) — Tipos TypeScript
- [backend/routers/cnpj.py](../../backend/routers/cnpj.py) — Router CNPJ

---

> **Nota:** Este plano é vivo. Priorize P0 primeiro e ajuste conforme feedback.
