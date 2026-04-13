import { useMemo, useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { sqlApi } from "../../api/client";
import type { SqlFile } from "../../api/types";

interface Props {
  isOpen: boolean;
  onClose: () => void;
}

function groupByFolder(files: SqlFile[]): Map<string, SqlFile[]> {
  const map = new Map<string, SqlFile[]>();
  for (const f of files) {
    const slash = f.path.indexOf("/");
    const folder = slash === -1 ? "" : f.path.slice(0, slash);
    if (!map.has(folder)) map.set(folder, []);
    map.get(folder)!.push(f);
  }
  return map;
}

export function GerenciarConsultasModal({ isOpen, onClose }: Props) {
  const queryClient = useQueryClient();

  const { data: sqlFiles = [], isLoading } = useQuery({
    queryKey: ["sqlFiles"],
    queryFn: sqlApi.listFiles,
    staleTime: Infinity,
    enabled: isOpen,
  });

  const [busca, setBusca] = useState("");
  const [pendingDelete, setPendingDelete] = useState<string | null>(null);

  // Formulário de adição
  const [newName, setNewName] = useState("");
  const [newFolder, setNewFolder] = useState("");
  const [newCustomFolder, setNewCustomFolder] = useState("");
  const [newContent, setNewContent] = useState("");
  const [showAddForm, setShowAddForm] = useState(false);
  const [formError, setFormError] = useState<string | null>(null);

  const folders = useMemo(() => {
    const set = new Set<string>();
    for (const f of sqlFiles) {
      const slash = f.path.indexOf("/");
      if (slash !== -1) set.add(f.path.slice(0, slash));
    }
    return [...set].sort();
  }, [sqlFiles]);

  const groups = useMemo(() => {
    const filtered = sqlFiles.filter((f) =>
      f.path.toLowerCase().includes(busca.toLowerCase()),
    );
    return groupByFolder(filtered);
  }, [sqlFiles, busca]);

  const deleteMutation = useMutation({
    mutationFn: (path: string) => sqlApi.deleteFile(path),
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: ["sqlFiles"] });
      setPendingDelete(null);
    },
  });

  const createMutation = useMutation({
    mutationFn: (payload: { name: string; folder: string; content: string }) =>
      sqlApi.createFile(payload),
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: ["sqlFiles"] });
      setNewName("");
      setNewFolder("");
      setNewCustomFolder("");
      setNewContent("");
      setShowAddForm(false);
      setFormError(null);
    },
    onError: (err: unknown) => {
      const msg =
        err instanceof Error ? err.message : "Erro ao criar arquivo.";
      setFormError(msg);
    },
  });

  const handleCreate = () => {
    setFormError(null);
    if (!newName.trim()) {
      setFormError("Informe o nome do arquivo.");
      return;
    }
    const folder =
      newFolder === "__novo__" ? newCustomFolder.trim() : newFolder;
    createMutation.mutate({
      name: newName.trim(),
      folder,
      content: newContent,
    });
  };

  if (!isOpen) return null;

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-slate-950/80 backdrop-blur-sm">
      <div
        className="flex flex-col rounded-lg border border-slate-600 bg-slate-900 shadow-2xl"
        style={{ width: 680, maxHeight: "85vh" }}
      >
        {/* Header */}
        <div className="flex items-center justify-between border-b border-slate-700 px-5 py-3">
          <h2 className="text-sm font-semibold text-slate-100">
            Gerenciar Consultas SQL
          </h2>
          <button
            onClick={onClose}
            className="text-slate-400 hover:text-slate-200 text-lg leading-none"
          >
            ✕
          </button>
        </div>

        {/* Busca + botão adicionar */}
        <div className="flex gap-2 items-center border-b border-slate-700 px-4 py-2">
          <input
            className="flex-1 rounded bg-slate-800 border border-slate-600 px-3 py-1.5 text-xs text-slate-200 placeholder-slate-500 focus:outline-none focus:border-blue-500"
            placeholder="Filtrar por nome ou caminho..."
            value={busca}
            onChange={(e) => setBusca(e.target.value)}
          />
          <button
            onClick={() => setShowAddForm((v) => !v)}
            className="rounded bg-emerald-700 px-3 py-1.5 text-xs font-medium text-white hover:bg-emerald-600"
          >
            {showAddForm ? "Cancelar" : "+ Adicionar"}
          </button>
        </div>

        {/* Formulário de adição */}
        {showAddForm && (
          <div className="border-b border-slate-700 bg-slate-800/50 px-4 py-3 flex flex-col gap-2">
            <div className="text-xs font-semibold text-slate-300 mb-1">
              Novo arquivo SQL
            </div>
            <div className="flex gap-2">
              <div className="flex flex-col gap-1 flex-1">
                <label className="text-[10px] text-slate-400">
                  Nome do arquivo (sem extensão)
                </label>
                <input
                  className="rounded bg-slate-700 border border-slate-600 px-2 py-1 text-xs text-slate-200 focus:outline-none focus:border-blue-500"
                  placeholder="ex: minha_consulta"
                  value={newName}
                  onChange={(e) => setNewName(e.target.value)}
                />
              </div>
              <div className="flex flex-col gap-1 w-44">
                <label className="text-[10px] text-slate-400">Pasta destino</label>
                <select
                  className="rounded bg-slate-700 border border-slate-600 px-2 py-1 text-xs text-slate-200 focus:outline-none focus:border-blue-500"
                  value={newFolder}
                  onChange={(e) => setNewFolder(e.target.value)}
                >
                  <option value="">raiz (sql/)</option>
                  {folders.map((f) => (
                    <option key={f} value={f}>
                      {f}/
                    </option>
                  ))}
                  <option value="__novo__">+ Nova pasta...</option>
                </select>
              </div>
            </div>

            {newFolder === "__novo__" && (
              <input
                className="rounded bg-slate-700 border border-slate-600 px-2 py-1 text-xs text-slate-200 focus:outline-none focus:border-blue-500 w-full"
                placeholder="Nome da nova pasta (ex: relatorios)"
                value={newCustomFolder}
                onChange={(e) => setNewCustomFolder(e.target.value)}
              />
            )}

            <div className="flex flex-col gap-1">
              <label className="text-[10px] text-slate-400">
                Conteúdo SQL
              </label>
              <textarea
                className="rounded bg-slate-700 border border-slate-600 px-2 py-1.5 text-xs text-slate-200 font-mono focus:outline-none focus:border-blue-500 resize-y"
                rows={6}
                placeholder="SELECT ..."
                value={newContent}
                onChange={(e) => setNewContent(e.target.value)}
              />
            </div>

            {formError && (
              <p className="text-[11px] text-rose-400">{formError}</p>
            )}

            <div className="flex justify-end">
              <button
                onClick={handleCreate}
                disabled={createMutation.isPending}
                className="rounded bg-emerald-700 px-4 py-1.5 text-xs font-medium text-white hover:bg-emerald-600 disabled:opacity-50"
              >
                {createMutation.isPending ? "Salvando..." : "Salvar arquivo"}
              </button>
            </div>
          </div>
        )}

        {/* Lista de arquivos */}
        <div className="flex-1 overflow-y-auto px-2 py-2">
          {isLoading && (
            <p className="px-4 py-6 text-center text-xs text-slate-400">
              Carregando...
            </p>
          )}

          {!isLoading && groups.size === 0 && (
            <p className="px-4 py-6 text-center text-xs text-slate-400">
              Nenhuma consulta encontrada.
            </p>
          )}

          {[...groups.entries()].map(([folder, files]) => (
            <div key={folder || "__root__"} className="mb-3">
              {/* Group header */}
              <div className="flex items-center gap-2 px-2 py-1">
                <span className="text-[10px] font-semibold uppercase tracking-wider text-blue-400">
                  {folder || "raiz"}
                </span>
                <span className="text-[10px] text-slate-500">
                  ({files.length})
                </span>
                <div className="flex-1 border-t border-slate-700" />
              </div>

              {/* Files */}
              {files.map((f) => {
                const isDeleting = pendingDelete === f.path;
                const isRunning =
                  deleteMutation.isPending &&
                  deleteMutation.variables === f.path;

                return (
                  <div
                    key={f.path}
                    className="flex items-center gap-2 rounded px-3 py-1.5 hover:bg-slate-800/60"
                  >
                    <span
                      className="flex-1 truncate text-[11px] text-slate-300"
                      title={f.path}
                    >
                      {f.name}
                    </span>
                    <span className="text-[10px] text-slate-500 shrink-0">
                      {f.path}
                    </span>

                    {!isDeleting ? (
                      <button
                        onClick={() => setPendingDelete(f.path)}
                        className="shrink-0 rounded bg-slate-700 px-2 py-0.5 text-[10px] text-rose-400 hover:bg-rose-900/50 hover:text-rose-300"
                      >
                        Excluir
                      </button>
                    ) : (
                      <div className="flex items-center gap-1 shrink-0">
                        <span className="text-[10px] text-rose-300">
                          Confirmar?
                        </span>
                        <button
                          onClick={() =>
                            deleteMutation.mutate(f.path)
                          }
                          disabled={isRunning}
                          className="rounded bg-rose-700 px-2 py-0.5 text-[10px] text-white hover:bg-rose-600 disabled:opacity-50"
                        >
                          {isRunning ? "..." : "Sim"}
                        </button>
                        <button
                          onClick={() => setPendingDelete(null)}
                          className="rounded bg-slate-600 px-2 py-0.5 text-[10px] text-slate-200 hover:bg-slate-500"
                        >
                          Não
                        </button>
                      </div>
                    )}
                  </div>
                );
              })}
            </div>
          ))}
        </div>

        {/* Footer */}
        <div className="flex justify-end border-t border-slate-700 px-5 py-3">
          <button
            onClick={onClose}
            className="rounded bg-slate-700 px-4 py-1.5 text-xs font-medium text-slate-300 hover:bg-slate-600"
          >
            Fechar
          </button>
        </div>
      </div>
    </div>
  );
}
