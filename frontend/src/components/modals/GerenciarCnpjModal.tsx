import { useState, useMemo, useCallback } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import type { AxiosError } from "axios";
import { cnpjApi, pipelineApi } from "../../api/client";
import type { ParquetFile } from "../../api/types";

interface Props {
  cnpj: string;
  razaoSocial: string | null;
  isOpen: boolean;
  onClose: () => void;
}

type FeedbackType = "success" | "error" | "warning";
type SortKey = "name" | "size";
type SimpleConfirm = { label: string; onConfirm: () => void };

const FILES_PER_PAGE = 50;

function formatBytes(bytes: number): string {
  if (bytes === 0) return "0 B";
  const k = 1024;
  const sizes = ["B", "KB", "MB", "GB"];
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  return `${parseFloat((bytes / Math.pow(k, i)).toFixed(1))} ${sizes[i]}`;
}

function getFileIcon(name: string): string {
  if (name.includes("produtos")) return "📦";
  if (name.includes("itens")) return "📋";
  if (name.includes("documentos")) return "📄";
  if (name.includes("fatores_conversao")) return "🔄";
  if (name.includes("agregacao")) return "📊";
  if (name.includes("estoque")) return "🏭";
  if (name.includes("cadastral")) return "🏢";
  return "📁";
}

function formatError(error: unknown): string {
  const e = error as AxiosError<{ detail?: string }>;
  return e?.response?.data?.detail ?? (e as Error)?.message ?? "Erro desconhecido";
}

export function GerenciarCnpjModal({ cnpj, razaoSocial, isOpen, onClose }: Props) {
  const queryClient = useQueryClient();

  // Busca, ordenação e paginação (T06, T10, T03)
  const [searchQuery, setSearchQuery] = useState("");
  const [sortBy, setSortBy] = useState<SortKey>("name");
  const [sortDesc, setSortDesc] = useState(false);
  const [filePage, setFilePage] = useState(1);

  // Loading state por arquivo (T02)
  const [deletingFiles, setDeletingFiles] = useState<Set<string>>(new Set());

  // Diálogos de confirmação (T11 + genérico)
  const [simpleConfirm, setSimpleConfirm] = useState<SimpleConfirm | null>(null);
  const [confirmDeleteAll, setConfirmDeleteAll] = useState(false);
  const [deleteAllInput, setDeleteAllInput] = useState("");

  // Feedback tipado (T01)
  const [feedback, setFeedback] = useState<{ msg: string; type: FeedbackType } | null>(null);

  const { data: files = [], isLoading: loadingFiles } = useQuery({
    queryKey: ["files", cnpj],
    queryFn: () => cnpjApi.listFiles(cnpj),
    enabled: isOpen,
  });

  const showFeedback = (msg: string, type: FeedbackType = "success") => {
    setFeedback({ msg, type });
    setTimeout(() => setFeedback(null), 4000);
  };

  const invalidateFiles = useCallback(() => {
    void queryClient.invalidateQueries({ queryKey: ["files", cnpj] });
  }, [queryClient, cnpj]);

  // --- Mutations com onError (T01) ---
  const deleteFileMutation = useMutation({
    mutationFn: (file: ParquetFile) => cnpjApi.deleteParquetFile(cnpj, file.path),
    onSuccess: (_, file) => {
      invalidateFiles();
      showFeedback(`✅ "${file.name}" apagado.`);
    },
    onError: (error, file) => {
      showFeedback(`❌ Erro ao apagar "${file.name}": ${formatError(error)}`, "error");
    },
  });

  const deleteAllMutation = useMutation({
    mutationFn: () => cnpjApi.deleteAllParquets(cnpj),
    onSuccess: (data) => {
      invalidateFiles();
      showFeedback(`✅ ${data.count} arquivo(s) apagado(s).`);
      setConfirmDeleteAll(false);
      setDeleteAllInput("");
    },
    onError: (error) => {
      showFeedback(`❌ Erro ao apagar arquivos: ${formatError(error)}`, "error");
    },
  });

  const deleteAgregacaoMutation = useMutation({
    mutationFn: () => cnpjApi.deleteAgregacao(cnpj),
    onSuccess: (data) => {
      invalidateFiles();
      showFeedback(
        data.count > 0
          ? `✅ ${data.count} arquivo(s) de agregação apagado(s).`
          : "ℹ️ Nenhum arquivo de agregação encontrado.",
        data.count > 0 ? "success" : "warning",
      );
    },
    onError: (error) => {
      showFeedback(`❌ Erro ao apagar agregação: ${formatError(error)}`, "error");
    },
  });

  const deleteConversaoMutation = useMutation({
    mutationFn: () => cnpjApi.deleteConversao(cnpj),
    onSuccess: (data) => {
      invalidateFiles();
      showFeedback(
        data.count > 0
          ? `✅ Fator(es) de conversão apagado(s).`
          : "ℹ️ Nenhum arquivo de conversão encontrado.",
        data.count > 0 ? "success" : "warning",
      );
    },
    onError: (error) => {
      showFeedback(`❌ Erro ao apagar conversão: ${formatError(error)}`, "error");
    },
  });

  const reprocessarMutation = useMutation({
    mutationFn: () => pipelineApi.run({ cnpj }),
    onSuccess: () => {
      showFeedback("✅ Pipeline iniciado. Acompanhe na aba Logs.", "success");
    },
    onError: (error) => {
      showFeedback(`❌ Erro ao iniciar pipeline: ${formatError(error)}`, "error");
    },
  });

  // Loading states independentes por seção (T05)
  const isSectionBusy = {
    parquet: deleteAllMutation.isPending,
    agregacao: deleteAgregacaoMutation.isPending,
    conversao: deleteConversaoMutation.isPending,
  };

  // Delete por arquivo com loading state individual (T02)
  const handleDeleteFile = (file: ParquetFile) => {
    setSimpleConfirm({
      label: `Apagar "${file.name}"?`,
      onConfirm: () => {
        setDeletingFiles((prev) => new Set(prev).add(file.path));
        deleteFileMutation.mutate(file, {
          onSettled: () => {
            setDeletingFiles((prev) => {
              const next = new Set(prev);
              next.delete(file.path);
              return next;
            });
          },
        });
      },
    });
  };

  // Filtro (T06)
  const filteredFiles = useMemo(() => {
    const q = searchQuery.trim().toLowerCase();
    return q ? files.filter((f) => f.name.toLowerCase().includes(q)) : files;
  }, [files, searchQuery]);

  // Ordenação (T10)
  const sortedFiles = useMemo(() => {
    return [...filteredFiles].sort((a, b) => {
      const cmp = sortBy === "name" ? a.name.localeCompare(b.name) : a.size - b.size;
      return sortDesc ? -cmp : cmp;
    });
  }, [filteredFiles, sortBy, sortDesc]);

  // Paginação (T03)
  const totalPages = Math.ceil(sortedFiles.length / FILES_PER_PAGE);
  const paginatedFiles = useMemo(() => {
    const start = (filePage - 1) * FILES_PER_PAGE;
    return sortedFiles.slice(start, start + FILES_PER_PAGE);
  }, [sortedFiles, filePage]);

  // Resumo de espaço (T04)
  const storageSummary = useMemo(
    () => formatBytes(files.reduce((sum, f) => sum + f.size, 0)),
    [files],
  );

  // Export de lista (T13)
  const exportFileList = () => {
    const data = files.map((f) => ({
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

  const handleSearchChange = (q: string) => {
    setSearchQuery(q);
    setFilePage(1);
  };

  const toggleSort = (key: SortKey) => {
    if (sortBy === key) setSortDesc((d) => !d);
    else {
      setSortBy(key);
      setSortDesc(false);
    }
  };

  const sortArrow = (key: SortKey) =>
    sortBy !== key ? " ↕" : sortDesc ? " ↓" : " ↑";

  if (!isOpen) return null;

  const feedbackClass: Record<FeedbackType, string> = {
    success: "text-emerald-400 bg-emerald-900/30 border-emerald-700/50",
    error: "text-rose-400 bg-rose-900/30 border-rose-700/50",
    warning: "text-amber-400 bg-amber-900/30 border-amber-700/50",
  };

  return (
    // Acessibilidade: role="dialog" + aria (T08)
    <div
      role="dialog"
      aria-modal="true"
      aria-labelledby="modal-gerenciar-title"
      className="fixed inset-0 z-50 flex items-center justify-center"
      style={{ background: "rgba(0,0,0,0.7)" }}
      onClick={(e) => {
        if (e.target === e.currentTarget) onClose();
      }}
      onKeyDown={(e) => {
        if (e.key === "Escape") onClose();
      }}
    >
      <div
        className="flex flex-col rounded-lg border border-slate-600 shadow-2xl"
        style={{
          background: "#0d1f3c",
          width: 560,
          maxWidth: "95vw",
          maxHeight: "88vh",
          minHeight: 300,
        }}
      >
        {/* Header */}
        <div className="flex items-center justify-between px-4 py-3 border-b border-slate-700">
          <div>
            <div id="modal-gerenciar-title" className="text-sm font-bold text-blue-300">
              Gerenciar CNPJ
            </div>
            <div className="text-xs text-slate-400 mt-0.5">
              {cnpj}
              {razaoSocial ? ` — ${razaoSocial}` : ""}
            </div>
          </div>
          <button
            onClick={onClose}
            aria-label="Fechar modal"
            className="text-slate-400 hover:text-slate-200 text-lg leading-none"
          >
            ✕
          </button>
        </div>

        {/* Body */}
        <div className="flex-1 overflow-y-auto px-4 py-3 flex flex-col gap-4">
          {/* Feedback tipado (T01) */}
          {feedback && (
            <div
              className={`text-xs border rounded px-3 py-2 ${feedbackClass[feedback.type]}`}
            >
              {feedback.msg}
            </div>
          )}

          {/* Diálogo de confirmação simples (arquivo individual / seções) */}
          {simpleConfirm && (
            <div className="bg-rose-950/60 border border-rose-700 rounded p-3 flex flex-col gap-2">
              <div className="text-xs text-rose-200 font-semibold">Confirmar exclusão</div>
              <div className="text-xs text-rose-300">{simpleConfirm.label}</div>
              <div className="flex gap-2 mt-1">
                <button
                  onClick={() => {
                    simpleConfirm.onConfirm();
                    setSimpleConfirm(null);
                  }}
                  className="px-3 py-1 rounded text-xs bg-rose-700 hover:bg-rose-600 text-white font-medium"
                >
                  Confirmar
                </button>
                <button
                  onClick={() => setSimpleConfirm(null)}
                  className="px-3 py-1 rounded text-xs bg-slate-700 hover:bg-slate-600 text-slate-200"
                >
                  Cancelar
                </button>
              </div>
            </div>
          )}

          {/* Section: Arquivos Parquet */}
          <div className="border border-slate-700 rounded p-3">
            {/* Cabeçalho da seção */}
            <div className="flex items-center justify-between mb-2 gap-2 flex-wrap">
              <div className="flex items-center gap-2">
                <span className="text-xs font-semibold text-slate-300 uppercase tracking-wide">
                  Arquivos Parquet
                </span>
                {/* Resumo de espaço (T04) */}
                {files.length > 0 && (
                  <span className="text-[10px] text-slate-500">({storageSummary})</span>
                )}
                {/* Export (T13) */}
                {files.length > 0 && (
                  <button
                    onClick={exportFileList}
                    className="text-[10px] px-1.5 py-0.5 rounded bg-slate-700 hover:bg-slate-600 text-slate-300"
                    title="Exportar lista de arquivos como JSON"
                  >
                    📥 Exportar
                  </button>
                )}
              </div>
              {files.length > 0 && (
                <button
                  onClick={() => setConfirmDeleteAll(true)}
                  disabled={isSectionBusy.parquet}
                  className="text-[11px] px-2 py-0.5 rounded bg-rose-800/70 hover:bg-rose-700 text-rose-200 disabled:opacity-50 shrink-0"
                >
                  Apagar todos
                </button>
              )}
            </div>

            {/* Confirmação reforçada "Apagar todos" (T11) */}
            {confirmDeleteAll && (
              <div className="bg-rose-950/60 border border-rose-700 rounded p-3 mb-2 flex flex-col gap-2">
                <div className="text-xs text-rose-200 font-semibold">
                  ⚠️ Ação destrutiva irreversível
                </div>
                <div className="text-xs text-rose-300">
                  Digite{" "}
                  <code className="bg-rose-900 px-1 rounded font-mono">APAGAR</code>{" "}
                  para confirmar a exclusão de {files.length} arquivo(s):
                </div>
                <input
                  value={deleteAllInput}
                  onChange={(e) => setDeleteAllInput(e.target.value)}
                  className="w-full px-2 py-1 text-xs bg-slate-800 border border-rose-700 rounded text-slate-300 placeholder-slate-600 font-mono"
                  placeholder="APAGAR"
                  autoFocus
                />
                <div className="flex gap-2">
                  <button
                    onClick={() => {
                      if (deleteAllInput === "APAGAR") deleteAllMutation.mutate();
                    }}
                    disabled={deleteAllInput !== "APAGAR" || deleteAllMutation.isPending}
                    className="px-3 py-1 rounded text-xs bg-rose-700 hover:bg-rose-600 text-white font-medium disabled:opacity-40"
                  >
                    {deleteAllMutation.isPending ? "Apagando..." : "Confirmar exclusão"}
                  </button>
                  <button
                    onClick={() => {
                      setConfirmDeleteAll(false);
                      setDeleteAllInput("");
                    }}
                    className="px-3 py-1 rounded text-xs bg-slate-700 hover:bg-slate-600 text-slate-200"
                  >
                    Cancelar
                  </button>
                </div>
              </div>
            )}

            {/* Busca + ordenação (T06, T10) */}
            {files.length > 0 && (
              <div className="flex gap-1 mb-2">
                <input
                  type="search"
                  placeholder="Buscar arquivo..."
                  value={searchQuery}
                  onChange={(e) => handleSearchChange(e.target.value)}
                  className="flex-1 px-2 py-1 text-xs bg-slate-800 border border-slate-700 rounded text-slate-300 placeholder-slate-500"
                  aria-label="Filtrar arquivos"
                />
                <button
                  onClick={() => toggleSort("name")}
                  className={`px-2 py-0.5 text-[10px] rounded border shrink-0 ${
                    sortBy === "name"
                      ? "border-blue-600 text-blue-300 bg-blue-900/30"
                      : "border-slate-700 text-slate-400 bg-slate-800 hover:bg-slate-700"
                  }`}
                >
                  Nome{sortArrow("name")}
                </button>
                <button
                  onClick={() => toggleSort("size")}
                  className={`px-2 py-0.5 text-[10px] rounded border shrink-0 ${
                    sortBy === "size"
                      ? "border-blue-600 text-blue-300 bg-blue-900/30"
                      : "border-slate-700 text-slate-400 bg-slate-800 hover:bg-slate-700"
                  }`}
                >
                  Tam{sortArrow("size")}
                </button>
              </div>
            )}

            {/* Lista de arquivos */}
            {loadingFiles ? (
              <div className="text-xs text-slate-500">Carregando...</div>
            ) : sortedFiles.length === 0 ? (
              <div className="text-xs text-slate-500 italic">
                {searchQuery.trim()
                  ? "Nenhum arquivo encontrado para a busca."
                  : "Nenhum arquivo parquet encontrado."}
              </div>
            ) : (
              <>
                <div
                  className="flex flex-col gap-0.5 overflow-y-auto"
                  style={{ maxHeight: 220 }}
                >
                  {paginatedFiles.map((f) => (
                    <div
                      key={f.path}
                      className="flex items-center justify-between px-2 py-1 rounded hover:bg-slate-800 group"
                    >
                      <div className="flex items-center gap-1.5 flex-1 min-w-0 mr-2">
                        {/* Ícone por tipo (T09) */}
                        <span className="text-sm leading-none shrink-0">
                          {getFileIcon(f.name)}
                        </span>
                        <div className="flex-1 min-w-0">
                          <div
                            className="text-xs text-slate-300 truncate"
                            title={f.name}
                          >
                            {f.name}
                          </div>
                          <div className="text-[10px] text-slate-500">
                            {formatBytes(f.size)}
                          </div>
                        </div>
                      </div>
                      {/* Loading state por arquivo (T02) */}
                      <button
                        onClick={() => handleDeleteFile(f)}
                        disabled={deletingFiles.has(f.path) || isSectionBusy.parquet}
                        aria-label={`Apagar ${f.name}`}
                        className="text-[10px] px-1.5 py-0.5 rounded bg-rose-900/50 hover:bg-rose-700 text-rose-300 opacity-0 group-hover:opacity-100 transition-opacity disabled:opacity-30 shrink-0"
                      >
                        {deletingFiles.has(f.path) ? "…" : "🗑"}
                      </button>
                    </div>
                  ))}
                </div>

                {/* Paginação (T03) */}
                <div className="flex items-center justify-between mt-2 text-[10px] text-slate-500">
                  <span>{filteredFiles.length} arquivo(s)</span>
                  {totalPages > 1 && (
                    <div className="flex items-center gap-1">
                      <button
                        disabled={filePage <= 1}
                        onClick={() => setFilePage((p) => p - 1)}
                        aria-label="Página anterior"
                        className="px-2 py-0.5 rounded bg-slate-700 hover:bg-slate-600 disabled:opacity-30 text-slate-300"
                      >
                        ←
                      </button>
                      <span>
                        {filePage}/{totalPages}
                      </span>
                      <button
                        disabled={filePage >= totalPages}
                        onClick={() => setFilePage((p) => p + 1)}
                        aria-label="Próxima página"
                        className="px-2 py-0.5 rounded bg-slate-700 hover:bg-slate-600 disabled:opacity-30 text-slate-300"
                      >
                        →
                      </button>
                    </div>
                  )}
                </div>
              </>
            )}
          </div>

          {/* Section: Agregação */}
          <div className="border border-slate-700 rounded p-3">
            <div className="flex items-center justify-between">
              <div>
                <span className="text-xs font-semibold text-slate-300 uppercase tracking-wide">
                  Dados de Agregação
                </span>
                <div className="text-[11px] text-slate-500 mt-0.5">
                  produtos_final, produtos_agrupados, itens, descricao_produtos, etc.
                </div>
              </div>
              <button
                onClick={() =>
                  setSimpleConfirm({
                    label: "Apagar todos os arquivos de agregação de produtos? Esta ação não pode ser desfeita.",
                    onConfirm: () => deleteAgregacaoMutation.mutate(),
                  })
                }
                disabled={isSectionBusy.agregacao}
                className="text-[11px] px-2 py-1 rounded bg-rose-800/70 hover:bg-rose-700 text-rose-200 disabled:opacity-50 shrink-0"
              >
                {isSectionBusy.agregacao ? "Apagando..." : "Apagar agregação"}
              </button>
            </div>
          </div>

          {/* Section: Conversão de unidades */}
          <div className="border border-slate-700 rounded p-3">
            <div className="flex items-center justify-between">
              <div>
                <span className="text-xs font-semibold text-slate-300 uppercase tracking-wide">
                  Fatores de Conversão de Unidades
                </span>
                <div className="text-[11px] text-slate-500 mt-0.5">
                  fatores_conversao_{cnpj}.parquet
                </div>
              </div>
              <button
                onClick={() =>
                  setSimpleConfirm({
                    label: "Apagar os fatores de conversão? Ajustes manuais serão perdidos.",
                    onConfirm: () => deleteConversaoMutation.mutate(),
                  })
                }
                disabled={isSectionBusy.conversao}
                className="text-[11px] px-2 py-1 rounded bg-amber-800/70 hover:bg-amber-700 text-amber-200 disabled:opacity-50 shrink-0"
              >
                {isSectionBusy.conversao ? "Apagando..." : "Apagar conversão"}
              </button>
            </div>
          </div>

          {/* Section: Reprocessar (T15) */}
          <div className="border border-slate-700 rounded p-3">
            <div className="flex items-center justify-between">
              <div>
                <span className="text-xs font-semibold text-slate-300 uppercase tracking-wide">
                  Pipeline
                </span>
                <div className="text-[11px] text-slate-500 mt-0.5">
                  Re-executa o pipeline ETL completo para este CNPJ.
                </div>
              </div>
              <button
                onClick={() =>
                  setSimpleConfirm({
                    label: "Iniciar o pipeline ETL para este CNPJ? Acompanhe o progresso na aba Logs.",
                    onConfirm: () => reprocessarMutation.mutate(),
                  })
                }
                disabled={reprocessarMutation.isPending}
                aria-label="Reprocessar pipeline para este CNPJ"
                className="text-[11px] px-2 py-1 rounded bg-blue-800/70 hover:bg-blue-700 text-blue-200 disabled:opacity-50 shrink-0"
              >
                {reprocessarMutation.isPending ? "Iniciando..." : "▶ Reprocessar"}
              </button>
            </div>
          </div>
        </div>

        {/* Footer */}
        <div className="px-4 py-3 border-t border-slate-700 flex justify-end">
          <button
            onClick={onClose}
            className="px-4 py-1.5 rounded text-xs bg-slate-700 hover:bg-slate-600 text-slate-200"
          >
            Fechar
          </button>
        </div>
      </div>
    </div>
  );
}
