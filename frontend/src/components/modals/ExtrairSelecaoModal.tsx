import { useMemo, useState } from "react";
import type { SqlFile } from "../../api/types";

const LS_KEY = "fiscalParquet.selectedConsultas";

interface Props {
  onClose: () => void;
  onConfirm: (consultas: string[] | null) => void;
  sqlFiles: SqlFile[];
}

/** Agrupa arquivos SQL por pasta (prefixo antes de '/').
 *  Arquivos na raiz ficam no grupo "". */
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

/** Le a selecao salva do localStorage e filtra apenas paths ainda existentes. */
function loadFromStorage(
  allPaths: string[],
  validPaths: Set<string>,
): Set<string> {
  try {
    const raw = localStorage.getItem(LS_KEY);
    if (!raw) return new Set(allPaths);
    const parsed: unknown = JSON.parse(raw);
    if (!Array.isArray(parsed)) return new Set(allPaths);
    const filtered = (parsed as unknown[]).filter(
      (p) => typeof p === "string" && validPaths.has(p as string),
    ) as string[];
    return filtered.length > 0 ? new Set(filtered) : new Set(allPaths);
  } catch {
    return new Set(allPaths);
  }
}

/** Montado condicionalmente pelo pai - sempre visivel quando montado. */
export function ExtrairSelecaoModal({ onClose, onConfirm, sqlFiles }: Props) {
  const allPaths = useMemo(() => sqlFiles.map((f) => f.path), [sqlFiles]);
  const allPathSet = useMemo(() => new Set(allPaths), [allPaths]);
  const groups = useMemo(() => groupByFolder(sqlFiles), [sqlFiles]);

  // Inicializar a partir do localStorage (roda apenas uma vez na montagem)
  const [selected, setSelected] = useState<Set<string>>(() =>
    loadFromStorage(allPaths, allPathSet),
  );

  const allSelected = selected.size === allPaths.length;

  const toggleAll = () => {
    if (allSelected) {
      setSelected(new Set());
    } else {
      setSelected(new Set(allPaths));
    }
  };

  const toggleFolder = (folder: string) => {
    const folderPaths = (groups.get(folder) ?? []).map((f) => f.path);
    const allFolderSelected = folderPaths.every((p) => selected.has(p));
    const next = new Set(selected);
    if (allFolderSelected) {
      folderPaths.forEach((p) => next.delete(p));
    } else {
      folderPaths.forEach((p) => next.add(p));
    }
    setSelected(next);
  };

  const toggleFile = (path: string) => {
    const next = new Set(selected);
    if (next.has(path)) {
      next.delete(path);
    } else {
      next.add(path);
    }
    setSelected(next);
  };

  const handleConfirm = () => {
    const selectedArr = [...selected];
    const isAll = selected.size === allPaths.length;
    if (isAll) {
      localStorage.removeItem(LS_KEY);
    } else {
      localStorage.setItem(LS_KEY, JSON.stringify(selectedArr));
    }
    onConfirm(isAll ? null : selectedArr);
  };

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-slate-950/80 backdrop-blur-sm">
      <div
        className="flex flex-col rounded-lg border border-slate-600 bg-slate-900 shadow-2xl"
        style={{ width: 560, maxHeight: "80vh" }}
      >
        {/* Header */}
        <div className="flex items-center justify-between border-b border-slate-700 px-5 py-3">
          <h2 className="text-sm font-semibold text-slate-100">
            Selecionar Consultas SQL para Extracao
          </h2>
          <button
            onClick={onClose}
            className="text-slate-400 hover:text-slate-200 text-lg leading-none"
          >
            x
          </button>
        </div>

        {/* Toolbar */}
        <div className="flex items-center gap-2 border-b border-slate-700 px-4 py-2">
          <button
            onClick={toggleAll}
            className="rounded bg-blue-700 px-3 py-1 text-xs font-medium text-white hover:bg-blue-600"
          >
            {allSelected ? "Desmarcar todas" : "Todas"}
          </button>
          <button
            onClick={() => setSelected(new Set())}
            className="rounded bg-slate-700 px-3 py-1 text-xs font-medium text-slate-300 hover:bg-slate-600"
          >
            Nenhuma
          </button>
          <span className="ml-auto text-xs text-slate-400">
            {selected.size} de {allPaths.length} selecionadas
          </span>
        </div>

        {/* File list */}
        <div className="flex-1 overflow-y-auto px-2 py-2">
          {[...groups.entries()].map(([folder, files]) => {
            const folderPaths = files.map((f) => f.path);
            const allFolderSelected = folderPaths.every((p) => selected.has(p));
            const someFolderSelected =
              !allFolderSelected && folderPaths.some((p) => selected.has(p));

            return (
              <div key={folder || "__root__"} className="mb-2">
                {/* Group header */}
                <label className="flex cursor-pointer items-center gap-2 rounded px-2 py-1 hover:bg-slate-800">
                  <input
                    type="checkbox"
                    className="accent-blue-500"
                    checked={allFolderSelected}
                    ref={(el) => {
                      if (el) el.indeterminate = someFolderSelected;
                    }}
                    onChange={() => toggleFolder(folder)}
                  />
                  <span className="text-xs font-semibold text-blue-300">
                    {folder || "raiz"}
                  </span>
                  <span className="ml-auto text-[10px] text-slate-500">
                    {folderPaths.filter((p) => selected.has(p)).length}/
                    {folderPaths.length}
                  </span>
                </label>

                {/* Group files */}
                <div className="ml-4 flex flex-col gap-0.5">
                  {files.map((f) => (
                    <label
                      key={f.path}
                      className="flex cursor-pointer items-center gap-2 rounded px-2 py-0.5 hover:bg-slate-800"
                    >
                      <input
                        type="checkbox"
                        className="accent-blue-500 shrink-0"
                        checked={selected.has(f.path)}
                        onChange={() => toggleFile(f.path)}
                      />
                      <span
                        className="truncate text-[11px] text-slate-300"
                        title={f.path}
                      >
                        {f.name}
                      </span>
                    </label>
                  ))}
                </div>
              </div>
            );
          })}
        </div>

        {/* Footer */}
        <div className="flex items-center justify-end gap-2 border-t border-slate-700 px-5 py-3">
          <button
            onClick={onClose}
            className="rounded bg-slate-700 px-4 py-1.5 text-xs font-medium text-slate-300 hover:bg-slate-600"
          >
            Cancelar
          </button>
          <button
            onClick={handleConfirm}
            disabled={selected.size === 0}
            className="rounded bg-blue-700 px-4 py-1.5 text-xs font-medium text-white hover:bg-blue-600 disabled:opacity-40 disabled:cursor-not-allowed"
          >
            Executar Extracao ({selected.size})
          </button>
        </div>
      </div>
    </div>
  );
}