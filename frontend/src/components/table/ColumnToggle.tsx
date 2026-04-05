import { useState, useRef, useEffect } from "react";

interface ColumnToggleProps {
  allColumns: string[];
  hiddenColumns: Set<string>;
  onChange: (col: string, visible: boolean) => void;
  onReset: () => void;
}

export function ColumnToggle({
  allColumns,
  hiddenColumns,
  onChange,
  onReset,
}: ColumnToggleProps) {
  const [open, setOpen] = useState(false);
  const ref = useRef<HTMLDivElement>(null);

  useEffect(() => {
    if (!open) return;
    const handler = (e: MouseEvent) => {
      if (ref.current && !ref.current.contains(e.target as Node)) {
        setOpen(false);
      }
    };
    document.addEventListener("mousedown", handler);
    return () => document.removeEventListener("mousedown", handler);
  }, [open]);

  const visibleCount = allColumns.length - hiddenColumns.size;

  return (
    <div ref={ref} className="relative">
      <button
        onClick={() => setOpen((o) => !o)}
        className="px-3 py-1.5 rounded text-xs font-medium cursor-pointer bg-slate-700 hover:bg-slate-600 text-slate-200 transition-colors flex items-center gap-1"
        title="Mostrar/ocultar colunas"
      >
        <span>Colunas</span>
        <span className="text-slate-400">
          ({visibleCount}/{allColumns.length})
        </span>
        <span className="text-slate-400">{open ? "▲" : "▾"}</span>
      </button>
      {open && (
        <div
          className="absolute z-50 rounded border border-slate-600 shadow-xl mt-1"
          style={{
            background: "#0f1b33",
            minWidth: 220,
            maxHeight: 300,
            overflowY: "auto",
          }}
        >
          <div className="flex gap-1 p-2 border-b border-slate-700 sticky top-0 bg-[#0f1b33]">
            <button
              onClick={() => allColumns.forEach((c) => onChange(c, true))}
              className="px-2 py-1 rounded text-xs bg-slate-700 hover:bg-slate-600 text-slate-200"
            >
              Todas
            </button>
            <button
              onClick={() => allColumns.forEach((c) => onChange(c, false))}
              className="px-2 py-1 rounded text-xs bg-slate-700 hover:bg-slate-600 text-slate-200"
            >
              Nenhuma
            </button>
            <button
              onClick={() => {
                onReset();
                setOpen(false);
              }}
              className="px-2 py-1 rounded text-xs bg-slate-700 hover:bg-slate-600 text-slate-400"
            >
              Padrão
            </button>
          </div>
          <div className="p-2 flex flex-col gap-0.5">
            {allColumns.map((col) => (
              <label
                key={col}
                className="flex items-center gap-2 px-1 py-0.5 rounded text-xs text-slate-300 hover:text-white hover:bg-slate-700 cursor-pointer"
              >
                <input
                  type="checkbox"
                  checked={!hiddenColumns.has(col)}
                  onChange={(e) => onChange(col, e.target.checked)}
                  className="accent-blue-500"
                />
                <span className="truncate" title={col}>
                  {col}
                </span>
              </label>
            ))}
          </div>
        </div>
      )}
    </div>
  );
}
