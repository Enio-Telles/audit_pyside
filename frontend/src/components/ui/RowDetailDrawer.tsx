import { useEffect } from "react";
import { formatCell } from "../table/formatCell";

interface RowDetailDrawerProps {
  row: Record<string, unknown> | null;
  onClose: () => void;
}

export function RowDetailDrawer({ row, onClose }: RowDetailDrawerProps) {
  useEffect(() => {
    if (!row) return;
    const handler = (e: KeyboardEvent) => {
      if (e.key === "Escape") onClose();
    };
    window.addEventListener("keydown", handler);
    return () => window.removeEventListener("keydown", handler);
  }, [row, onClose]);

  if (!row) return null;

  return (
    <>
      {/* Backdrop */}
      <div
        className="fixed inset-0 z-40 bg-black/40"
        onClick={onClose}
        aria-hidden="true"
      />
      {/* Drawer */}
      <div
        className="fixed top-0 right-0 h-full z-50 flex flex-col shadow-2xl"
        style={{ width: 360, background: "#0d1f3c", borderLeft: "1px solid #2d4a7a" }}
        role="dialog"
        aria-label="Detalhes da linha"
      >
        {/* Header */}
        <div
          className="flex items-center justify-between px-4 py-3 border-b border-slate-700"
          style={{ background: "#162035" }}
        >
          <span className="text-sm font-semibold text-slate-200">Detalhes da linha</span>
          <button
            onClick={onClose}
            className="text-slate-400 hover:text-white text-lg leading-none px-1"
            aria-label="Fechar"
            title="Fechar (Esc)"
          >
            ✕
          </button>
        </div>

        {/* Fields list */}
        <div className="flex-1 overflow-y-auto px-4 py-3">
          <dl className="space-y-2">
            {Object.entries(row).map(([key, value]) => (
              <div key={key} className="flex flex-col gap-0.5">
                <dt className="text-[10px] font-medium text-slate-400 uppercase tracking-wide truncate">
                  {key}
                </dt>
                <dd
                  className="text-xs text-slate-100 break-all"
                  style={{
                    background: "#0a1628",
                    borderRadius: 4,
                    padding: "4px 8px",
                    border: "1px solid #1e2d4a",
                  }}
                >
                  {value === null || value === undefined ? (
                    <span className="text-slate-500 italic">nulo</span>
                  ) : (
                    formatCell(value)
                  )}
                </dd>
              </div>
            ))}
          </dl>
        </div>
      </div>
    </>
  );
}
