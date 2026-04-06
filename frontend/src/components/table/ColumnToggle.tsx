import { useState, useRef, useEffect } from "react";

interface ColumnToggleProps {
  allColumns: string[];
  orderedColumns?: string[];
  hiddenColumns: Set<string>;
  columnWidths?: Record<string, number>;
  onChange: (col: string, visible: boolean) => void;
  onOrderChange?: (orderedColumns: string[]) => void;
  onWidthChange?: (column: string, width: number) => void;
  onReset: () => void;
}

export function ColumnToggle({
  allColumns,
  orderedColumns,
  hiddenColumns,
  columnWidths,
  onChange,
  onOrderChange,
  onWidthChange,
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
  const colunasOrdenadas =
    orderedColumns?.length
      ? [
          ...orderedColumns.filter((coluna) => allColumns.includes(coluna)),
          ...allColumns.filter((coluna) => !orderedColumns.includes(coluna)),
        ]
      : allColumns;

  function moverColuna(nomeColuna: string, deslocamento: number) {
    if (!onOrderChange) return;

    const indiceAtual = colunasOrdenadas.indexOf(nomeColuna);
    if (indiceAtual < 0) return;

    const novoIndice = indiceAtual + deslocamento;
    if (novoIndice < 0 || novoIndice >= colunasOrdenadas.length) return;

    const proximaOrdem = [...colunasOrdenadas];
    const [colunaMovida] = proximaOrdem.splice(indiceAtual, 1);
    proximaOrdem.splice(novoIndice, 0, colunaMovida);
    onOrderChange(proximaOrdem);
  }

  function obterLarguraAtual(nomeColuna: string): number {
    const largura = columnWidths?.[nomeColuna];
    if (!largura || Number.isNaN(largura)) return 120;
    return Math.max(80, largura);
  }

  function ajustarLargura(nomeColuna: string, largura: number) {
    if (!onWidthChange) return;
    onWidthChange(nomeColuna, Math.max(80, Math.min(600, largura)));
  }

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
            {colunasOrdenadas.map((col, indice) => (
              <div
                key={col}
                className="flex items-center gap-2 px-1 py-1 rounded text-xs text-slate-300 hover:text-white hover:bg-slate-700"
              >
                <input
                  type="checkbox"
                  checked={!hiddenColumns.has(col)}
                  onChange={(e) => onChange(col, e.target.checked)}
                  className="accent-blue-500 cursor-pointer"
                />
                <span className="truncate flex-1 cursor-pointer" title={col}>
                  {col}
                </span>
                {onWidthChange && (
                  <div className="flex items-center gap-1 shrink-0">
                    <button
                      type="button"
                      onClick={() => ajustarLargura(col, obterLarguraAtual(col) - 20)}
                      className="px-1 rounded bg-slate-800 hover:bg-slate-600 text-slate-300"
                      title={`Diminuir largura de ${col}`}
                    >
                      -
                    </button>
                    <input
                      type="number"
                      min={80}
                      max={600}
                      value={obterLarguraAtual(col)}
                      onChange={(e) =>
                        ajustarLargura(col, Number(e.target.value) || 120)
                      }
                      className="w-16 bg-slate-900 border border-slate-700 rounded px-1 py-0.5 text-right text-slate-200"
                      title={`Largura em pixels da coluna ${col}`}
                    />
                    <button
                      type="button"
                      onClick={() => ajustarLargura(col, obterLarguraAtual(col) + 20)}
                      className="px-1 rounded bg-slate-800 hover:bg-slate-600 text-slate-300"
                      title={`Aumentar largura de ${col}`}
                    >
                      +
                    </button>
                  </div>
                )}
                {onOrderChange && (
                  <div className="flex items-center gap-1 shrink-0">
                    <button
                      type="button"
                      onClick={() => moverColuna(col, -1)}
                      disabled={indice === 0}
                      className="px-1 rounded bg-slate-800 hover:bg-slate-600 text-slate-300 disabled:opacity-40 disabled:cursor-not-allowed"
                      title={`Mover ${col} para a esquerda`}
                    >
                      ↑
                    </button>
                    <button
                      type="button"
                      onClick={() => moverColuna(col, 1)}
                      disabled={indice === colunasOrdenadas.length - 1}
                      className="px-1 rounded bg-slate-800 hover:bg-slate-600 text-slate-300 disabled:opacity-40 disabled:cursor-not-allowed"
                      title={`Mover ${col} para a direita`}
                    >
                      ↓
                    </button>
                  </div>
                )}
              </div>
            ))}
          </div>
        </div>
      )}
    </div>
  );
}
