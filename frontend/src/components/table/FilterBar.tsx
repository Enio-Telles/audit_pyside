import { useState } from "react";
import type { FilterItem } from "../../api/types";
import { FILTER_OPERATORS } from "../../api/types";

interface FilterBarProps {
  columns: string[];
  filters: FilterItem[];
  onAdd: (f: FilterItem) => void;
  onRemove: (idx: number) => void;
  onClear: () => void;
}

export function FilterBar({
  columns,
  filters,
  onAdd,
  onRemove,
  onClear,
}: FilterBarProps) {
  const [col, setCol] = useState(columns[0] ?? "");
  const [op, setOp] = useState<string>("contem");
  const [val, setVal] = useState("");

  const inputCls =
    "bg-slate-800 border border-slate-600 rounded px-2 py-1 text-sm text-slate-200 focus:outline-none focus:border-blue-500";
  const btnCls = "px-3 py-1 rounded text-xs font-medium";

  const handleAdd = () => {
    if (!col) return;
    onAdd({ column: col, operator: op, value: val });
    setVal("");
  };

  return (
    <div
      className="border border-slate-700 rounded p-2 mb-2"
      style={{ background: "#0f1b33" }}
    >
      <div className="text-xs text-slate-400 mb-2 font-semibold">Filtros</div>
      <div className="flex flex-wrap gap-2 items-end mb-2">
        <div className="flex flex-col gap-1">
          <label
            htmlFor="coluna"
            className="text-xs text-slate-400 cursor-pointer"
          >
            Coluna
          </label>
          <select
            id="coluna"
            value={col}
            onChange={(e) => setCol(e.target.value)}
            className={inputCls}
          >
            {columns.map((c) => (
              <option key={c} value={c}>
                {c}
              </option>
            ))}
          </select>
        </div>
        <div className="flex flex-col gap-1">
          <label
            htmlFor="operador"
            className="text-xs text-slate-400 cursor-pointer"
          >
            Operador
          </label>
          <select
            id="operador"
            value={op}
            onChange={(e) => setOp(e.target.value)}
            className={inputCls}
          >
            {FILTER_OPERATORS.map((o) => (
              <option key={o} value={o}>
                {o}
              </option>
            ))}
          </select>
        </div>
        <div className="flex flex-col gap-1 flex-1 min-w-40">
          <label
            htmlFor="valor"
            className="text-xs text-slate-400 cursor-pointer"
          >
            Valor
          </label>
          <input
            id="valor"
            className={inputCls + " w-full"}
            placeholder="Valor do filtro"
            value={val}
            onChange={(e) => setVal(e.target.value)}
            onKeyDown={(e) => e.key === "Enter" && handleAdd()}
          />
        </div>
        <button
          onClick={handleAdd}
          className={btnCls + " bg-blue-600 hover:bg-blue-500 text-white"}
        >
          Adicionar filtro
        </button>
        <button
          onClick={onClear}
          className={btnCls + " bg-slate-700 hover:bg-slate-600 text-slate-300"}
        >
          Limpar filtros
        </button>
      </div>

      {filters.length > 0 && (
        <div className="flex flex-wrap gap-2">
          {filters.map((f, i) => (
            <span
              key={i}
              className="flex items-center gap-1 bg-blue-900 border border-blue-700 text-blue-200 text-xs px-2 py-0.5 rounded-full"
            >
              {f.column} {f.operator} "{f.value}"
              <button
                onClick={() => onRemove(i)}
                className="ml-1 hover:text-red-400 focus-visible:outline-none focus-visible:ring-1 focus-visible:ring-blue-400 rounded px-1"
                aria-label="Remover filtro"
                title="Remover filtro"
              >
                ×
              </button>
            </span>
          ))}
        </div>
      )}
    </div>
  );
}
