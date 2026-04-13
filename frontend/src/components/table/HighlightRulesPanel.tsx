import { useState } from "react";
import type { HighlightRule, HighlightRuleOperator } from "../../api/types";

const PRESET_COLORS: { label: string; value: string }[] = [
  { label: "Verde", value: "rgba(30,120,50,0.55)" },
  { label: "Vermelho", value: "rgba(150,30,30,0.55)" },
  { label: "Azul", value: "rgba(30,80,200,0.45)" },
  { label: "Amarelo", value: "rgba(180,140,0,0.45)" },
  { label: "Laranja", value: "rgba(200,90,0,0.5)" },
  { label: "Roxo", value: "rgba(100,30,160,0.5)" },
  { label: "Ciano", value: "rgba(0,140,160,0.5)" },
  { label: "Rosa", value: "rgba(180,30,100,0.5)" },
];

const RULE_OPERATORS: HighlightRuleOperator[] = [
  "igual",
  "contem",
  "maior",
  "menor",
  "e_nulo",
  "nao_e_nulo",
];

interface HighlightRulesPanelProps {
  columns: string[];
  rules: HighlightRule[];
  onAdd: (rule: HighlightRule) => void;
  onRemove: (index: number) => void;
}

export function HighlightRulesPanel({
  columns,
  rules,
  onAdd,
  onRemove,
}: HighlightRulesPanelProps) {
  const [open, setOpen] = useState(false);
  const [type, setType] = useState<"row" | "column">("row");
  const [col, setCol] = useState(columns[0] ?? "");
  const [op, setOp] = useState<HighlightRuleOperator>("igual");
  const [val, setVal] = useState("");
  const [color, setColor] = useState(PRESET_COLORS[0].value);
  const [label, setLabel] = useState("");

  const inputCls =
    "bg-slate-800 border border-slate-600 rounded px-2 py-1 text-xs text-slate-200 focus:outline-none focus:border-blue-500";

  const needsValue = op !== "e_nulo" && op !== "nao_e_nulo";

  const handleAdd = () => {
    if (!col) return;
    onAdd({
      type,
      column: col,
      operator: op,
      value: val,
      color,
      label: label || undefined,
    });
    setVal("");
    setLabel("");
  };

  return (
    <div
      className="border border-slate-700 rounded mb-1"
      style={{ background: "#0f1b33" }}
    >
      <button
        onClick={() => setOpen((o) => !o)}
        className="w-full flex items-center gap-2 px-3 py-1.5 text-xs text-slate-400 hover:text-slate-200 transition-colors"
      >
        <span className="font-semibold">Destaques</span>
        {rules.length > 0 && (
          <span className="px-1.5 py-0.5 rounded-full bg-blue-800 text-blue-200 text-[10px] font-medium">
            {rules.length}
          </span>
        )}
        <span className="ml-auto">{open ? "▲" : "▾"}</span>
      </button>

      {open && (
        <div className="px-3 pb-3 border-t border-slate-700">
          {/* Form */}
          <div className="flex flex-wrap gap-2 mt-2 items-end">
            <div className="flex flex-col gap-1">
              <label className="text-[10px] text-slate-400">Tipo</label>
              <select
                value={type}
                onChange={(e) => setType(e.target.value as "row" | "column")}
                className={inputCls}
              >
                <option value="row">Linha</option>
                <option value="column">Coluna</option>
              </select>
            </div>
            <div className="flex flex-col gap-1">
              <label className="text-[10px] text-slate-400">Coluna</label>
              <select
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
              <label className="text-[10px] text-slate-400">Condição</label>
              <select
                value={op}
                onChange={(e) => setOp(e.target.value as HighlightRuleOperator)}
                className={inputCls}
              >
                {RULE_OPERATORS.map((o) => (
                  <option key={o} value={o}>
                    {o}
                  </option>
                ))}
              </select>
            </div>
            {needsValue && (
              <div className="flex flex-col gap-1">
                <label className="text-[10px] text-slate-400">Valor</label>
                <input
                  className={inputCls + " w-32"}
                  placeholder="Valor"
                  value={val}
                  onChange={(e) => setVal(e.target.value)}
                  onKeyDown={(e) => e.key === "Enter" && handleAdd()}
                />
              </div>
            )}
            <div className="flex flex-col gap-1">
              <label className="text-[10px] text-slate-400">Rótulo (opc.)</label>
              <input
                className={inputCls + " w-24"}
                placeholder="Ex: Erro"
                value={label}
                onChange={(e) => setLabel(e.target.value)}
              />
            </div>
            <div className="flex flex-col gap-1">
              <label className="text-[10px] text-slate-400">Cor</label>
              <div className="flex gap-1">
                {PRESET_COLORS.map((c) => (
                  <button
                    key={c.value}
                    title={c.label}
                    onClick={() => setColor(c.value)}
                    className="w-5 h-5 rounded-sm border-2 transition-all cursor-pointer"
                    style={{
                      background: c.value,
                      borderColor:
                        color === c.value ? "#fff" : "transparent",
                    }}
                  />
                ))}
              </div>
            </div>
            <button
              onClick={handleAdd}
              className="px-3 py-1 rounded text-xs font-medium bg-blue-600 hover:bg-blue-500 text-white cursor-pointer"
            >
              + Adicionar
            </button>
          </div>

          {/* Active rules */}
          {rules.length > 0 && (
            <div className="mt-3 flex flex-col gap-1">
              <div className="text-[10px] text-slate-400 font-semibold mb-1">
                Regras ativas
              </div>
              {rules.map((rule, i) => (
                <div
                  key={i}
                  className="flex items-center gap-2 px-2 py-1 rounded"
                  style={{ background: "#162035" }}
                >
                  <span
                    className="w-3 h-3 rounded-sm shrink-0"
                    style={{ background: rule.color }}
                  />
                  <span className="text-xs text-slate-300 truncate flex-1">
                    <span className="text-slate-500">
                      {rule.type === "row" ? "Linha" : "Col"}
                    </span>
                    {" · "}
                    <span className="text-blue-300">{rule.column}</span>
                    {" · "}
                    {rule.operator}
                    {rule.value ? ` "${rule.value}"` : ""}
                    {rule.label ? ` — ${rule.label}` : ""}
                  </span>
                  <button
                    onClick={() => onRemove(i)}
                    className="ml-auto text-red-400 hover:text-red-300 text-xs shrink-0 cursor-pointer"
                    title="Remover"
                  >
                    ✕
                  </button>
                </div>
              ))}
            </div>
          )}
        </div>
      )}
    </div>
  );
}
