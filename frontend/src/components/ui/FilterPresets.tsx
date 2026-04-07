import { useMemo } from "react";
import type { FilterOperator } from "../../api/types";

interface FilterPreset {
  label: string;
  title: string;
  column: string;
  operator: FilterOperator;
  value: string;
}

interface FilterPresetsProps {
  columns: string[];
  /** Called when user picks a preset */
  onApply: (filter: { column: string; operator: FilterOperator; value: string }) => void;
}

const NULL_PATTERN_COLS = [
  "cnpj",
  "cpf",
  "ie",
  "codigo",
  "chave",
  "gtin",
  "cest",
  "ncm",
  "cfop",
];

const PRECO_PATTERN = /preco|valor|vlr|aliq|base_calc|vl_/i;
const DIVERGENTE_PATTERN = /divergen|diferen|inconsist/i;

function buildSmartPresets(columns: string[]): FilterPreset[] {
  const presets: FilterPreset[] = [];

  // Smart presets for null-critical fiscal columns
  for (const pattern of NULL_PATTERN_COLS) {
    const col = columns.find(
      (c) => c.toLowerCase() === pattern || c.toLowerCase().startsWith(pattern + "_"),
    );
    if (col) {
      presets.push({
        label: `Sem ${col}`,
        title: `Mostrar apenas linhas onde ${col} é nulo`,
        column: col,
        operator: "e_nulo",
        value: "",
      });
    }
  }

  // Null presets for value/price columns
  const priceCols = columns.filter((c) => PRECO_PATTERN.test(c));
  for (const col of priceCols.slice(0, 3)) {
    presets.push({
      label: `${col} nulo`,
      title: `Mostrar apenas linhas onde ${col} é nulo`,
      column: col,
      operator: "e_nulo",
      value: "",
    });
  }

  // Divergent/inconsistency columns
  const divCols = columns.filter((c) => DIVERGENTE_PATTERN.test(c));
  for (const col of divCols.slice(0, 2)) {
    presets.push({
      label: `Com divergência (${col})`,
      title: `Mostrar apenas linhas onde ${col} não é nulo`,
      column: col,
      operator: "nao_e_nulo",
      value: "",
    });
  }

  return presets;
}

const BTN_CLS =
  "inline-flex items-center gap-1 px-2 py-1 rounded text-[11px] font-medium cursor-pointer transition-colors bg-slate-700 hover:bg-slate-600 text-slate-200 border border-slate-600 hover:border-slate-500 whitespace-nowrap";

export function FilterPresets({ columns, onApply }: FilterPresetsProps) {
  const smartPresets = useMemo(() => buildSmartPresets(columns), [columns]);

  if (smartPresets.length === 0) return null;

  return (
    <div className="flex items-center gap-2 flex-wrap">
      <span className="text-[10px] text-slate-500 uppercase tracking-wide shrink-0">
        Presets:
      </span>
      {smartPresets.map((preset) => (
        <button
          key={`${preset.column}-${preset.operator}`}
          className={BTN_CLS}
          title={preset.title}
          onClick={() =>
            onApply({
              column: preset.column,
              operator: preset.operator,
              value: preset.value,
            })
          }
        >
          {preset.label}
        </button>
      ))}
    </div>
  );
}
