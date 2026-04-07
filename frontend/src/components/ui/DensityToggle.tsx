import type { TableDensity } from "../table/DataTable";

interface DensityToggleProps {
  value: TableDensity;
  onChange: (d: TableDensity) => void;
}

const OPTIONS: { value: TableDensity; label: string }[] = [
  { value: "compact", label: "Compacto" },
  { value: "normal", label: "Normal" },
  { value: "comfortable", label: "Espaçado" },
];

export function DensityToggle({ value, onChange }: DensityToggleProps) {
  return (
    <div className="flex items-center gap-1 border border-slate-700 rounded overflow-hidden">
      {OPTIONS.map((opt) => (
        <button
          key={opt.value}
          onClick={() => onChange(opt.value)}
          className={`px-2 py-1 text-xs transition-colors ${
            value === opt.value
              ? "bg-blue-700 text-white"
              : "bg-slate-800 text-slate-400 hover:bg-slate-700 hover:text-slate-200"
          }`}
          title={`Densidade: ${opt.label}`}
        >
          {opt.label}
        </button>
      ))}
    </div>
  );
}
