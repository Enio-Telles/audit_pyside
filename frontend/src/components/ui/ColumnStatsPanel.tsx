import { useEffect, useMemo } from "react";
import {
  Bar,
  BarChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
} from "recharts";
import type { ParquetMetadata } from "../../api/types";

interface ColumnStatsPanelProps {
  col: string | null;
  metadata: ParquetMetadata | null;
  onClose: () => void;
}

function StatRow({ label, value }: { label: string; value: string | number | null }) {
  return (
    <div className="flex flex-col gap-0.5">
      <dt className="text-[10px] font-medium text-slate-400 uppercase tracking-wide truncate">
        {label}
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
          <span className="text-slate-500 italic">—</span>
        ) : (
          String(value)
        )}
      </dd>
    </div>
  );
}

function formatNum(v: number | null): string {
  if (v === null || v === undefined) return "—";
  if (Number.isNaN(v) || !Number.isFinite(v)) return "—";
  const abs = Math.abs(v);
  if (abs >= 1_000_000) return v.toLocaleString("pt-BR", { maximumFractionDigits: 2 });
  if (abs >= 1) return v.toLocaleString("pt-BR", { maximumFractionDigits: 4 });
  return v.toLocaleString("pt-BR", { maximumFractionDigits: 6 });
}

export function ColumnStatsPanel({ col, metadata, onClose }: ColumnStatsPanelProps) {
  useEffect(() => {
    if (!col) return;
    const handler = (e: KeyboardEvent) => {
      if (e.key === "Escape") onClose();
    };
    window.addEventListener("keydown", handler);
    return () => window.removeEventListener("keydown", handler);
  }, [col, onClose]);

  const topValues = useMemo(() => {
    if (!col || !metadata?.sample) return [];
    const counts: Record<string, number> = {};
    for (const row of metadata.sample) {
      const v = row[col];
      const key = v === null || v === undefined ? "(nulo)" : String(v);
      counts[key] = (counts[key] ?? 0) + 1;
    }
    return Object.entries(counts)
      .sort((a, b) => b[1] - a[1])
      .slice(0, 8);
  }, [col, metadata]);

  if (!col) return null;

  const dtype = metadata?.dtypes?.[col] ?? "—";
  const numStats = metadata?.numeric_stats?.[col];

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
        aria-label={`Estatísticas da coluna ${col}`}
      >
        {/* Header */}
        <div
          className="flex items-center justify-between px-4 py-3 border-b border-slate-700"
          style={{ background: "#162035" }}
        >
          <div className="flex flex-col min-w-0">
            <span className="text-sm font-semibold text-slate-200 truncate">{col}</span>
            <span className="text-[10px] text-slate-500 font-mono">{dtype}</span>
          </div>
          <button
            onClick={onClose}
            className="text-slate-400 hover:text-white text-lg leading-none px-1 ml-2 shrink-0"
            aria-label="Fechar"
            title="Fechar (Esc)"
          >
            ✕
          </button>
        </div>

        {/* Content */}
        <div className="flex-1 overflow-y-auto px-4 py-3">
          {!metadata ? (
            <p className="text-xs text-slate-500">Carregando metadados…</p>
          ) : (
            <dl className="space-y-2">
              {/* Numeric stats */}
              {numStats && (
                <>
                  <p className="text-[10px] font-semibold text-blue-400 uppercase tracking-wider mt-1 mb-1">
                    Estatísticas numéricas
                  </p>
                  <StatRow label="Mínimo" value={formatNum(numStats.min)} />
                  <StatRow label="Máximo" value={formatNum(numStats.max)} />
                  <StatRow label="Média" value={formatNum(numStats.mean)} />
                  <StatRow
                    label="Nulos"
                    value={numStats.null_count !== null ? String(numStats.null_count) : "—"}
                  />
                  {/* Mini distribution bar chart */}
                  {numStats.min !== null && numStats.max !== null && numStats.mean !== null && (
                    <div className="mt-2">
                      <p className="text-[10px] font-semibold text-blue-400 uppercase tracking-wider mb-1">
                        Distribuição (mín / méd / máx)
                      </p>
                      <ResponsiveContainer width="100%" height={64}>
                        <BarChart
                          data={[
                            { name: "mín", v: numStats.min },
                            { name: "méd", v: numStats.mean },
                            { name: "máx", v: numStats.max },
                          ]}
                          margin={{ top: 4, right: 4, left: 4, bottom: 0 }}
                        >
                          <XAxis
                            dataKey="name"
                            tick={{ fontSize: 9, fill: "#94a3b8" }}
                            axisLine={false}
                            tickLine={false}
                          />
                          <Tooltip
                            contentStyle={{
                              background: "#0d1f3c",
                              border: "1px solid #2d4a7a",
                              fontSize: 11,
                              color: "#e2e8f0",
                            }}
                            formatter={(v) => [formatNum(v as number), col] as [string, string]}
                            cursor={{ fill: "rgba(59,130,246,0.1)" }}
                          />
                          <Bar dataKey="v" fill="#3b82f6" radius={[2, 2, 0, 0]} maxBarSize={40} />
                        </BarChart>
                      </ResponsiveContainer>
                    </div>
                  )}
                </>
              )}

              {/* Top values from sample */}
              {topValues.length > 0 && (
                <>
                  <p className="text-[10px] font-semibold text-blue-400 uppercase tracking-wider mt-3 mb-1">
                    Valores na amostra
                  </p>
                  {topValues.map(([val, count]) => (
                    <div
                      key={val}
                      className="flex items-center justify-between text-xs py-1"
                      style={{
                        background: "#0a1628",
                        borderRadius: 4,
                        padding: "4px 8px",
                        border: "1px solid #1e2d4a",
                        marginBottom: 2,
                      }}
                    >
                      <span
                        className="text-slate-100 truncate mr-2"
                        style={{ maxWidth: "72%" }}
                        title={val}
                      >
                        {val}
                      </span>
                      <span className="text-slate-400 shrink-0">{count}×</span>
                    </div>
                  ))}
                </>
              )}

              {/* No stats available */}
              {!numStats && topValues.length === 0 && (
                <p className="text-xs text-slate-500">
                  Sem estatísticas disponíveis para esta coluna.
                </p>
              )}
            </dl>
          )}
        </div>
      </div>
    </>
  );
}
