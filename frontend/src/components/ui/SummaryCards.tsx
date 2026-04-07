interface SummaryCardsProps {
  totalRows: number;
  currentPageRows: number;
  loading?: boolean;
}

export function SummaryCards({ totalRows, currentPageRows, loading }: SummaryCardsProps) {
  const fmt = (n: number) => n.toLocaleString("pt-BR");

  const cards = [
    { label: "Total", value: fmt(totalRows) },
    { label: "Página atual", value: fmt(currentPageRows) },
  ];

  return (
    <div className="flex gap-2 flex-wrap">
      {cards.map(({ label, value }) => (
        <div
          key={label}
          className="flex flex-col px-3 py-1.5 rounded border border-slate-700"
          style={{ background: "#0d1f3c", minWidth: 90 }}
        >
          <span className="text-[10px] text-slate-400 uppercase tracking-wide leading-none mb-0.5">
            {label}
          </span>
          <span
            className={`text-sm font-semibold text-white tabular-nums ${loading ? "opacity-50" : ""}`}
          >
            {loading ? "…" : value}
          </span>
        </div>
      ))}
    </div>
  );
}
