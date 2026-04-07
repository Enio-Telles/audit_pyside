interface TableSkeletonProps {
  rows?: number;
  cols?: number;
}

export function TableSkeleton({ rows = 8, cols = 5 }: TableSkeletonProps) {
  return (
    <div className="w-full animate-pulse p-2">
      {/* Header */}
      <div className="flex gap-2 mb-2">
        {Array.from({ length: cols }).map((_, i) => (
          <div key={i} className="h-5 bg-slate-700 rounded flex-1" />
        ))}
      </div>
      {/* Rows */}
      {Array.from({ length: rows }).map((_, r) => (
        <div key={r} className="flex gap-2 mb-1.5">
          {Array.from({ length: cols }).map((_, c) => (
            <div
              key={c}
              className="h-4 bg-slate-800 rounded flex-1"
              style={{ opacity: 1 - r * 0.08 }}
            />
          ))}
        </div>
      ))}
    </div>
  );
}
