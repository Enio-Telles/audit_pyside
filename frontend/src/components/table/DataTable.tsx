import { useMemo } from "react";
import {
  useReactTable,
  getCoreRowModel,
  flexRender,
  type ColumnDef,
} from "@tanstack/react-table";

interface DataTableProps {
  columns: string[];
  rows: Record<string, unknown>[];
  totalRows?: number;
  loading?: boolean;
  page?: number;
  totalPages?: number;
  onPageChange?: (p: number) => void;
  highlightRows?: boolean;
  /** Campo que identifica unicamente cada linha para seleção */
  rowKey?: string;
  /** Conjunto de chaves de linhas selecionadas */
  selectedRowKeys?: Set<string>;
  /** Callback ao clicar no checkbox de uma linha */
  onRowSelect?: (key: string, checked: boolean) => void;
  /** Callback ao clicar no checkbox de cabeçalho (selecionar/desmarcar todos visíveis) */
  onSelectAll?: (checked: boolean, visibleKeys: string[]) => void;
}

const intlInteger = new Intl.NumberFormat("pt-BR");
const intlDecimal = new Intl.NumberFormat("pt-BR", {
  minimumFractionDigits: 2,
  maximumFractionDigits: 2,
});

function formatCell(value: unknown): string {
  if (value === null || value === undefined) return "";
  if (Array.isArray(value)) return value.join(" | ");
  if (typeof value === "number") {
    // ⚡ Bolt Optimization: Use cached Intl.NumberFormat instances instead of Number.prototype.toLocaleString()
    // This avoids repeatedly allocating locale data and parsing options on every cell render, improving performance up to ~100x for number formatting.
    if (Number.isInteger(value)) return intlInteger.format(value);
    return intlDecimal.format(value);
  }
  return String(value);
}

export function DataTable({
  columns,
  rows,
  totalRows,
  loading,
  page = 1,
  totalPages = 1,
  onPageChange,
  highlightRows,
  rowKey,
  selectedRowKeys,
  onRowSelect,
  onSelectAll,
}: DataTableProps) {
  const selectable = !!onRowSelect && !!rowKey;
  const colDefs = useMemo<ColumnDef<Record<string, unknown>>[]>(
    () =>
      columns.map((col) => ({
        id: col,
        accessorKey: col,
        header: col,
        cell: (info) => formatCell(info.getValue()),
        size: 120,
      })),
    [columns],
  );

  const table = useReactTable({
    data: rows,
    columns: colDefs,
    getCoreRowModel: getCoreRowModel(),
    manualPagination: true,
  });

  return (
    <div className="flex flex-col h-full">
      <div className="overflow-auto flex-1">
        {loading ? (
          <div className="flex items-center justify-center h-32 text-slate-400">
            Carregando...
          </div>
        ) : (
          <table
            className="w-full border-collapse text-xs"
            style={{
              tableLayout: "fixed",
              minWidth: columns.length * 120 + (selectable ? 36 : 0),
            }}
          >
            <thead
              className="sticky top-0 z-10"
              style={{ background: "#1e2d4a" }}
            >
              {table.getHeaderGroups().map((hg) => {
                const visibleKeys = selectable
                  ? table
                      .getRowModel()
                      .rows.map((r) => String(r.original[rowKey!] ?? ""))
                  : [];
                const allSelected =
                  selectable &&
                  visibleKeys.length > 0 &&
                  visibleKeys.every((k) => selectedRowKeys!.has(k));
                return (
                  <tr key={hg.id}>
                    {selectable && (
                      <th className="w-9 px-2 py-2 border-b border-slate-700 text-center">
                        <input
                          type="checkbox"
                          aria-label="Selecionar todas as linhas"
                          title="Selecionar todas as linhas"
                          checked={allSelected}
                          onChange={(e) =>
                            onSelectAll?.(e.target.checked, visibleKeys)
                          }
                          className="accent-blue-500 cursor-pointer"
                        />
                      </th>
                    )}
                    <th className="w-10 px-2 py-2 text-slate-400 text-right border-b border-slate-700">
                      #
                    </th>
                    {hg.headers.map((h) => (
                      <th
                        key={h.id}
                        className="px-2 py-2 text-left text-slate-300 font-semibold border-b border-slate-700 truncate"
                        style={{ maxWidth: 200 }}
                        title={h.column.id}
                      >
                        {flexRender(h.column.columnDef.header, h.getContext())}
                      </th>
                    ))}
                  </tr>
                );
              })}
            </thead>
            <tbody>
              {table.getRowModel().rows.map((row, idx) => {
                const tipoOp = row.original["Tipo_operacao"] as
                  | string
                  | undefined;
                const isEntrada = tipoOp?.includes("ENTRADA");
                const isSaida =
                  tipoOp?.includes("SAIDA") || tipoOp?.includes("SAÍDAS");
                const rowKeyVal = rowKey
                  ? String(row.original[rowKey] ?? "")
                  : "";
                const isSelected =
                  selectable && selectedRowKeys!.has(rowKeyVal);
                const bg = isSelected
                  ? "rgba(37,99,235,0.25)"
                  : highlightRows
                    ? isEntrada
                      ? "rgba(30,80,30,0.5)"
                      : isSaida
                        ? "rgba(120,30,30,0.5)"
                        : idx % 2 === 0
                          ? "#0f1b33"
                          : "#0a1628"
                    : idx % 2 === 0
                      ? "#0f1b33"
                      : "#0a1628";
                return (
                  <tr
                    key={row.id}
                    style={{
                      background: bg,
                      outline: isSelected
                        ? "1px solid rgba(59,130,246,0.5)"
                        : undefined,
                      cursor: selectable ? "pointer" : undefined,
                    }}
                    className="hover:brightness-125 transition-all"
                    onClick={
                      selectable
                        ? () => onRowSelect!(rowKeyVal, !isSelected)
                        : undefined
                    }
                  >
                    {selectable && (
                      <td className="px-2 py-1.5 border-b border-slate-800 text-center">
                        <input
                          type="checkbox"
                          aria-label={`Selecionar linha ${rowKeyVal}`}
                          title={`Selecionar linha ${rowKeyVal}`}
                          checked={isSelected}
                          onChange={(e) => {
                            e.stopPropagation();
                            onRowSelect!(rowKeyVal, e.target.checked);
                          }}
                          onClick={(e) => e.stopPropagation()}
                          className="accent-blue-500 cursor-pointer"
                        />
                      </td>
                    )}
                    <td className="px-2 py-1.5 text-slate-500 text-right border-b border-slate-800">
                      {(page - 1) * 200 + idx + 1}
                    </td>
                    {row.getVisibleCells().map((cell) => (
                      <td
                        key={cell.id}
                        className="px-2 py-1.5 border-b border-slate-800 truncate"
                        style={{ maxWidth: 200 }}
                        title={formatCell(cell.getValue())}
                      >
                        {flexRender(
                          cell.column.columnDef.cell,
                          cell.getContext(),
                        )}
                      </td>
                    ))}
                  </tr>
                );
              })}
            </tbody>
          </table>
        )}
      </div>

      {/* Pagination */}
      {onPageChange && (
        <div className="flex items-center gap-3 px-3 py-2 border-t border-slate-700 bg-slate-900 text-xs text-slate-400">
          <button
            onClick={() => onPageChange(1)}
            disabled={page <= 1}
            aria-label="Primeira página"
            title="Primeira página"
            className="px-2 py-1 rounded bg-slate-700 disabled:opacity-40 disabled:cursor-not-allowed hover:bg-slate-600"
          >
            «
          </button>
          <button
            onClick={() => onPageChange(page - 1)}
            disabled={page <= 1}
            className="px-2 py-1 rounded bg-slate-700 disabled:opacity-40 disabled:cursor-not-allowed hover:bg-slate-600"
          >
            Página anterior
          </button>
          <span>
            Página {page} / {totalPages} | Linhas filtradas:{" "}
            {(totalRows ?? 0).toLocaleString("pt-BR")}
          </span>
          <button
            onClick={() => onPageChange(page + 1)}
            disabled={page >= totalPages}
            className="px-2 py-1 rounded bg-slate-700 disabled:opacity-40 disabled:cursor-not-allowed hover:bg-slate-600"
          >
            Próxima página
          </button>
          <button
            onClick={() => onPageChange(totalPages)}
            disabled={page >= totalPages}
            aria-label="Última página"
            title="Última página"
            className="px-2 py-1 rounded bg-slate-700 disabled:opacity-40 disabled:cursor-not-allowed hover:bg-slate-600"
          >
            »
          </button>
        </div>
      )}
    </div>
  );
}
