import { useMemo, useState } from "react";
import {
  useReactTable,
  getCoreRowModel,
  getSortedRowModel,
  flexRender,
  type ColumnDef,
  type SortingState,
} from "@tanstack/react-table";
import type { HighlightRule } from "../../api/types";

interface DataTableProps {
  columns: string[];
  rows: Record<string, unknown>[];
  totalRows?: number;
  loading?: boolean;
  page?: number;
  totalPages?: number;
  onPageChange?: (p: number) => void;
  /** @deprecated use autoHighlight */
  highlightRows?: boolean;
  autoHighlight?: boolean;
  rowKey?: string;
  selectedRowKeys?: Set<string>;
  onRowSelect?: (key: string, checked: boolean) => void;
  onSelectAll?: (checked: boolean, visibleKeys: string[]) => void;
  // Sorting
  sortBy?: string;
  sortDesc?: boolean;
  onSortChange?: (col: string, desc: boolean) => void;
  // Column visibility
  hiddenColumns?: Set<string>;
  // Inline column filters
  columnFilters?: Record<string, string>;
  onColumnFilterChange?: (col: string, val: string) => void;
  showColumnFilters?: boolean;
  // Highlight rules
  highlightRules?: HighlightRule[];
}

function formatCell(value: unknown): string {
  if (value === null || value === undefined) return "";
  if (Array.isArray(value)) return value.join(" | ");
  if (typeof value === "number") {
    if (Number.isInteger(value)) return value.toLocaleString("pt-BR");
    return value.toLocaleString("pt-BR", {
      minimumFractionDigits: 2,
      maximumFractionDigits: 2,
    });
  }
  return String(value);
}

function matchesRule(
  rule: HighlightRule,
  row: Record<string, unknown>,
): boolean {
  const cellVal = String(row[rule.column] ?? "");
  const v = rule.value ?? "";
  switch (rule.operator) {
    case "igual":
      return cellVal === v;
    case "contem":
      return cellVal.toLowerCase().includes(v.toLowerCase());
    case "maior":
      return (
        parseFloat(cellVal.replace(",", ".")) >
        parseFloat(v.replace(",", "."))
      );
    case "menor":
      return (
        parseFloat(cellVal.replace(",", ".")) <
        parseFloat(v.replace(",", "."))
      );
    case "e_nulo":
      return cellVal === "" || cellVal === "null" || cellVal === "undefined";
    case "nao_e_nulo":
      return (
        cellVal !== "" && cellVal !== "null" && cellVal !== "undefined"
      );
    default:
      return false;
  }
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
  autoHighlight,
  rowKey,
  selectedRowKeys,
  onRowSelect,
  onSelectAll,
  sortBy,
  sortDesc,
  onSortChange,
  hiddenColumns,
  columnFilters,
  onColumnFilterChange,
  showColumnFilters,
  highlightRules,
}: DataTableProps) {
  const selectable = !!onRowSelect && !!rowKey;
  const shouldAutoHighlight = autoHighlight ?? highlightRows ?? false;
  const isServerSort = !!onSortChange;

  const [localSort, setLocalSort] = useState<SortingState>([]);
  const [localColFilters, setLocalColFilters] = useState<
    Record<string, string>
  >({});

  const effectiveColFilters =
    columnFilters !== undefined ? columnFilters : localColFilters;

  const handleColFilterChange = (col: string, val: string) => {
    if (onColumnFilterChange) {
      onColumnFilterChange(col, val);
    } else {
      setLocalColFilters((prev) => ({ ...prev, [col]: val }));
    }
  };

  const effectiveRows = useMemo(() => {
    if (onColumnFilterChange) return rows;
    const hasFilters = Object.values(effectiveColFilters).some((v) => v !== "");
    if (!hasFilters) return rows;
    return rows.filter((row) =>
      Object.entries(effectiveColFilters).every(([col, val]) => {
        if (!val) return true;
        return String(row[col] ?? "")
          .toLowerCase()
          .includes(val.toLowerCase());
      }),
    );
  }, [rows, effectiveColFilters, onColumnFilterChange]);

  const controlledSort: SortingState = useMemo(
    () => (sortBy ? [{ id: sortBy, desc: sortDesc ?? false }] : []),
    [sortBy, sortDesc],
  );

  const activeSorting: SortingState = isServerSort ? controlledSort : localSort;

  const visibleCols = useMemo(
    () =>
      hiddenColumns?.size
        ? columns.filter((c) => !hiddenColumns!.has(c))
        : columns,
    [columns, hiddenColumns],
  );

  const colDefs = useMemo<ColumnDef<Record<string, unknown>>[]>(
    () =>
      visibleCols.map((col) => ({
        id: col,
        accessorKey: col,
        header: col,
        cell: (info) => formatCell(info.getValue()),
        size: 120,
      })),
    [visibleCols],
  );

  const table = useReactTable({
    data: effectiveRows,
    columns: colDefs,
    state: { sorting: activeSorting },
    onSortingChange: isServerSort ? undefined : setLocalSort,
    getCoreRowModel: getCoreRowModel(),
    getSortedRowModel: getSortedRowModel(),
    manualSorting: isServerSort,
    manualPagination: true,
  });

  const handleHeaderClick = (colId: string) => {
    if (isServerSort) {
      const newDesc = sortBy === colId ? !(sortDesc ?? false) : false;
      onSortChange!(colId, newDesc);
    } else {
      const current = localSort.find((s) => s.id === colId);
      if (!current) {
        setLocalSort([{ id: colId, desc: false }]);
      } else if (!current.desc) {
        setLocalSort([{ id: colId, desc: true }]);
      } else {
        setLocalSort([]);
      }
    }
  };

  const getSortIcon = (colId: string): string => {
    const s = activeSorting.find((x) => x.id === colId);
    if (!s) return "â†•";
    return s.desc ? "â–¼" : "â–²";
  };

  const rowRules = useMemo(
    () => (highlightRules ?? []).filter((r) => r.type === "row"),
    [highlightRules],
  );
  const colRules = useMemo(
    () => (highlightRules ?? []).filter((r) => r.type === "column"),
    [highlightRules],
  );

  const getRowHighlightColor = (
    row: Record<string, unknown>,
  ): string | undefined => {
    for (const rule of rowRules) {
      if (matchesRule(rule, row)) return rule.color;
    }
    return undefined;
  };

  const getCellHighlightColor = (
    colId: string,
    row: Record<string, unknown>,
  ): string | undefined => {
    for (const rule of colRules) {
      if (rule.column === colId && (!rule.value || matchesRule(rule, row))) {
        return rule.color;
      }
    }
    return undefined;
  };

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
              minWidth: visibleCols.length * 120 + (selectable ? 36 : 0),
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
                        className="px-2 py-2 text-left text-slate-300 font-semibold border-b border-slate-700 truncate select-none cursor-pointer hover:bg-slate-700 transition-colors group"
                        style={{ maxWidth: 200 }}
                        title={`${h.column.id} â€” clique para ordenar`}
                        onClick={() => handleHeaderClick(h.column.id)}
                      >
                        <span className="flex items-center gap-1">
                          <span className="truncate flex-1">
                            {flexRender(
                              h.column.columnDef.header,
                              h.getContext(),
                            )}
                          </span>
                          <span className="text-slate-500 group-hover:text-slate-300 shrink-0 text-[10px]">
                            {getSortIcon(h.column.id)}
                          </span>
                        </span>
                      </th>
                    ))}
                  </tr>
                );
              })}
              {showColumnFilters && (
                <tr style={{ background: "#162035" }}>
                  {selectable && <th className="w-9" />}
                  <th className="w-10" />
                  {visibleCols.map((col) => (
                    <th
                      key={col}
                      className="px-1 py-1 border-b border-slate-700"
                    >
                      <input
                        className="w-full bg-slate-900 border border-slate-700 rounded px-1.5 py-0.5 text-xs text-slate-200 focus:outline-none focus:border-blue-500 placeholder-slate-600"
                        placeholder="â–½"
                        value={effectiveColFilters[col] ?? ""}
                        onChange={(e) =>
                          handleColFilterChange(col, e.target.value)
                        }
                      />
                    </th>
                  ))}
                </tr>
              )}
            </thead>
            <tbody>
              {table.getRowModel().rows.map((row, idx) => {
                const tipoOp = row.original["Tipo_operacao"] as
                  | string
                  | undefined;
                const isEntrada = tipoOp?.includes("ENTRADA");
                const isSaida =
                  tipoOp?.includes("SAIDA") || tipoOp?.includes("SAÃDAS");
                const rowKeyVal = rowKey
                  ? String(row.original[rowKey] ?? "")
                  : "";
                const isSelected =
                  selectable && selectedRowKeys!.has(rowKeyVal);
                const ruleColor = getRowHighlightColor(row.original);
                const bg = isSelected
                  ? "rgba(37,99,235,0.25)"
                  : ruleColor
                    ? ruleColor
                    : shouldAutoHighlight
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
                    {row.getVisibleCells().map((cell) => {
                      const cellColor = getCellHighlightColor(
                        cell.column.id,
                        row.original,
                      );
                      return (
                        <td
                          key={cell.id}
                          className="px-2 py-1.5 border-b border-slate-800 truncate"
                          style={{
                            maxWidth: 200,
                            background: cellColor ?? undefined,
                          }}
                          title={formatCell(cell.getValue())}
                        >
                          {flexRender(
                            cell.column.columnDef.cell,
                            cell.getContext(),
                          )}
                        </td>
                      );
                    })}
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
            aria-label="Primeira pÃ¡gina"
            title="Primeira pÃ¡gina"
            className="px-2 py-1 rounded bg-slate-700 disabled:opacity-40 disabled:cursor-not-allowed hover:bg-slate-600"
          >
            Â«
          </button>
          <button
            onClick={() => onPageChange(page - 1)}
            disabled={page <= 1}
            className="px-2 py-1 rounded bg-slate-700 disabled:opacity-40 disabled:cursor-not-allowed hover:bg-slate-600"
          >
            PÃ¡gina anterior
          </button>
          <span>
            PÃ¡gina {page} / {totalPages} | Linhas filtradas:{" "}
            {(totalRows ?? 0).toLocaleString("pt-BR")}
          </span>
          <button
            onClick={() => onPageChange(page + 1)}
            disabled={page >= totalPages}
            className="px-2 py-1 rounded bg-slate-700 disabled:opacity-40 disabled:cursor-not-allowed hover:bg-slate-600"
          >
            PrÃ³xima pÃ¡gina
          </button>
          <button
            onClick={() => onPageChange(totalPages)}
            disabled={page >= totalPages}
            aria-label="Ãšltima pÃ¡gina"
            title="Ãšltima pÃ¡gina"
            className="px-2 py-1 rounded bg-slate-700 disabled:opacity-40 disabled:cursor-not-allowed hover:bg-slate-600"
          >
            Â»
          </button>
        </div>
      )}
    </div>
  );
}
