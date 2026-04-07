import {
  useMemo,
  useRef,
  useState,
  useCallback,
  type MouseEvent as EventoMouseReact,
  type ReactNode,
} from "react";
import {
  useReactTable,
  getCoreRowModel,
  getSortedRowModel,
  flexRender,
  type ColumnDef,
  type SortingState,
} from "@tanstack/react-table";
import { useVirtualizer } from "@tanstack/react-virtual";
import type { HighlightRule } from "../../api/types";
import { TableSkeleton } from "../ui/TableSkeleton";
import { RowDetailDrawer } from "../ui/RowDetailDrawer";
import { formatCell } from "./formatCell";

export type TableDensity = "compact" | "normal" | "comfortable";

const densityRowCls: Record<TableDensity, string> = {
  compact: "py-0.5 px-1 text-[10px]",
  normal: "py-1 px-2 text-xs",
  comfortable: "py-2 px-3 text-sm",
};

const densityRowHeight: Record<TableDensity, number> = {
  compact: 20,
  normal: 28,
  comfortable: 36,
};

interface DataTableProps {
  columns: string[];
  orderedColumns?: string[];
  columnWidths?: Record<string, number>;
  onOrderedColumnsChange?: (orderedColumns: string[]) => void;
  onColumnWidthChange?: (column: string, width: number) => void;
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
  sortBy?: string;
  sortDesc?: boolean;
  onSortChange?: (col: string, desc: boolean) => void;
  hiddenColumns?: Set<string>;
  columnFilters?: Record<string, string>;
  onColumnFilterChange?: (col: string, val: string) => void;
  showColumnFilters?: boolean;
  highlightRules?: HighlightRule[];
  density?: TableDensity;
  onRowClick?: (row: Record<string, unknown>) => void;
  onHeaderClick?: (col: string) => void;
  renderCell?: (col: string, value: unknown, row: Record<string, unknown>) => ReactNode | null;
  getRowBackground?: (row: Record<string, unknown>, idx: number) => string | undefined;
  pageSize?: number;
  showBarCharts?: boolean;
}

function obterLarguraColuna(
  largurasColunas: Record<string, number> | undefined,
  nomeColuna: string,
): number {
  const largura = largurasColunas?.[nomeColuna];
  if (!largura || Number.isNaN(largura)) return 120;
  return Math.max(80, largura);
}

function montarOrdemBaseColunas(
  columns: string[],
  orderedColumns?: string[],
): string[] {
  if (!orderedColumns?.length) return columns;
  return [
    ...orderedColumns.filter((coluna) => columns.includes(coluna)),
    ...columns.filter((coluna) => !orderedColumns.includes(coluna)),
  ];
}

function moverColunaNaOrdem(
  ordemAtual: string[],
  colunaOrigem: string,
  colunaDestino: string,
): string[] {
  if (colunaOrigem === colunaDestino) return ordemAtual;

  const proximaOrdem = [...ordemAtual];
  const indiceOrigem = proximaOrdem.indexOf(colunaOrigem);
  const indiceDestino = proximaOrdem.indexOf(colunaDestino);

  if (indiceOrigem < 0 || indiceDestino < 0) return ordemAtual;

  const [colunaMovida] = proximaOrdem.splice(indiceOrigem, 1);
  proximaOrdem.splice(indiceDestino, 0, colunaMovida);
  return proximaOrdem;
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
  orderedColumns,
  columnWidths,
  onOrderedColumnsChange,
  onColumnWidthChange,
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
  density = "normal",
  onRowClick,
  onHeaderClick,
  renderCell,
  getRowBackground,
  pageSize = 200,
  showBarCharts = false,
}: DataTableProps) {
  const selectable = !!onRowSelect && !!rowKey;
  const shouldAutoHighlight = autoHighlight ?? highlightRows ?? false;
  const isServerSort = !!onSortChange;
  const podeReordenarColunas = !!onOrderedColumnsChange;
  const podeRedimensionarColunas = !!onColumnWidthChange;

  const [localSort, setLocalSort] = useState<SortingState>([]);
  const [localColFilters, setLocalColFilters] = useState<
    Record<string, string>
  >({});
  const colunaArrastadaRef = useRef<string | null>(null);
  const momentoUltimaInteracaoCabecalhoRef = useRef(0);
  const scrollContainerRef = useRef<HTMLDivElement>(null);
  const [detailRow, setDetailRow] = useState<Record<string, unknown> | null>(null);
  const closeDetail = useCallback(() => setDetailRow(null), []);

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

  const ordemBaseColunas = useMemo(
    () => montarOrdemBaseColunas(columns, orderedColumns),
    [columns, orderedColumns],
  );

  const visibleCols = useMemo(() => {
    if (!hiddenColumns?.size) return ordemBaseColunas;
    return ordemBaseColunas.filter((coluna) => !hiddenColumns.has(coluna));
  }, [hiddenColumns, ordemBaseColunas]);

  const colRanges = useMemo(() => {
    if (!showBarCharts) return {} as Record<string, { min: number; max: number }>;
    const ranges: Record<string, { min: number; max: number }> = {};
    for (const col of visibleCols) {
      let min = Infinity;
      let max = -Infinity;
      for (const row of rows) {
        const v = row[col];
        const n = typeof v === "number" ? v : typeof v === "string" ? parseFloat(v) : NaN;
        if (isFinite(n)) {
          if (n < min) min = n;
          if (n > max) max = n;
        }
      }
      if (isFinite(min) && min !== max) ranges[col] = { min, max };
    }
    return ranges;
  }, [showBarCharts, rows, visibleCols]);

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

  const tableRows = table.getRowModel().rows;
  const rowVirtualizer = useVirtualizer({
    count: tableRows.length,
    getScrollElement: () => scrollContainerRef.current,
    estimateSize: () => densityRowHeight[density],
    overscan: 10,
  });
  const virtualItems = rowVirtualizer.getVirtualItems();
  const paddingTop = virtualItems.length > 0 ? virtualItems[0].start : 0;
  const paddingBottom =
    rowVirtualizer.getTotalSize() - (virtualItems.at(-1)?.end ?? 0);
  const colSpanTotal =
    visibleCols.length + 1 + (selectable ? 1 : 0);

  const handleHeaderClick = (colId: string) => {
    if (isServerSort) {
      const newDesc = sortBy === colId ? !(sortDesc ?? false) : false;
      onSortChange?.(colId, newDesc);
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
    onHeaderClick?.(colId);
  };

  const getSortIcon = (colId: string): string => {
    const s = activeSorting.find((x) => x.id === colId);
    if (!s) return "<>";
    return s.desc ? "v" : "^";
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

  function marcarInteracaoCabecalho() {
    momentoUltimaInteracaoCabecalhoRef.current = Date.now();
  }

  function cliqueCabecalhoDeveSerIgnorado(): boolean {
    return Date.now() - momentoUltimaInteracaoCabecalhoRef.current < 200;
  }

  function iniciarArrasteColuna(nomeColuna: string) {
    if (!podeReordenarColunas) return;
    colunaArrastadaRef.current = nomeColuna;
    marcarInteracaoCabecalho();
  }

  function finalizarArrasteColuna() {
    colunaArrastadaRef.current = null;
    marcarInteracaoCabecalho();
  }

  function soltarColuna(nomeColunaDestino: string) {
    if (!podeReordenarColunas) return;

    const colunaOrigem = colunaArrastadaRef.current;
    if (!colunaOrigem) return;

    onOrderedColumnsChange?.(
      moverColunaNaOrdem(ordemBaseColunas, colunaOrigem, nomeColunaDestino),
    );
    finalizarArrasteColuna();
  }

  function iniciarRedimensionamentoColuna(
    evento: EventoMouseReact<HTMLDivElement>,
    nomeColuna: string,
  ) {
    if (!podeRedimensionarColunas) return;

    evento.preventDefault();
    evento.stopPropagation();

    const posicaoInicial = evento.clientX;
    const larguraInicial = obterLarguraColuna(columnWidths, nomeColuna);
    marcarInteracaoCabecalho();

    const aoMoverMouse = (movimento: MouseEvent) => {
      const deslocamento = movimento.clientX - posicaoInicial;
      onColumnWidthChange?.(nomeColuna, larguraInicial + deslocamento);
    };

    const aoSoltarMouse = () => {
      window.removeEventListener("mousemove", aoMoverMouse);
      window.removeEventListener("mouseup", aoSoltarMouse);
      marcarInteracaoCabecalho();
    };

    window.addEventListener("mousemove", aoMoverMouse);
    window.addEventListener("mouseup", aoSoltarMouse);
  }

  return (
    <div className="flex flex-col h-full">
      <div ref={scrollContainerRef} className="overflow-auto flex-1">
        {loading ? (
          <TableSkeleton rows={8} cols={Math.min(visibleCols.length || 5, 8)} />
        ) : (
          <table
            className="w-full border-collapse text-xs"
            style={{
              tableLayout: "fixed",
              minWidth:
                visibleCols.reduce(
                  (total, coluna) =>
                    total + obterLarguraColuna(columnWidths, coluna),
                  0,
                ) + (selectable ? 36 : 0),
            }}
          >
            <colgroup>
              {selectable && <col style={{ width: 36 }} />}
              <col style={{ width: 40 }} />
              {visibleCols.map((coluna) => (
                <col
                  key={coluna}
                  style={{ width: obterLarguraColuna(columnWidths, coluna) }}
                />
              ))}
            </colgroup>
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
                        className="px-2 py-2 text-left text-slate-300 font-semibold border-b border-slate-700 truncate select-none cursor-pointer hover:bg-slate-700 transition-colors group relative"
                        style={{
                          width: obterLarguraColuna(columnWidths, h.column.id),
                          maxWidth: obterLarguraColuna(columnWidths, h.column.id),
                        }}
                        title={`${h.column.id} - clique para ordenar`}
                        draggable={podeReordenarColunas}
                        onDragStart={() => iniciarArrasteColuna(h.column.id)}
                        onDragOver={(evento) => {
                          if (!podeReordenarColunas) return;
                          evento.preventDefault();
                        }}
                        onDrop={() => soltarColuna(h.column.id)}
                        onDragEnd={finalizarArrasteColuna}
                        onClick={() => {
                          if (cliqueCabecalhoDeveSerIgnorado()) return;
                          handleHeaderClick(h.column.id);
                        }}
                      >
                        <span className="flex items-center gap-1 pr-3">
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
                        {podeRedimensionarColunas && (
                          <div
                            className="absolute top-0 right-0 h-full w-2 cursor-col-resize"
                            onMouseDown={(evento) =>
                              iniciarRedimensionamentoColuna(
                                evento,
                                h.column.id,
                              )
                            }
                            title={`Redimensionar ${h.column.id}`}
                          />
                        )}
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
                        placeholder="v"
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
              {paddingTop > 0 && (
                <tr>
                  <td style={{ height: paddingTop }} colSpan={colSpanTotal} />
                </tr>
              )}
              {virtualItems.map((vi) => {
                const row = tableRows[vi.index];
                const idx = vi.index;
                const tipoOp = row.original["Tipo_operacao"] as
                  | string
                  | undefined;
                const isEntrada = tipoOp?.includes("ENTRADA");
                const isSaida =
                  tipoOp?.includes("SAIDA") || tipoOp?.includes("SAIDAS");
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
                const customBg = getRowBackground?.(row.original, idx);
                const resolvedBg = customBg ?? bg;
                const cellCls = densityRowCls[density];
                return (
                  <tr
                    key={row.id}
                    data-index={vi.index}
                    style={{
                      background: resolvedBg,
                      outline: isSelected
                        ? "1px solid rgba(59,130,246,0.5)"
                        : undefined,
                      cursor: selectable || onRowClick ? "pointer" : undefined,
                    }}
                    className="hover:brightness-125 transition-all"
                    onClick={
                      selectable
                        ? () => onRowSelect?.(rowKeyVal, !isSelected)
                        : onRowClick
                          ? () => { setDetailRow(row.original); onRowClick(row.original); }
                          : undefined
                    }
                  >
                    {selectable && (
                      <td className={`${cellCls} border-b border-slate-800 text-center`} style={{ width: 36 }}>
                        <input
                          type="checkbox"
                          aria-label={`Selecionar linha ${rowKeyVal}`}
                          title={`Selecionar linha ${rowKeyVal}`}
                          checked={isSelected}
                          onChange={(e) => {
                            e.stopPropagation();
                            onRowSelect?.(rowKeyVal, e.target.checked);
                          }}
                          onClick={(e) => e.stopPropagation()}
                          className="accent-blue-500 cursor-pointer"
                        />
                      </td>
                    )}
                    <td className={`${cellCls} text-slate-500 text-right border-b border-slate-800`} style={{ width: 40 }}>
                      {(page - 1) * pageSize + idx + 1}
                    </td>
                    {row.getVisibleCells().map((cell) => {
                      const cellColor = getCellHighlightColor(
                        cell.column.id,
                        row.original,
                      );
                      const customContent = renderCell
                        ? renderCell(cell.column.id, cell.getValue(), row.original)
                        : null;
                      const barRange = showBarCharts ? colRanges[cell.column.id] : undefined;
                      const rawVal = cell.getValue();
                      const numVal = barRange
                        ? (typeof rawVal === "number" ? rawVal : typeof rawVal === "string" ? parseFloat(rawVal) : NaN)
                        : NaN;
                      const barPct = (barRange && isFinite(numVal))
                        ? Math.max(0, Math.min(100, ((numVal - barRange.min) / (barRange.max - barRange.min)) * 100))
                        : null;
                      return (
                        <td
                          key={cell.id}
                          className={`${cellCls} border-b border-slate-800 truncate${barPct !== null ? " relative overflow-hidden" : ""}`}
                          style={{
                            width: obterLarguraColuna(columnWidths, cell.column.id),
                            maxWidth: obterLarguraColuna(
                              columnWidths,
                              cell.column.id,
                            ),
                            background: cellColor ?? undefined,
                          }}
                          title={customContent == null ? formatCell(cell.getValue()) : undefined}
                        >
                          {barPct !== null && customContent == null ? (
                            <>
                              <div
                                className="absolute inset-y-0 left-0 bg-blue-500/20 pointer-events-none"
                                style={{ width: `${barPct}%` }}
                              />
                              <span className="relative">{formatCell(rawVal)}</span>
                            </>
                          ) : customContent != null
                            ? customContent
                            : flexRender(
                                cell.column.columnDef.cell,
                                cell.getContext(),
                              )}
                        </td>
                      );
                    })}
                  </tr>
                );
              })}
              {paddingBottom > 0 && (
                <tr>
                  <td style={{ height: paddingBottom }} colSpan={colSpanTotal} />
                </tr>
              )}
            </tbody>
          </table>
        )}
      </div>

      {onPageChange && (
        <div className="flex items-center gap-3 px-3 py-2 border-t border-slate-700 bg-slate-900 text-xs text-slate-400">
          <button
            onClick={() => onPageChange(1)}
            disabled={page <= 1}
            aria-label="Primeira pagina"
            title="Primeira pagina"
            className="px-2 py-1 rounded bg-slate-700 disabled:opacity-40 disabled:cursor-not-allowed hover:bg-slate-600"
          >
            {"<<"}
          </button>
          <button
            onClick={() => onPageChange(page - 1)}
            disabled={page <= 1}
            className="px-2 py-1 rounded bg-slate-700 disabled:opacity-40 disabled:cursor-not-allowed hover:bg-slate-600"
          >
            Pagina anterior
          </button>
          <span>
            Pagina {page} / {totalPages} | Linhas filtradas:{" "}
            {(totalRows ?? 0).toLocaleString("pt-BR")}
          </span>
          <button
            onClick={() => onPageChange(page + 1)}
            disabled={page >= totalPages}
            className="px-2 py-1 rounded bg-slate-700 disabled:opacity-40 disabled:cursor-not-allowed hover:bg-slate-600"
          >
            Proxima pagina
          </button>
          <button
            onClick={() => onPageChange(totalPages)}
            disabled={page >= totalPages}
            aria-label="Ultima pagina"
            title="Ultima pagina"
            className="px-2 py-1 rounded bg-slate-700 disabled:opacity-40 disabled:cursor-not-allowed hover:bg-slate-600"
          >
            {">>"}
          </button>
        </div>
      )}
      <RowDetailDrawer row={detailRow} onClose={closeDetail} />
    </div>
  );
}
