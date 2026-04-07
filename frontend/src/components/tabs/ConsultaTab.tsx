import { useMemo, useState } from "react";
import * as XLSX from "xlsx";
import { useQuery } from "@tanstack/react-query";
import { parquetApi, cnpjApi } from "../../api/client";
import { useAppStore } from "../../store/appStore";
import { DataTable } from "../table/DataTable";
import type { TableDensity } from "../table/DataTable";
import { FilterBar } from "../table/FilterBar";
import { ColumnToggle } from "../table/ColumnToggle";
import { HighlightRulesPanel } from "../table/HighlightRulesPanel";
import { usePreferenciasColunas } from "../../hooks/usePreferenciasColunas";
import { useAutoHighlight } from "../../hooks/useAutoHighlight";
import type { AutoHighlightMode } from "../../hooks/useAutoHighlight";
import { SummaryCards } from "../ui/SummaryCards";
import { DensityToggle } from "../ui/DensityToggle";
import { ColumnStatsPanel } from "../ui/ColumnStatsPanel";
import { FilterPresets } from "../ui/FilterPresets";

const CHAVE_PREFERENCIAS_COLUNAS_CONSULTA = "consulta_colunas_v1";

export function ConsultaTab() {
  const {
    selectedFile,
    selectedCnpj,
    consultaFilters,
    addConsultaFilter,
    removeConsultaFilter,
    clearConsultaFilters,
    consultaVisibleCols,
    consultaPage,
    setConsultaPage,
    consultaSortList,
    setConsultaSortList,
    consultaColumnFilters,
    setConsultaColumnFilter,
    clearConsultaColumnFilters,
    consultaHiddenCols,
    setConsultaHiddenCol,
    resetConsultaHiddenCols,
    consultaHighlightRules,
    addConsultaHighlightRule,
    removeConsultaHighlightRule,
  } = useAppStore();

  const [showColFilters, setShowColFilters] = useState(false);
  const [density, setDensity] = useState<TableDensity>("normal");
  const [statsCol, setStatsCol] = useState<string | null>(null);
  const [autoHighlightMode, setAutoHighlightMode] = useState<AutoHighlightMode | null>(null);
  const [showBarCharts, setShowBarCharts] = useState(false);

  const { data: schema } = useQuery({
    queryKey: ['schema', selectedCnpj, selectedFile?.path],
    queryFn: () => cnpjApi.getSchema(selectedCnpj!, selectedFile!.path),
    enabled: !!selectedCnpj && !!selectedFile,
  });

  const { data: metadata } = useQuery({
    queryKey: ['parquet-metadata', selectedFile?.path],
    queryFn: () => parquetApi.metadata(selectedFile!.path),
    enabled: !!selectedFile,
    staleTime: 5 * 60_000,
  });

  const allCols = useMemo(() => schema?.columns ?? [], [schema?.columns]);
  const {
    ordemColunas,
    largurasColunas,
    definirOrdemColunas,
    definirLarguraColuna,
    redefinirPreferenciasColunas,
  } = usePreferenciasColunas(CHAVE_PREFERENCIAS_COLUNAS_CONSULTA, allCols);
  const baseVisibleCols =
    consultaVisibleCols.length > 0 ? consultaVisibleCols : ordemColunas;
  const visibleCols = baseVisibleCols.filter((c) => !consultaHiddenCols.has(c));

  // Merge server-side column filters with user-added filters
  const colFilterItems = Object.entries(consultaColumnFilters)
    .filter(([, v]) => v !== '')
    .map(([column, value]) => ({ column, operator: 'contem' as const, value }));
  const allFilters = [...consultaFilters, ...colFilterItems];

  const { data, isLoading } = useQuery({
    queryKey: [
      'parquet',
      selectedFile?.path,
      allFilters,
      visibleCols,
      consultaPage,
      consultaSortList,
    ],
    queryFn: () =>
      parquetApi.query({
        path: selectedFile!.path,
        filters: allFilters,
        visible_columns: visibleCols,
        page: consultaPage,
        page_size: 200,
        sort_by: consultaSortList[0]?.col,
        sort_desc: consultaSortList[0]?.desc,
        sort_by_list: consultaSortList.map((s) => s.col),
      }),
    enabled: !!selectedFile,
    placeholderData: (prev) => prev,
  });

  const autoHighlightRules = useAutoHighlight(
    visibleCols,
    data?.rows ?? [],
    autoHighlightMode ?? "nulls",
  );
  const effectiveHighlightRules = useMemo(
    () =>
      autoHighlightMode !== null
        ? [...autoHighlightRules, ...consultaHighlightRules]
        : consultaHighlightRules,
    [autoHighlightMode, autoHighlightRules, consultaHighlightRules],
  );

  const btnCls =
    'px-3 py-1.5 rounded text-xs font-medium cursor-pointer transition-colors';

  if (!selectedFile) {
    return (
      <div className="flex items-center justify-center h-full text-slate-500">
        Selecione um arquivo Parquet na barra lateral.
      </div>
    );
  }

  return (
    <div className="flex flex-col h-full p-3 gap-2">
      {/* File info */}
      <div className="text-xs text-slate-400">
        {selectedFile.name} | Colunas visíveis: {visibleCols.length}/
        {allCols.length}
      </div>

      {/* Filter bar */}
      <FilterBar
        columns={allCols}
        filters={consultaFilters}
        onAdd={addConsultaFilter}
        onRemove={removeConsultaFilter}
        onClear={clearConsultaFilters}
      />

      {/* Highlight rules */}
      <HighlightRulesPanel
        columns={allCols}
        rules={consultaHighlightRules}
        onAdd={addConsultaHighlightRule}
        onRemove={removeConsultaHighlightRule}
      />

      {/* Filter presets */}
      <FilterPresets
        columns={allCols}
        onApply={(f) => {
          addConsultaFilter(f);
          setConsultaPage(1);
        }}
      />

      {/* Toolbar */}
      <div className="flex gap-2 items-center flex-wrap">
        <ColumnToggle
          allColumns={allCols}
          orderedColumns={ordemColunas}
          hiddenColumns={consultaHiddenCols}
          columnWidths={largurasColunas}
          onChange={setConsultaHiddenCol}
          onOrderChange={definirOrdemColunas}
          onWidthChange={definirLarguraColuna}
          onReset={() => {
            resetConsultaHiddenCols();
            redefinirPreferenciasColunas();
          }}
        />

        <button
          className={
            btnCls +
            (showColFilters
              ? ' bg-blue-700 hover:bg-blue-600 text-white'
              : ' bg-slate-700 hover:bg-slate-600 text-slate-200')
          }
          title="Alternar filtros por coluna"
          onClick={() => setShowColFilters((v) => !v)}
        >
          Filtros ▽
        </button>

        {Object.values(consultaColumnFilters).some((v) => v !== '') && (
          <button
            className={btnCls + ' bg-red-800 hover:bg-red-700 text-slate-200'}
            onClick={clearConsultaColumnFilters}
          >
            Limpar filtros col.
          </button>
        )}

        {/* Auto-highlight toggle */}
        <button
          className={
            btnCls +
            (autoHighlightMode !== null
              ? ' bg-amber-700 hover:bg-amber-600 text-white'
              : ' bg-slate-700 hover:bg-slate-600 text-slate-200')
          }
          title={
            autoHighlightMode === null
              ? 'Ativar destaque automático de nulos'
              : autoHighlightMode === 'nulls'
              ? 'Modo: nulos — clique para ativar destaque de outliers'
              : 'Modo: nulos + outliers — clique para desativar'
          }
          onClick={() => {
            setAutoHighlightMode((prev) =>
              prev === null ? 'nulls' : prev === 'nulls' ? 'all' : null,
            );
          }}
        >
          {autoHighlightMode === null
            ? 'Auto ✦'
            : autoHighlightMode === 'nulls'
            ? 'Auto ✦ nulos'
            : 'Auto ✦ nulos+outliers'}
        </button>

        <button
          className={btnCls + ' bg-slate-700 hover:bg-slate-600 text-slate-200'}
          onClick={async () => {
            if (!selectedFile) return;
            const blob = await parquetApi.exportCsv({
              path: selectedFile.path,
              filters: allFilters,
              visible_columns: visibleCols,
              sort_by: consultaSortList[0]?.col,
              sort_desc: consultaSortList[0]?.desc,
              sort_by_list: consultaSortList.map((s) => s.col),
            });
            const url = URL.createObjectURL(blob);
            const a = document.createElement("a");
            a.href = url;
            a.download = `${selectedFile.name.replace(/\.parquet$/, "")}.csv`;
            a.click();
            URL.revokeObjectURL(url);
          }}
        >
          Exportar CSV
        </button>

        <button
          className={btnCls + ' bg-emerald-800 hover:bg-emerald-700 text-slate-200'}
          onClick={() => {
            if (!data || !data.rows.length) return;
            const ws = XLSX.utils.json_to_sheet(
              data.rows.map((r) =>
                Object.fromEntries(visibleCols.map((c) => [c, r[c] ?? ""])),
              ),
            );
            const wb = XLSX.utils.book_new();
            XLSX.utils.book_append_sheet(wb, ws, "Dados");
            const fname = selectedFile?.name.replace(/\.parquet$/, "") ?? "export";
            XLSX.writeFile(wb, `${fname}.xlsx`);
          }}
        >
          Exportar XLSX
        </button>

        <DensityToggle value={density} onChange={setDensity} />
        <button
          className={btnCls + (showBarCharts ? ' bg-blue-700 text-white' : ' bg-slate-700 hover:bg-slate-600 text-slate-300')}
          title="Barras inline em células numéricas"
          onClick={() => setShowBarCharts((v) => !v)}
        >
          ▦ Barras
        </button>
      </div>

      {/* Summary cards */}
      <SummaryCards
        totalRows={data?.total_rows ?? 0}
        currentPageRows={data?.rows?.length ?? 0}
        loading={isLoading}
      />

      {/* Table */}
      <div className="flex-1 overflow-hidden border border-slate-700 rounded">
        <DataTable
          columns={data?.columns ?? visibleCols}
          orderedColumns={ordemColunas}
          columnWidths={largurasColunas}
          onOrderedColumnsChange={definirOrdemColunas}
          onColumnWidthChange={definirLarguraColuna}
          rows={data?.rows ?? []}
          totalRows={data?.total_rows}
          loading={isLoading}
          page={consultaPage}
          totalPages={data?.total_pages}
          onPageChange={(p) => {
            setConsultaPage(p);
          }}
          sortBy={consultaSortList[0]?.col}
          sortDesc={consultaSortList[0]?.desc}
          onSortChange={(col, desc) => {
            setConsultaSortList([{ col, desc }]);
            setConsultaPage(1);
          }}
          hiddenColumns={consultaHiddenCols}
          columnFilters={consultaColumnFilters}
          onColumnFilterChange={(col, val) => {
            setConsultaColumnFilter(col, val);
            setConsultaPage(1);
          }}
          showColumnFilters={showColFilters}
          highlightRules={effectiveHighlightRules}
          density={density}
          onHeaderClick={setStatsCol}
          showBarCharts={showBarCharts}
        />
      </div>

      <ColumnStatsPanel
        col={statsCol}
        metadata={metadata ?? null}
        onClose={() => setStatsCol(null)}
      />
    </div>
  );
}
