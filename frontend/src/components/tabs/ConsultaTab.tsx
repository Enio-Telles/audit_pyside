import { useMemo, useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { parquetApi, cnpjApi } from "../../api/client";
import { useAppStore } from "../../store/appStore";
import { DataTable } from "../table/DataTable";
import { FilterBar } from "../table/FilterBar";
import { ColumnToggle } from "../table/ColumnToggle";
import { HighlightRulesPanel } from "../table/HighlightRulesPanel";
import { usePreferenciasColunas } from "../../hooks/usePreferenciasColunas";

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
    consultaSort,
    setConsultaSort,
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

  const { data: schema } = useQuery({
    queryKey: ['schema', selectedCnpj, selectedFile?.path],
    queryFn: () => cnpjApi.getSchema(selectedCnpj!, selectedFile!.path),
    enabled: !!selectedCnpj && !!selectedFile,
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
      consultaSort,
    ],
    queryFn: () =>
      parquetApi.query({
        path: selectedFile!.path,
        filters: allFilters,
        visible_columns: visibleCols,
        page: consultaPage,
        page_size: 200,
        sort_by: consultaSort?.col,
        sort_desc: consultaSort?.desc,
      }),
    enabled: !!selectedFile,
    placeholderData: (prev) => prev,
  });

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

        <button
          className={btnCls + ' bg-slate-700 hover:bg-slate-600 text-slate-200'}
          onClick={() => {
            if (!data) return;
            const csv = [
              visibleCols.join(","),
              ...data.rows.map((r) =>
                visibleCols.map((c) => JSON.stringify(r[c] ?? "")).join(","),
              ),
            ].join("\n");
            const blob = new Blob([csv], { type: "text/csv" });
            const a = document.createElement("a");
            a.href = URL.createObjectURL(blob);
            a.download = "export.csv";
            a.click();
          }}
        >
          Exportar CSV
        </button>
      </div>

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
          sortBy={consultaSort?.col}
          sortDesc={consultaSort?.desc}
          onSortChange={(col, desc) => {
            setConsultaSort({ col, desc });
            setConsultaPage(1);
          }}
          hiddenColumns={consultaHiddenCols}
          columnFilters={consultaColumnFilters}
          onColumnFilterChange={(col, val) => {
            setConsultaColumnFilter(col, val);
            setConsultaPage(1);
          }}
          showColumnFilters={showColFilters}
          highlightRules={consultaHighlightRules}
        />
      </div>
    </div>
  );
}
