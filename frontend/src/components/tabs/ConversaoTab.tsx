import {
  useState,
  useMemo,
  useCallback,
  type ReactNode,
} from "react";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { estoqueApi } from "../../api/client";
import { useAppStore } from "../../store/appStore";
import { ColumnToggle } from "../table/ColumnToggle";
import { DataTable } from "../table/DataTable";
import { usePreferenciasColunas } from "../../hooks/usePreferenciasColunas";

type Row = Record<string, unknown>;

const ROWS_PER_PAGE = 150;
const CHAVE_PREFERENCIAS_COLUNAS_CONVERSAO = "conversao_colunas_v1";

function rowKey(r: Row) {
  return `${r["id_agrupado"]}__${r["id_produtos"]}`;
}

const LARGURAS_INICIAIS_COLUNAS: Record<string, number> = {
  id_agrupado: 176,
  id_produtos: 144,
  descr_padrao: 288,
  unid: 80,
  unid_ref: 96,
  fator: 112,
  fator_manual: 90,
  unid_ref_manual: 110,
  preco_medio: 128,
  origem_preco: 128,
};

const DISPLAY_COLS_ORDER = [
  "id_agrupado",
  "descr_padrao",
  "unid",
  "unid_ref",
  "fator",
  "fator_manual",
  "unid_ref_manual",
  "preco_medio",
  "origem_preco",
  "id_produtos",
];

export function ConversaoTab() {
  const { selectedCnpj } = useAppStore();
  const queryClient = useQueryClient();
  const [filterDesc, setFilterDesc] = useState("");
  const [filterIdAgrupado, setFilterIdAgrupado] = useState("");
  const [showSingleUnit, setShowSingleUnit] = useState(false);
  const [selectedIdAgrupado, setSelectedIdAgrupado] = useState<string | null>(
    null,
  );
  const [editingKey, setEditingKey] = useState<string | null>(null);
  const [editValue, setEditValue] = useState("");
  const [newUnidRef, setNewUnidRef] = useState("");
  const [hiddenCols, setHiddenCols] = useState<Set<string>>(new Set());
  const [page, setPage] = useState(1);

  const { data, isLoading, refetch } = useQuery({
    queryKey: ["fatores_conversao", selectedCnpj],
    queryFn: () => estoqueApi.fatoresConversao(selectedCnpj!, 1, 2000),
    enabled: !!selectedCnpj,
    placeholderData: (prev) => prev,
  });

  const updateMutation = useMutation({
    mutationFn: (vars: {
      id_agrupado: string;
      id_produtos: string;
      fator?: number;
      unid_ref?: string;
    }) =>
      estoqueApi.updateFator(
        selectedCnpj!,
        vars.id_agrupado,
        vars.id_produtos,
        vars.fator,
        vars.unid_ref,
      ),
    onSuccess: () =>
      queryClient.invalidateQueries({
        queryKey: ["fatores_conversao", selectedCnpj],
      }),
  });

  const batchMutation = useMutation({
    mutationFn: (vars: { id_agrupado: string; unid_ref: string }) =>
      estoqueApi.batchUpdateUnidRef(
        selectedCnpj!,
        vars.id_agrupado,
        vars.unid_ref,
      ),
    onSuccess: () =>
      queryClient.invalidateQueries({
        queryKey: ["fatores_conversao", selectedCnpj],
      }),
  });

  const rows = data?.rows;
  const allRows: Row[] = useMemo(() => rows ?? [], [rows]);
  const dataColumns: string[] = data?.columns ?? [];
  const {
    ordemColunas,
    largurasColunas,
    definirOrdemColunas,
    definirLarguraColuna,
    redefinirPreferenciasColunas,
  } = usePreferenciasColunas(
    CHAVE_PREFERENCIAS_COLUNAS_CONVERSAO,
    dataColumns,
    LARGURAS_INICIAIS_COLUNAS,
  );

  const displayCols = useMemo(
    () => ordemColunas.filter((c) => DISPLAY_COLS_ORDER.includes(c)),
    [ordemColunas],
  );

  // Unique id_agrupadoes for the filter dropdown
  const uniqueIdAgrupados = useMemo(() => {
    const seen = new Set<string>();
    for (const r of allRows) {
      const id = String(r["id_agrupado"] ?? "");
      if (id) seen.add(id);
    }
    return [...seen].sort();
  }, [allRows]);

  // Compute which id_agrupadoes have only one unit type (single-unit products)
  const singleUnitIds = useMemo(() => {
    const grouped = new Map<string, Set<string>>();
    for (const r of allRows) {
      const id = String(r["id_agrupado"] ?? "");
      const u = String(r["unid"] ?? "");
      if (!grouped.has(id)) grouped.set(id, new Set());
      grouped.get(id)!.add(u);
    }
    const result = new Set<string>();
    for (const [id, units] of grouped) {
      if (units.size <= 1) result.add(id);
    }
    return result;
  }, [allRows]);

  const filteredRows = useMemo(() => {
    // ⚡ Bolt Optimization: Hoist string casing outside filter loop to prevent O(N) performance degradation
    const filterDescLower = filterDesc ? filterDesc.toLowerCase() : "";
    return allRows.filter((r) => {
      if (filterIdAgrupado) {
        if (String(r["id_agrupado"] ?? "") !== filterIdAgrupado) return false;
      }
      if (filterDescLower) {
        const desc = String(r["descr_padrao"] ?? "").toLowerCase();
        if (!desc.includes(filterDescLower)) return false;
      }
      if (!showSingleUnit && singleUnitIds.has(String(r["id_agrupado"] ?? "")))
        return false;
      return true;
    });
  }, [allRows, filterIdAgrupado, filterDesc, showSingleUnit, singleUnitIds]);

  const totalFiltered = filteredRows.length;
  const totalPages = Math.max(1, Math.ceil(totalFiltered / ROWS_PER_PAGE));
  const pagedRows = useMemo(
    () => filteredRows.slice((page - 1) * ROWS_PER_PAGE, page * ROWS_PER_PAGE),
    [filteredRows, page],
  );

  // Available units for the selected id_agrupado's unid_ref panel
  const availableUnids = useMemo(
    () =>
      selectedIdAgrupado
        ? [
            ...new Set(
              allRows
                .filter(
                  (r) => String(r["id_agrupado"] ?? "") === selectedIdAgrupado,
                )
                .map((r) => String(r["unid"] ?? ""))
                .filter(Boolean),
            ),
          ]
        : [],
    [allRows, selectedIdAgrupado],
  );

  const selectedDescr = useMemo(() => {
    const r = allRows.find(
      (row) => String(row["id_agrupado"] ?? "") === selectedIdAgrupado,
    );
    return r ? String(r["descr_padrao"] ?? "") : "";
  }, [allRows, selectedIdAgrupado]);

  const inputCls =
    "bg-slate-800 border border-slate-600 rounded px-2 py-1 text-xs text-slate-200 focus:outline-none focus:border-blue-500";
  const btnCls =
    "px-3 py-1.5 rounded text-xs font-medium cursor-pointer transition-colors";
  const isBusy = updateMutation.isPending || batchMutation.isPending;

  function handleRowClick(r: Row) {
    const id = String(r["id_agrupado"] ?? "");
    if (selectedIdAgrupado === id) {
      setSelectedIdAgrupado(null);
      setNewUnidRef("");
    } else {
      setSelectedIdAgrupado(id);
      // Pre-fill with the current unid_ref of the first row of this group
      const firstRow = allRows.find(
        (row) => String(row["id_agrupado"] ?? "") === id,
      );
      setNewUnidRef(firstRow ? String(firstRow["unid_ref"] ?? "") : "");
    }
  }

  const handleFatorClick = useCallback((e: React.MouseEvent, r: Row) => {
    e.stopPropagation();
    setEditingKey(rowKey(r));
    setEditValue(String(r["fator"] ?? ""));
  }, []);

  const commitFator = useCallback(
    (r: Row) => {
      const newVal = parseFloat(editValue.replace(",", "."));
      if (!isNaN(newVal)) {
        updateMutation.mutate({
          id_agrupado: String(r["id_agrupado"]),
          id_produtos: String(r["id_produtos"]),
          fator: newVal,
        });
      }
      setEditingKey(null);
    },
    [editValue, updateMutation],
  );

  const handleFatorKeyDown = useCallback(
    (e: React.KeyboardEvent, r: Row) => {
      if (e.key === "Enter") commitFator(r);
      if (e.key === "Escape") setEditingKey(null);
    },
    [commitFator],
  );

  function applyUnidRef() {
    if (!selectedIdAgrupado || !newUnidRef) return;
    batchMutation.mutate({
      id_agrupado: selectedIdAgrupado,
      unid_ref: newUnidRef,
    });
  }

  const convRenderCell = useCallback(
    (col: string, value: unknown, row: Row): ReactNode | null => {
      if (col === "fator") {
        const k = rowKey(row);
        const isEditing = k === editingKey;
        if (isEditing) {
          return (
            <input
              type="number"
              step="any"
              value={editValue}
              onChange={(e) => setEditValue(e.target.value)}
              onBlur={() => commitFator(row)}
              onKeyDown={(e) => handleFatorKeyDown(e, row)}
              onFocus={(e) => e.target.select()}
              autoFocus
              onClick={(e) => e.stopPropagation()}
              className="w-full bg-slate-900 border border-blue-500 rounded px-1 text-white focus:outline-none"
              style={{ minWidth: "4rem", fontSize: "inherit" }}
            />
          );
        }
        return (
          <span
            onClick={(e) => handleFatorClick(e, row)}
            className="hover:bg-blue-900/40 rounded px-1 cursor-text inline-block w-full"
            title="Clique para editar"
          >
            {value != null ? Number(value).toFixed(4) : "—"}
          </span>
        );
      }
      if (col === "fator_manual" || col === "unid_ref_manual") {
        return value === true ? (
          <span className="inline-block px-1 bg-amber-700/80 text-amber-200 rounded text-xs">
            M
          </span>
        ) : (
          <span className="text-slate-700">—</span>
        );
      }
      return null;
    },
    [editingKey, editValue, commitFator, handleFatorClick, handleFatorKeyDown],
  );

  const convGetRowBg = useCallback(
    (r: Row): string | undefined => {
      const isGroupSelected =
        selectedIdAgrupado !== null &&
        String(r["id_agrupado"] ?? "") === selectedIdAgrupado;
      if (isGroupSelected) return "#1a3558";
      if (r["fator_manual"] === true) return "#2a1e00";
      return undefined;
    },
    [selectedIdAgrupado],
  );

  if (!selectedCnpj) {
    return (
      <div className="flex items-center justify-center h-full text-slate-500">
        Selecione um CNPJ.
      </div>
    );
  }

  return (
    <div className="flex flex-col h-full p-3 gap-2">
      {/* Toolbar */}
      <div className="flex gap-2 items-center flex-wrap">
        <ColumnToggle
          allColumns={DISPLAY_COLS_ORDER.filter((c) => dataColumns.includes(c))}
          orderedColumns={ordemColunas.filter((c) => DISPLAY_COLS_ORDER.includes(c))}
          hiddenColumns={hiddenCols}
          columnWidths={largurasColunas}
          onChange={(col, visible) =>
            setHiddenCols((prev) => {
              const next = new Set(prev);
              if (visible) next.delete(col);
              else next.add(col);
              return next;
            })
          }
          onOrderChange={definirOrdemColunas}
          onWidthChange={definirLarguraColuna}
          onReset={() => {
            setHiddenCols(new Set());
            redefinirPreferenciasColunas();
          }}
        />
        <button
          className={btnCls + " bg-slate-700 hover:bg-slate-600 text-slate-200"}
          onClick={() => refetch()}
          disabled={isLoading}
        >
          Recarregar
        </button>
        <label className="flex items-center gap-2 text-xs text-slate-300 cursor-pointer">
          <input
            type="checkbox"
            checked={showSingleUnit}
            onChange={(e) => { setShowSingleUnit(e.target.checked); setPage(1); }}
            className="rounded"
          />
          Mostrar itens de unidade única
        </label>
        {isLoading && (
          <span className="text-xs text-slate-400 animate-pulse">
            Carregando...
          </span>
        )}
        {isBusy && (
          <span className="text-xs text-amber-400 animate-pulse">
            Salvando...
          </span>
        )}
        {updateMutation.isError && (
          <span className="text-xs text-red-400">Erro ao salvar fator.</span>
        )}
        {batchMutation.isError && (
          <span className="text-xs text-red-400">
            Erro ao atualizar unid_ref.
          </span>
        )}
      </div>

      {/* Filters */}
      <div className="flex gap-2 flex-wrap">
        <select
          className={inputCls + " w-52"}
          value={filterIdAgrupado}
          onChange={(e) => {
            setFilterIdAgrupado(e.target.value);
            setPage(1);
            // auto-select product when chosen from dropdown
            if (e.target.value) {
              setSelectedIdAgrupado(e.target.value);
              const firstRow = allRows.find(
                (r) => String(r["id_agrupado"] ?? "") === e.target.value,
              );
              setNewUnidRef(firstRow ? String(firstRow["unid_ref"] ?? "") : "");
            } else {
              setSelectedIdAgrupado(null);
              setNewUnidRef("");
            }
          }}
        >
          <option value="">— Todos os produtos —</option>
          {uniqueIdAgrupados.map((id) => (
            <option key={id} value={id}>
              {id}
            </option>
          ))}
        </select>
        <input
          className={inputCls + " flex-1"}
          placeholder="Filtrar descr_padrao"
          value={filterDesc}
          onChange={(e) => { setFilterDesc(e.target.value); setPage(1); }}
        />
        {(filterIdAgrupado || filterDesc) && (
          <button
            className={
              btnCls + " bg-slate-700 hover:bg-slate-600 text-slate-400 text-xs"
            }
            onClick={() => {
              setFilterIdAgrupado("");
              setFilterDesc("");
              setPage(1);
            }}
          >
            Limpar filtros
          </button>
        )}
      </div>

      {/* unid_ref panel */}
      <div
        className="border border-slate-700 rounded p-2"
        style={{ background: "#0f1b33" }}
      >
        <div className="text-xs text-slate-400 mb-1">
          Alterar Unidade de Referencia do Produto Selecionado
        </div>
        <div className="flex items-center gap-2 flex-wrap">
          {selectedIdAgrupado ? (
            <span
              className="text-xs text-blue-400 font-medium truncate max-w-sm"
              title={selectedIdAgrupado}
            >
              {selectedIdAgrupado}
              {selectedDescr && (
                <span className="text-slate-400 ml-1 font-normal">
                  — {selectedDescr}
                </span>
              )}
            </span>
          ) : (
            <span className="text-xs text-slate-500">
              Nenhum produto selecionado — clique numa linha para selecionar
            </span>
          )}
          <span className="text-xs text-slate-400">→ Nova unid_ref:</span>
          <select
            className={inputCls + " w-28"}
            value={newUnidRef}
            onChange={(e) => setNewUnidRef(e.target.value)}
            disabled={!selectedIdAgrupado}
          >
            <option value="">—</option>
            {availableUnids.map((u) => (
              <option key={u} value={u}>
                {u}
              </option>
            ))}
          </select>
          <button
            className={
              btnCls +
              (selectedIdAgrupado && newUnidRef
                ? " bg-blue-600 hover:bg-blue-500 text-white"
                : " bg-slate-700 text-slate-400 cursor-not-allowed")
            }
            disabled={
              !selectedIdAgrupado || !newUnidRef || batchMutation.isPending
            }
            onClick={applyUnidRef}
          >
            Aplicar a todos os itens
          </button>
          {selectedIdAgrupado && (
            <button
              className={
                btnCls +
                " bg-slate-700 hover:bg-slate-600 text-slate-400 text-xs"
              }
              onClick={() => {
                setSelectedIdAgrupado(null);
                setNewUnidRef("");
              }}
            >
              Deselecionar
            </button>
          )}
        </div>
      </div>

      {/* Table */}
      <div className="flex-1 min-h-0">
        <DataTable
          columns={dataColumns.filter((c) => DISPLAY_COLS_ORDER.includes(c))}
          orderedColumns={displayCols}
          columnWidths={largurasColunas}
          onOrderedColumnsChange={definirOrdemColunas}
          onColumnWidthChange={definirLarguraColuna}
          rows={pagedRows}
          loading={isLoading}
          page={page}
          pageSize={ROWS_PER_PAGE}
          hiddenColumns={hiddenCols}
          onRowClick={handleRowClick}
          renderCell={convRenderCell}
          getRowBackground={convGetRowBg}
          density="compact"
        />
      </div>

      {/* Status bar */}
      <div className="flex items-center justify-between text-xs text-slate-500">
        <span>
          {filteredRows.length} de {allRows.length} registros
          {!showSingleUnit && singleUnitIds.size > 0 && (
            <span className="ml-2 text-slate-600">
              ({singleUnitIds.size} produto(s) de unidade única oculto(s))
            </span>
          )}
        </span>
        {totalPages > 1 && (
          <div className="flex items-center gap-2">
            <button
              onClick={() => setPage((p) => Math.max(1, p - 1))}
              disabled={page <= 1}
              className="px-2 py-0.5 rounded bg-slate-700 hover:bg-slate-600 disabled:opacity-40"
            >
              ‹
            </button>
            <span>
              Pág. {page}/{totalPages}
            </span>
            <button
              onClick={() => setPage((p) => Math.min(totalPages, p + 1))}
              disabled={page >= totalPages}
              className="px-2 py-0.5 rounded bg-slate-700 hover:bg-slate-600 disabled:opacity-40"
            >
              ›
            </button>
          </div>
        )}
      </div>
    </div>
  );
}
