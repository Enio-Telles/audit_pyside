import { useState, useMemo } from "react";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import { aggregationApi } from "../../api/client";
import { useAppStore } from "../../store/appStore";
import { DataTable } from "../table/DataTable";

// ---------------------------------------------------------------------------
// Modal de confirmação de agregação
// ---------------------------------------------------------------------------
interface MergeModalProps {
  cnpj: string;
  selected: Map<string, Record<string, unknown>>;
  onClose: () => void;
  onSuccess: () => void;
}

function MergeModal({ cnpj, selected, onClose, onSuccess }: MergeModalProps) {
  const ids = Array.from(selected.keys());
  const [destino, setDestino] = useState(ids[0] ?? "");
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");

  const idsOrigem = ids.filter((id) => id !== destino);

  async function handleConfirm() {
    if (!destino || idsOrigem.length === 0) return;
    setLoading(true);
    setError("");
    try {
      await aggregationApi.merge(cnpj, destino, idsOrigem);
      onSuccess();
    } catch (e: unknown) {
      const msg = e instanceof Error ? e.message : String(e);
      setError(msg || "Erro ao agregar.");
    } finally {
      setLoading(false);
    }
  }

  const descricaoDestino = String(
    selected.get(destino)?.["descr_padrao"] ?? "",
  );

  return (
    <div
      className="fixed inset-0 z-50 flex items-center justify-center"
      style={{ background: "rgba(0,0,0,0.7)" }}
    >
      <div
        className="rounded-lg border border-slate-600 p-5 w-[520px] max-w-full flex flex-col gap-4"
        style={{ background: "#0f1b33" }}
      >
        <h3 className="text-sm font-semibold text-slate-100">
          Agregar {ids.length} produtos selecionados
        </h3>

        <div className="text-xs text-slate-400 leading-relaxed">
          Escolha o{" "}
          <span className="text-blue-400 font-semibold">ID de destino</span> —
          todas as demais ocorrências serão substituídas por ele na base
          inteira.
        </div>

        {/* Seletor do destino */}
        <div className="flex flex-col gap-1">
          <label className="text-xs text-slate-400">
            ID de destino (canônico)
          </label>
          <select
            value={destino}
            onChange={(e) => setDestino(e.target.value)}
            className="bg-slate-800 border border-slate-600 rounded px-2 py-1.5 text-xs text-slate-100 focus:outline-none focus:border-blue-500"
          >
            {ids.map((id) => (
              <option key={id} value={id}>
                {id} — {String(selected.get(id)?.["descr_padrao"] ?? "")}
              </option>
            ))}
          </select>
          {descricaoDestino && (
            <span className="text-xs text-green-400 mt-0.5">
              ↑ Descrição que será mantida: {descricaoDestino}
            </span>
          )}
        </div>

        {/* IDs de origem */}
        <div className="flex flex-col gap-1">
          <span className="text-xs text-slate-400">
            IDs que serão substituídos ({idsOrigem.length}):
          </span>
          <div className="flex flex-wrap gap-1 max-h-28 overflow-y-auto">
            {idsOrigem.map((id) => (
              <span
                key={id}
                className="px-2 py-0.5 rounded text-xs bg-slate-700 text-red-300 border border-slate-600"
                title={String(selected.get(id)?.["descr_padrao"] ?? "")}
              >
                {id}
              </span>
            ))}
            {idsOrigem.length === 0 && (
              <span className="text-xs text-slate-500 italic">
                Nenhum — selecione ao menos 2 IDs distintos.
              </span>
            )}
          </div>
        </div>

        {error && (
          <div className="text-xs text-red-400 border border-red-800 rounded px-2 py-1">
            {error}
          </div>
        )}

        <div className="flex justify-end gap-2 mt-1">
          <button
            onClick={onClose}
            disabled={loading}
            className="px-4 py-1.5 rounded text-xs bg-slate-700 hover:bg-slate-600 text-slate-200 cursor-pointer disabled:opacity-50"
          >
            Cancelar
          </button>
          <button
            onClick={handleConfirm}
            disabled={loading || idsOrigem.length === 0}
            className="px-4 py-1.5 rounded text-xs bg-blue-600 hover:bg-blue-500 text-white font-semibold cursor-pointer disabled:opacity-50"
          >
            {loading ? "Agregando..." : `Confirmar (${ids.length} IDs)`}
          </button>
        </div>
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Tab principal
// ---------------------------------------------------------------------------
export function AgregacaoTab() {
  const { selectedCnpj } = useAppStore();
  const queryClient = useQueryClient();
  const [page, setPage] = useState(1);
  const [searchDesc, setSearchDesc] = useState("");
  const [searchNcm, setSearchNcm] = useState("");
  const [searchCest, setSearchCest] = useState("");

  // Mapa id_agrupado → linha completa (persiste entre páginas)
  const [selectedRows, setSelectedRows] = useState<
    Map<string, Record<string, unknown>>
  >(new Map());
  const [showMergeModal, setShowMergeModal] = useState(false);
  const [mergeSuccess, setMergeSuccess] = useState("");

  const { data, isLoading } = useQuery({
    queryKey: ["tabela_agrupada", selectedCnpj, page],
    queryFn: () => aggregationApi.tabelaAgrupada(selectedCnpj!, page, 300),
    enabled: !!selectedCnpj,
    placeholderData: (prev) => prev,
  });

  const inputCls =
    "bg-slate-800 border border-slate-600 rounded px-2 py-1 text-xs text-slate-200 focus:outline-none focus:border-blue-500";
  const btnCls =
    "px-3 py-1.5 rounded text-xs font-medium cursor-pointer transition-colors";

  // ⚡ Bolt Optimization: Wrap filtering in useMemo and hoist string casing outside filter loop to prevent O(N) performance degradation
  const rows = useMemo(() => {
    const sDesc = searchDesc ? searchDesc.toLowerCase() : "";
    const sNcm = searchNcm ? searchNcm.toLowerCase() : "";
    const sCest = searchCest ? searchCest.toLowerCase() : "";

    return (data?.rows ?? []).filter((r) => {
      const desc = String(r["descr_padrao"] ?? "").toLowerCase();
      const ncm = String(r["ncm_padrao"] ?? "").toLowerCase();
      const cest = String(r["cest_padrao"] ?? "").toLowerCase();
      return (
        (!sDesc || desc.includes(sDesc)) &&
        (!sNcm || ncm.includes(sNcm)) &&
        (!sCest || cest.includes(sCest))
      );
    });
  }, [data?.rows, searchDesc, searchNcm, searchCest]);

  const selectedKeys = new Set(selectedRows.keys());
  const selCount = selectedRows.size;

  function handleRowSelect(key: string, checked: boolean) {
    setSelectedRows((prev) => {
      const next = new Map(prev);
      if (checked) {
        const row = rows.find((r) => String(r["id_agrupado"] ?? "") === key);
        if (row) next.set(key, row);
      } else {
        next.delete(key);
      }
      return next;
    });
    setMergeSuccess("");
  }

  function handleSelectAll(checked: boolean, visibleKeys: string[]) {
    setSelectedRows((prev) => {
      const next = new Map(prev);
      if (checked) {
        for (const key of visibleKeys) {
          if (!next.has(key)) {
            const row = rows.find(
              (r) => String(r["id_agrupado"] ?? "") === key,
            );
            if (row) next.set(key, row);
          }
        }
      } else {
        for (const key of visibleKeys) next.delete(key);
      }
      return next;
    });
    setMergeSuccess("");
  }

  function handleMergeSuccess() {
    setShowMergeModal(false);
    setSelectedRows(new Map());
    setMergeSuccess("Agregação realizada com sucesso! Tabela atualizada.");
    queryClient.invalidateQueries({
      queryKey: ["tabela_agrupada", selectedCnpj],
    });
  }

  function handleClearSelection() {
    setSelectedRows(new Map());
    setMergeSuccess("");
  }

  if (!selectedCnpj) {
    return (
      <div className="flex items-center justify-center h-full text-slate-500">
        Selecione um CNPJ.
      </div>
    );
  }

  // Linhas selecionadas para exibição (listas as colunas mais relevantes)
  const selectedList = Array.from(selectedRows.entries());
  const displayCols = [
    "id_agrupado",
    "descr_padrao",
    "ncm_padrao",
    "cest_padrao",
  ];

  return (
    <div className="flex flex-col h-full p-3 gap-2">
      {showMergeModal && selectedCnpj && (
        <MergeModal
          cnpj={selectedCnpj}
          selected={selectedRows}
          onClose={() => setShowMergeModal(false)}
          onSuccess={handleMergeSuccess}
        />
      )}

      {/* Tabela Agrupada */}
      <div
        className="border border-slate-700 rounded p-2"
        style={{
          background: "#0f1b33",
          flex: "0 0 auto",
          maxHeight: "55%",
          display: "flex",
          flexDirection: "column",
        }}
      >
        <div className="flex items-center justify-between mb-2 flex-wrap gap-2">
          <span className="text-xs font-semibold text-slate-300">
            Tabela Agrupada
            {selCount > 0 && (
              <span className="ml-2 text-blue-400">
                ({selCount} selecionado{selCount !== 1 ? "s" : ""})
              </span>
            )}
          </span>
          <div className="flex gap-2 flex-wrap">
            {selCount > 0 && (
              <button
                onClick={handleClearSelection}
                className={
                  btnCls + " bg-slate-700 hover:bg-slate-600 text-slate-300"
                }
              >
                Limpar seleção
              </button>
            )}
            <button
              onClick={() =>
                selCount >= 2 ? setShowMergeModal(true) : undefined
              }
              disabled={selCount < 2}
              className={
                btnCls +
                (selCount >= 2
                  ? " bg-green-700 hover:bg-green-600 text-white"
                  : " bg-slate-700 text-slate-500 cursor-not-allowed")
              }
              title={
                selCount < 2
                  ? "Selecione ao menos 2 linhas para agregar"
                  : `Agregar ${selCount} produtos selecionados`
              }
            >
              Agregar seleção ({selCount})
            </button>
            <button
              onClick={() =>
                queryClient.invalidateQueries({
                  queryKey: ["tabela_agrupada", selectedCnpj],
                })
              }
              className={btnCls + " bg-blue-600 hover:bg-blue-500 text-white"}
            >
              Reprocessar
            </button>
          </div>
        </div>

        {mergeSuccess && (
          <div className="text-xs text-green-400 border border-green-800 rounded px-2 py-1 mb-2">
            {mergeSuccess}
          </div>
        )}

        {/* Filtros */}
        <div className="flex gap-2 mb-2 flex-wrap">
          <input
            className={inputCls + " w-44"}
            placeholder="Filtrar Descrição..."
            value={searchDesc}
            onChange={(e) => {
              setSearchDesc(e.target.value);
              setPage(1);
            }}
          />
          <input
            className={inputCls + " w-28"}
            placeholder="Filtrar NCM"
            value={searchNcm}
            onChange={(e) => {
              setSearchNcm(e.target.value);
              setPage(1);
            }}
          />
          <input
            className={inputCls + " w-28"}
            placeholder="Filtrar CEST"
            value={searchCest}
            onChange={(e) => {
              setSearchCest(e.target.value);
              setPage(1);
            }}
          />
        </div>

        <div className="overflow-hidden border border-slate-700 rounded flex-1 min-h-0">
          <DataTable
            columns={data?.columns ?? []}
            rows={rows}
            totalRows={data?.total_rows}
            loading={isLoading}
            page={page}
            totalPages={data?.total_pages}
            onPageChange={setPage}
            rowKey="id_agrupado"
            selectedRowKeys={selectedKeys}
            onRowSelect={handleRowSelect}
            onSelectAll={handleSelectAll}
          />
        </div>
      </div>

      {/* Linhas Selecionadas para Agregação */}
      <div
        className="border border-slate-700 rounded p-2 flex-1 min-h-0 flex flex-col"
        style={{ background: "#0f1b33" }}
      >
        <div className="flex items-center justify-between mb-2">
          <span className="text-xs font-semibold text-slate-300">
            Seleção para Agregar{selCount > 0 ? ` (${selCount})` : ""}
          </span>
          {selCount >= 2 && (
            <button
              onClick={() => setShowMergeModal(true)}
              className={btnCls + " bg-green-700 hover:bg-green-600 text-white"}
            >
              Agregar {selCount} produtos →
            </button>
          )}
        </div>

        {selCount === 0 ? (
          <div className="text-xs text-slate-500 italic">
            Clique nas linhas da tabela acima (ou use os checkboxes) para
            selecioná-las para agregação.
          </div>
        ) : (
          <div className="overflow-auto flex-1">
            <table className="w-full border-collapse text-xs">
              <thead className="sticky top-0" style={{ background: "#1e2d4a" }}>
                <tr>
                  {displayCols.map((col) => (
                    <th
                      key={col}
                      className="px-2 py-1.5 text-left text-slate-300 font-semibold border-b border-slate-700"
                    >
                      {col}
                    </th>
                  ))}
                  <th className="px-2 py-1.5 border-b border-slate-700 w-8"></th>
                </tr>
              </thead>
              <tbody>
                {selectedList.map(([key, row], idx) => (
                  <tr
                    key={key}
                    style={{
                      background: idx % 2 === 0 ? "#0f1b33" : "#0a1628",
                    }}
                  >
                    {displayCols.map((col) => (
                      <td
                        key={col}
                        className="px-2 py-1.5 border-b border-slate-800 truncate max-w-xs"
                        title={String(row[col] ?? "")}
                      >
                        {String(row[col] ?? "")}
                      </td>
                    ))}
                    <td className="px-2 py-1.5 border-b border-slate-800 text-center">
                      <button
                        onClick={() => handleRowSelect(key, false)}
                        className="text-red-400 hover:text-red-300 text-xs leading-none"
                        title="Remover da seleção"
                      >
                        ✕
                      </button>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </div>
    </div>
  );
}
