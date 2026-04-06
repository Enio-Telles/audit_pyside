import { useState, useMemo } from "react";
import { useQuery } from "@tanstack/react-query";
import { estoqueApi } from "../../api/client";
import { useAppStore } from "../../store/appStore";
import { DataTable } from "../table/DataTable";

type SubTab =
  | "mov_estoque"
  | "tabela_mensal"
  | "tabela_anual"
  | "resumo"
  | "produtos"
  | "id_agrupados";

// Sub-tabs that show the produtos_final table and support full selection
const PRODUTOS_SUBS = new Set<SubTab>(["produtos", "id_agrupados"]);

function EstoqueSubTab({ cnpj, sub }: { cnpj: string; sub: SubTab }) {
  const [page, setPage] = useState(1);
  const [search, setSearch] = useState("");
  const [filterIdAgrupado, setFilterIdAgrupado] = useState("");
  const [selectedRowKeys, setSelectedRowKeys] = useState<Set<string>>(
    new Set(),
  );

  const isProdutosSub = PRODUTOS_SUBS.has(sub);

  // Para sub-tabs de produtos carrega todos de uma vez (sem paginação) para popular dropdown
  const queryFn = isProdutosSub
    ? () => estoqueApi.idAgrupados(cnpj, 1, 10000)
    : {
        mov_estoque: () => estoqueApi.movEstoque(cnpj, page),
        tabela_mensal: () => estoqueApi.tabelaMensal(cnpj, page),
        tabela_anual: () => estoqueApi.tabelaAnual(cnpj, page),
        resumo: () => estoqueApi.tabelaAnual(cnpj, page),
        produtos: () => estoqueApi.idAgrupados(cnpj, page),
        id_agrupados: () => estoqueApi.idAgrupados(cnpj, page),
      }[sub];

  const { data, isLoading, refetch } = useQuery({
    queryKey: isProdutosSub ? [sub, cnpj, "all"] : [sub, cnpj, page],
    queryFn,
    placeholderData: (prev) => prev,
  });

  const inputCls =
    "bg-slate-800 border border-slate-600 rounded px-2 py-1 text-xs text-slate-200 focus:outline-none focus:border-blue-500";
  const btnCls =
    "px-3 py-1.5 rounded text-xs font-medium cursor-pointer transition-colors";

  // Unique id_agrupado values for the dropdown (only relevant for produtos sub-tabs)
  const uniqueIdAgrupados = useMemo(() => {
    if (!isProdutosSub) return [];
    const seen = new Set<string>();
    for (const r of data?.rows ?? []) {
      const v = String(r["id_agrupado"] ?? "");
      if (v) seen.add(v);
    }
    return [...seen].sort();
  }, [data?.rows, isProdutosSub]);

  const filteredRows = useMemo(() => {
    // ⚡ Bolt Optimization: Hoist string casing outside filter loop to prevent O(N) performance degradation
    const searchLower = search ? search.toLowerCase() : "";
    return (data?.rows ?? []).filter((r) => {
      if (
        filterIdAgrupado &&
        String(r["id_agrupado"] ?? "") !== filterIdAgrupado
      )
        return false;
      if (searchLower) {
        let hit = false;
        // ⚡ Bolt Optimization: Replace Object.values().some() with for...in loop and early break to prevent O(N*C) object allocations
        for (const key in r) {
          if (
            String(r[key] ?? "")
              .toLowerCase()
              .includes(searchLower)
          ) {
            hit = true;
            break;
          }
        }
        if (!hit) return false;
      }
      return true;
    });
  }, [data?.rows, filterIdAgrupado, search]);

  const hasFilters = !!(filterIdAgrupado || search);

  function clearFilters() {
    setFilterIdAgrupado("");
    setSearch("");
  }

  function handleRowSelect(key: string, checked: boolean) {
    setSelectedRowKeys((prev) => {
      const next = new Set(prev);
      if (checked) next.add(key);
      else next.delete(key);
      return next;
    });
  }

  function handleSelectAll(checked: boolean, visibleKeys: string[]) {
    setSelectedRowKeys((prev) => {
      const next = new Set(prev);
      if (checked) visibleKeys.forEach((k) => next.add(k));
      else visibleKeys.forEach((k) => next.delete(k));
      return next;
    });
  }

  const tableTitle = {
    mov_estoque: "Tabela: mov_estoque",
    tabela_mensal: "Tabela: aba_mensal",
    tabela_anual: "Tabela: aba_anual",
    resumo: "Resumo Global",
    produtos: "Tabela: produtos_selecionados",
    id_agrupados: "Tabela: id_agrupados",
  }[sub];

  return (
    <div className="flex flex-col h-full">
      <div className="px-3 py-2 border-b border-slate-700">
        <div className="text-xs font-semibold text-slate-300 mb-2">
          {tableTitle}
        </div>
        <div className="flex gap-2 items-center flex-wrap">
          <button
            className={
              btnCls + " bg-slate-700 hover:bg-slate-600 text-slate-200"
            }
            onClick={() => refetch()}
            disabled={isLoading}
          >
            Recarregar
          </button>
          {hasFilters && (
            <button
              className={
                btnCls + " bg-slate-700 hover:bg-slate-600 text-slate-400"
              }
              onClick={clearFilters}
            >
              Limpar filtros
            </button>
          )}
          {isProdutosSub && selectedRowKeys.size > 0 && (
            <span className="text-xs text-blue-400 font-medium">
              {selectedRowKeys.size} produto(s) selecionado(s)
            </span>
          )}
          {isProdutosSub && selectedRowKeys.size > 0 && (
            <button
              className={
                btnCls +
                " bg-slate-700 hover:bg-slate-600 text-slate-400 text-xs"
              }
              onClick={() => setSelectedRowKeys(new Set())}
            >
              Limpar seleção
            </button>
          )}
          <div className="flex-1" />
          <button
            className={
              btnCls + " bg-slate-700 hover:bg-slate-600 text-slate-200"
            }
          >
            Exportar Excel
          </button>
        </div>

        {/* Filters row */}
        <div className="flex gap-2 mt-2 flex-wrap">
          {isProdutosSub ? (
            <select
              className={inputCls + " w-56"}
              value={filterIdAgrupado}
              onChange={(e) => setFilterIdAgrupado(e.target.value)}
            >
              <option value="">— Todos os produtos —</option>
              {uniqueIdAgrupados.map((id) => (
                <option key={id} value={id}>
                  {id}
                </option>
              ))}
            </select>
          ) : (
            <select className={inputCls + " w-48"} disabled>
              <option>Filtrar id_agrupado</option>
            </select>
          )}
          <input
            className={inputCls + " flex-1"}
            placeholder="Filtrar descricao..."
            value={search}
            onChange={(e) => setSearch(e.target.value)}
          />
          {sub === "tabela_mensal" && (
            <>
              <select className={inputCls + " w-24"}>
                <option>Ano: Todos</option>
              </select>
              <select className={inputCls + " w-24"}>
                <option>Mes: Todos</option>
              </select>
            </>
          )}
          {sub === "tabela_anual" && (
            <select className={inputCls + " w-24"}>
              <option>Todos</option>
            </select>
          )}
        </div>

        <div className="flex items-center gap-3 mt-1">
          <span className="text-xs text-slate-400">
            {isLoading
              ? "Carregando..."
              : `Exibindo ${filteredRows.length.toLocaleString("pt-BR")} de ${(data?.total_rows ?? 0).toLocaleString("pt-BR")} linhas.`}
          </span>
          {hasFilters && (
            <span className="text-xs text-amber-400">
              Filtros ativos:{" "}
              {[filterIdAgrupado && "id_agrupado", search && "descricao"]
                .filter(Boolean)
                .join(", ")}
            </span>
          )}
        </div>
      </div>

      <div className="flex-1 overflow-hidden">
        <DataTable
          columns={data?.columns ?? []}
          rows={filteredRows}
          totalRows={isProdutosSub ? filteredRows.length : data?.total_rows}
          loading={isLoading}
          page={isProdutosSub ? 1 : page}
          totalPages={isProdutosSub ? 1 : data?.total_pages}
          onPageChange={isProdutosSub ? undefined : setPage}
          highlightRows={sub === "mov_estoque"}
          rowKey={isProdutosSub ? "id_agrupado" : undefined}
          selectedRowKeys={isProdutosSub ? selectedRowKeys : undefined}
          onRowSelect={isProdutosSub ? handleRowSelect : undefined}
          onSelectAll={isProdutosSub ? handleSelectAll : undefined}
        />
      </div>
    </div>
  );
}

export function EstoqueTab() {
  const { selectedCnpj } = useAppStore();
  const [subTab, setSubTab] = useState<SubTab>("mov_estoque");

  const { data: movData } = useQuery({
    queryKey: ["mov_estoque_count", selectedCnpj],
    queryFn: () => estoqueApi.movEstoque(selectedCnpj!, 1, 1),
    enabled: !!selectedCnpj,
  });
  const { data: mensalData } = useQuery({
    queryKey: ["tabela_mensal_count", selectedCnpj],
    queryFn: () => estoqueApi.tabelaMensal(selectedCnpj!, 1, 1),
    enabled: !!selectedCnpj,
  });
  const { data: anualData } = useQuery({
    queryKey: ["tabela_anual_count", selectedCnpj],
    queryFn: () => estoqueApi.tabelaAnual(selectedCnpj!, 1, 1),
    enabled: !!selectedCnpj,
  });
  const { data: idAgrData } = useQuery({
    queryKey: ["id_agrupados_count", selectedCnpj],
    queryFn: () => estoqueApi.idAgrupados(selectedCnpj!, 1, 1),
    enabled: !!selectedCnpj,
  });

  if (!selectedCnpj) {
    return (
      <div className="flex items-center justify-center h-full text-slate-500">
        Selecione um CNPJ.
      </div>
    );
  }

  const subtabs: { key: SubTab; label: string; count?: number }[] = [
    {
      key: "mov_estoque",
      label: "Tabela mov_estoque",
      count: movData?.total_rows,
    },
    {
      key: "tabela_mensal",
      label: "Tabela mensal",
      count: mensalData?.total_rows,
    },
    {
      key: "tabela_anual",
      label: "Tabela anual",
      count: anualData?.total_rows,
    },
    { key: "resumo", label: "Resumo Global" },
    {
      key: "produtos",
      label: "Produtos selecionados",
      count: idAgrData?.total_rows,
    },
    {
      key: "id_agrupados",
      label: "id_agrupados",
      count: idAgrData?.total_rows,
    },
  ];

  return (
    <div className="flex flex-col h-full">
      {/* Subtab bar */}
      <div
        className="flex gap-1 px-3 pt-2 border-b border-slate-700"
        style={{ background: "#0a1628" }}
      >
        {subtabs.map((st) => (
          <button
            key={st.key}
            onClick={() => setSubTab(st.key)}
            className={`px-3 py-1.5 rounded-t text-xs font-medium transition-colors border-t border-l border-r ${
              subTab === st.key
                ? "border-slate-600 text-white"
                : "border-transparent text-slate-400 hover:text-slate-200"
            }`}
            style={subTab === st.key ? { background: "#0f1b33" } : {}}
          >
            {st.label}
            {st.count !== undefined
              ? ` (${st.count.toLocaleString("pt-BR")})`
              : ""}
          </button>
        ))}
      </div>

      <div className="flex-1 overflow-hidden">
        <EstoqueSubTab cnpj={selectedCnpj} sub={subTab} />
      </div>
    </div>
  );
}
