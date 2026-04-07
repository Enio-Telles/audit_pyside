import { useEffect, useMemo, useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { pipelineApi, ressarcimentoApi } from "../../api/client";

// ⚡ Bolt Optimization: Use cached Intl.NumberFormat instance instead of Number.prototype.toLocaleString()
// This avoids repeatedly allocating locale data and parsing options on every render, improving performance.
const intlInteger = new Intl.NumberFormat("pt-BR");
import { useAppStore } from "../../store/appStore";
import { DataTable } from "../table/DataTable";
import { ColumnToggle } from "../table/ColumnToggle";
import { usePreferenciasColunas } from "../../hooks/usePreferenciasColunas";

type SubTab = "itens" | "mensal" | "conciliacao" | "validacoes";

function RessarcimentoGrid({ cnpj, subTab }: { cnpj: string; subTab: SubTab }) {
  const [page, setPage] = useState(1);
  const [search, setSearch] = useState("");
  const [hiddenCols, setHiddenCols] = useState<Set<string>>(new Set());

  const queryFn = {
    itens: () => ressarcimentoApi.itens(cnpj, page, 300),
    mensal: () => ressarcimentoApi.mensal(cnpj, page, 300),
    conciliacao: () => ressarcimentoApi.conciliacao(cnpj, page, 300),
    validacoes: () => ressarcimentoApi.validacoes(cnpj, page, 300),
  }[subTab];

  const { data, isLoading, refetch } = useQuery({
    queryKey: ["ressarcimento", subTab, cnpj, page],
    queryFn,
    enabled: !!cnpj,
    placeholderData: (prev) => prev,
  });

  const colunasDisponiveis = useMemo(
    () => data?.columns ?? [],
    [data?.columns],
  );
  const rows = useMemo(() => {
    const termo = search.toLowerCase();
    if (!termo) return data?.rows ?? [];
    return (data?.rows ?? []).filter((row) =>
      Object.values(row).some((value) =>
        String(value ?? "")
          .toLowerCase()
          .includes(termo),
      ),
    );
  }, [data?.rows, search]);

  const {
    ordemColunas,
    largurasColunas,
    definirOrdemColunas,
    definirLarguraColuna,
    redefinirPreferenciasColunas,
  } = usePreferenciasColunas(
    `ressarcimento_${subTab}_colunas_v1`,
    colunasDisponiveis,
  );

  const btnCls =
    "px-3 py-1.5 rounded text-xs font-medium cursor-pointer transition-colors";
  const inputCls =
    "bg-slate-800 border border-slate-600 rounded px-2 py-1 text-xs text-slate-200 focus:outline-none focus:border-blue-500";

  return (
    <div className="flex flex-col h-full">
      <div className="px-3 py-2 border-b border-slate-700">
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
          <input
            className={inputCls + " flex-1 min-w-64"}
            placeholder="Buscar em qualquer coluna..."
            value={search}
            onChange={(e) => setSearch(e.target.value)}
          />
          <ColumnToggle
            allColumns={colunasDisponiveis}
            orderedColumns={ordemColunas}
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
        </div>
        <div className="mt-2 text-xs text-slate-400">
          {isLoading
            ? "Carregando..."
            : `Exibindo ${intlInteger.format(rows.length)} de ${intlInteger.format(data?.total_rows ?? 0)} linhas.`}
        </div>
      </div>

      <div className="flex-1 overflow-hidden">
        <DataTable
          columns={colunasDisponiveis}
          orderedColumns={ordemColunas}
          columnWidths={largurasColunas}
          onOrderedColumnsChange={definirOrdemColunas}
          onColumnWidthChange={definirLarguraColuna}
          rows={rows}
          totalRows={data?.total_rows}
          loading={isLoading}
          page={page}
          totalPages={data?.total_pages}
          onPageChange={setPage}
          hiddenColumns={hiddenCols}
        />
      </div>
    </div>
  );
}

export function RessarcimentoTab() {
  const queryClient = useQueryClient();
  const {
    selectedCnpj,
    pipelineStatus,
    pipelineWatchCnpj,
    startPipelineMonitor,
  } = useAppStore();
  const [subTab, setSubTab] = useState<SubTab>("itens");

  const { data: resumo, isLoading: resumoLoading } = useQuery({
    queryKey: ["ressarcimento_resumo", selectedCnpj],
    queryFn: () => ressarcimentoApi.resumo(selectedCnpj!),
    enabled: !!selectedCnpj,
  });

  useEffect(() => {
    if (
      pipelineWatchCnpj === selectedCnpj &&
      pipelineStatus?.status === "done"
    ) {
      void queryClient.invalidateQueries({ queryKey: ["ressarcimento"] });
      void queryClient.invalidateQueries({
        queryKey: ["ressarcimento_resumo", selectedCnpj],
      });
    }
  }, [pipelineStatus?.status, pipelineWatchCnpj, queryClient, selectedCnpj]);

  const processMutation = useMutation({
    mutationFn: () =>
      pipelineApi.run({ cnpj: selectedCnpj!, tabelas: ["ressarcimento_st"] }),
    onSuccess: () => {
      startPipelineMonitor(selectedCnpj!, {
        status: "queued",
        progresso: [],
        erros: [],
        percentual: 0,
        etapas_concluidas: 0,
        total_etapas: 0,
        etapa_atual: "fila",
        item_atual: null,
      });
    },
  });

  if (!selectedCnpj) {
    return (
      <div className="flex items-center justify-center h-full text-slate-500">
        Selecione um CNPJ.
      </div>
    );
  }

  const subtabs: { key: SubTab; label: string }[] = [
    { key: "itens", label: "Itens" },
    { key: "mensal", label: "Mensal" },
    { key: "conciliacao", label: "Conciliação" },
    { key: "validacoes", label: "Pendências" },
  ];

  const statusBadgeCls =
    pipelineWatchCnpj === selectedCnpj && pipelineStatus
      ? pipelineStatus.status === "done"
        ? "text-emerald-300 bg-emerald-900/30 border-emerald-700"
        : pipelineStatus.status === "error"
          ? "text-rose-300 bg-rose-900/30 border-rose-700"
          : "text-amber-200 bg-amber-900/30 border-amber-700"
      : "text-slate-300 bg-slate-800 border-slate-700";

  return (
    <div className="flex flex-col h-full">
      <div className="px-3 py-3 border-b border-slate-700">
        <div className="flex items-start justify-between gap-3 flex-wrap">
          <div>
            <div className="text-sm font-semibold text-slate-100">
              Ressarcimento ST
            </div>
            <div className="text-xs text-slate-400 mt-1">
              Recalcula o ressarcimento com base em C176, agregação por
              id_agrupado, conversão para unid_ref, Fronteira e E111.
            </div>
          </div>

          <div className="flex items-center gap-2 flex-wrap">
            <span
              className={`text-xs px-2 py-1 rounded border ${statusBadgeCls}`}
            >
              {pipelineWatchCnpj === selectedCnpj && pipelineStatus
                ? `Pipeline ${pipelineStatus.status.toUpperCase()}`
                : "Pronto para processar"}
            </span>
            <button
              className="px-4 py-2 rounded text-xs font-semibold bg-blue-600 hover:bg-blue-500 text-white disabled:opacity-50"
              onClick={() => processMutation.mutate()}
              disabled={processMutation.isPending}
            >
              {processMutation.isPending
                ? "Enfileirando..."
                : "Processar Ressarcimento ST"}
            </button>
          </div>
        </div>

        <div className="grid grid-cols-2 lg:grid-cols-4 gap-2 mt-3">
          <div className="rounded border border-slate-700 bg-slate-900/50 p-3">
            <div className="text-[11px] uppercase tracking-wide text-slate-500">
              Itens
            </div>
            <div className="text-lg font-semibold text-slate-100 mt-1">
              {resumoLoading
                ? "..."
                : intlInteger.format(resumo?.qtd_itens ?? 0)}
            </div>
          </div>
          <div className="rounded border border-slate-700 bg-slate-900/50 p-3">
            <div className="text-[11px] uppercase tracking-wide text-slate-500">
              Pendências de Conversão
            </div>
            <div className="text-lg font-semibold text-amber-300 mt-1">
              {resumoLoading
                ? "..."
                : intlInteger.format(resumo?.pendencias_conversao ?? 0)}
            </div>
          </div>
          <div className="rounded border border-slate-700 bg-slate-900/50 p-3">
            <div className="text-[11px] uppercase tracking-wide text-slate-500">
              Cobertura Até 2022
            </div>
            <div className="text-lg font-semibold text-emerald-300 mt-1">
              {resumoLoading ? "..." : `${resumo?.cobertura_pre_2023 ?? 0}%`}
            </div>
          </div>
          <div className="rounded border border-slate-700 bg-slate-900/50 p-3">
            <div className="text-[11px] uppercase tracking-wide text-slate-500">
              Cobertura Pós-2022
            </div>
            <div className="text-lg font-semibold text-cyan-300 mt-1">
              {resumoLoading ? "..." : `${resumo?.cobertura_pos_2023 ?? 0}%`}
            </div>
          </div>
        </div>

        {!!resumo?.faltantes.length && (
          <div className="mt-3 rounded border border-amber-800 bg-amber-950/30 px-3 py-2 text-xs text-amber-200">
            Pré-requisitos ausentes: {resumo.faltantes.join(", ")}
          </div>
        )}
      </div>

      <div
        className="flex items-center gap-1 px-3 pt-2 border-b border-slate-700"
        style={{ background: "#0a1628" }}
      >
        {subtabs.map((tab) => (
          <button
            key={tab.key}
            onClick={() => setSubTab(tab.key)}
            className={`px-4 py-1.5 text-xs font-medium rounded-t transition-colors border-t border-l border-r ${
              subTab === tab.key
                ? "border-slate-600 text-white"
                : "border-transparent text-slate-400 hover:text-slate-200"
            }`}
            style={subTab === tab.key ? { background: "#0f1b33" } : {}}
          >
            {tab.label}
          </button>
        ))}
      </div>

      <div className="flex-1 overflow-hidden">
        <RessarcimentoGrid cnpj={selectedCnpj} subTab={subTab} />
      </div>
    </div>
  );
}
