import { useState } from "react";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { cnpjApi, pipelineApi } from "../../api/client";
import { useAppStore } from "../../store/appStore";
import { usePipelineStatus } from "../../hooks/usePipelineStatus";

const inputCls =
  "w-full bg-slate-800 border border-slate-600 rounded px-2 py-1.5 text-sm text-slate-200 placeholder-slate-500 focus:outline-none focus:border-blue-500";
const btnCls =
  "px-3 py-1.5 rounded text-xs font-medium cursor-pointer transition-colors";
const CONSULTAS_ATOMIZADAS_PADRAO = [
  "arquivos_parquet/atomizadas/shared/01_reg0000_historico.sql",
  "arquivos_parquet/atomizadas/shared/02_reg0000_versionado.sql",
  "arquivos_parquet/atomizadas/shared/03_reg0000_ultimo_periodo.sql",
  "arquivos_parquet/atomizadas/c100/10_c100_raw.sql",
  "arquivos_parquet/atomizadas/c170/20_c170_raw.sql",
  "arquivos_parquet/atomizadas/c176/30_c176_raw.sql",
  "arquivos_parquet/atomizadas/bloco_h/40_h005_raw.sql",
  "arquivos_parquet/atomizadas/bloco_h/41_h010_raw.sql",
  "arquivos_parquet/atomizadas/bloco_h/42_h020_raw.sql",
  "arquivos_parquet/atomizadas/dimensions/50_reg0200_raw.sql",
];

export function LeftPanel() {
  const queryClient = useQueryClient();
  const {
    selectedCnpj,
    setSelectedCnpj,
    selectedFile,
    setSelectedFile,
    pipelineStatus,
    pipelinePolling,
    startPipelineMonitor,
  } = useAppStore();

  const [newCnpj, setNewCnpj] = useState("");
  const [dataLimite, setDataLimite] = useState("12/03/2026");

  const { data: cnpjs = [] } = useQuery({
    queryKey: ["cnpjs"],
    queryFn: cnpjApi.list,
  });

  const { data: files = [] } = useQuery({
    queryKey: ["files", selectedCnpj],
    queryFn: () => cnpjApi.listFiles(selectedCnpj!),
    enabled: !!selectedCnpj,
  });

  const addMutation = useMutation({
    mutationFn: (cnpj: string) => cnpjApi.add(cnpj),
    onSuccess: () => queryClient.invalidateQueries({ queryKey: ["cnpjs"] }),
  });

  const ensureSelectedCnpj = async () => {
    const typedCnpj = newCnpj.trim();
    if (typedCnpj) {
      const record = await addMutation.mutateAsync(typedCnpj);
      setSelectedCnpj(record.cnpj);
      setNewCnpj("");
      return record.cnpj;
    }
    return selectedCnpj;
  };

  const runPipeline = async (
    modo: "full" | "extract" | "process" | "atomized",
  ) => {
    const cnpj = await ensureSelectedCnpj();
    if (!cnpj) return;

    startPipelineMonitor(cnpj, {
      status: "queued",
      progresso: [],
      erros: [],
      percentual: 0,
      etapas_concluidas: 0,
      total_etapas: 0,
      etapa_atual: "fila",
      item_atual: null,
    });

    await pipelineApi.run({
      cnpj,
      data_limite:
        modo === "process" || modo === "atomized" ? undefined : dataLimite,
      incluir_extracao: modo !== "process",
      incluir_processamento: modo !== "extract" && modo !== "atomized",
      consultas: modo === "atomized" ? CONSULTAS_ATOMIZADAS_PADRAO : undefined,
      tabelas: modo === "atomized" ? ["efd_atomizacao"] : undefined,
    });
    setSelectedCnpj(cnpj);
  };

  usePipelineStatus();

  const progressValue = pipelineStatus?.percentual ?? 0;
  const etapasConcluidas = pipelineStatus?.etapas_concluidas ?? 0;
  const totalEtapas = pipelineStatus?.total_etapas ?? 0;
  const etapaAtualTexto =
    pipelineStatus?.etapa_atual === "extracao"
      ? "Extraindo tabelas brutas"
      : pipelineStatus?.etapa_atual === "processamento"
        ? "Processando tabelas analiticas"
        : pipelineStatus?.etapa_atual === "fila"
          ? "Aguardando na fila"
          : pipelineStatus?.etapa_atual === "preparacao"
            ? "Preparando execucao"
            : pipelineStatus?.etapa_atual === "concluido"
              ? "Concluido"
              : pipelineStatus?.etapa_atual === "erro"
                ? "Falha no pipeline"
                : "Pipeline";

  const pipelineStatusLabel =
    pipelineStatus?.status === "done"
      ? "Concluido"
      : pipelineStatus?.status === "error"
        ? "Falha no pipeline"
        : pipelineStatus?.item_atual
          ? `${etapaAtualTexto}: ${pipelineStatus.item_atual}`
          : etapaAtualTexto;

  const progressBarCls =
    pipelineStatus?.status === "done"
      ? "bg-emerald-500"
      : pipelineStatus?.status === "error"
        ? "bg-rose-500"
        : "bg-blue-500";

  const sectionCls = "border border-slate-700 rounded p-2 mb-3";
  const sectionTitleCls =
    "text-xs text-slate-400 font-semibold mb-2 uppercase tracking-wide";

  return (
    <div
      className="flex flex-col h-full p-2 gap-2 overflow-y-auto bg-[#0d1f3c] w-[260px] min-w-[260px]"
    >
      {/* Header */}
      <div className="text-center py-2 border-b border-slate-700">
        <div className="text-sm font-bold text-blue-300">
          Fiscal Parquet Analyzer
        </div>
      </div>

      {/* Add CNPJ */}
      <div className={sectionCls}>
        <label htmlFor="input-cpf-cnpj" className={sectionTitleCls + " block"}>
          CPF/CNPJ
        </label>
        <input
          id="input-cpf-cnpj"
          className={inputCls + " mb-2"}
          placeholder="Digite o CPF ou CNPJ..."
          value={newCnpj}
          onChange={(e) => setNewCnpj(e.target.value)}
          onKeyDown={(e) => {
            if (e.key === "Enter" && newCnpj.trim()) {
              addMutation.mutate(newCnpj.trim());
              setNewCnpj("");
            }
          }}
        />
        <div className="flex gap-1 mb-2">
          <button
            className={
              btnCls + " flex-1 bg-blue-700 hover:bg-blue-600 text-white"
            }
            onClick={() => void runPipeline("full")}
          >
            Extrair + Processar
          </button>
        </div>
        <div className="flex items-center gap-2 mb-2">
          <label htmlFor="input-data-limite" className="text-xs text-slate-400">
            Data limite EFD:
          </label>
          <input
            id="input-data-limite"
            className="bg-slate-800 border border-slate-600 rounded px-2 py-1 text-xs text-slate-200 focus:outline-none w-28"
            value={dataLimite}
            onChange={(e) => setDataLimite(e.target.value)}
          />
        </div>
        <div className="grid grid-cols-2 gap-1">
          <button
            className={
              btnCls + " bg-slate-700 hover:bg-slate-600 text-slate-200"
            }
            onClick={() => void runPipeline("extract")}
            disabled={(!selectedCnpj && !newCnpj.trim()) || pipelinePolling}
          >
            Extrair Tabelas Brutas
          </button>
          <button
            className={
              btnCls + " bg-slate-700 hover:bg-slate-600 text-slate-200"
            }
            onClick={() => void runPipeline("process")}
            disabled={(!selectedCnpj && !newCnpj.trim()) || pipelinePolling}
          >
            {pipelinePolling ? "Processando..." : "Processamento"}
          </button>
          <button
            className={
              btnCls +
              " col-span-2 bg-cyan-900/70 hover:bg-cyan-800 text-cyan-100"
            }
            onClick={() => void runPipeline("atomized")}
            disabled={(!selectedCnpj && !newCnpj.trim()) || pipelinePolling}
          >
            {pipelinePolling ? "Executando etapa..." : "EFD Atomizada"}
          </button>
          <button
            className={
              btnCls + " bg-slate-700 hover:bg-slate-600 text-slate-200"
            }
            onClick={() => {
              void queryClient.invalidateQueries({ queryKey: ["cnpjs"] });
              if (selectedCnpj) {
                void queryClient.invalidateQueries({
                  queryKey: ["files", selectedCnpj],
                });
              }
            }}
          >
            Atualizar lista
          </button>
          <button
            className={
              btnCls + " bg-slate-700 hover:bg-slate-600 text-slate-200"
            }
          >
            Abrir pasta
          </button>
        </div>
      </div>

      {/* Pipeline progress */}
      {pipelineStatus && pipelineStatus.status !== "idle" && (
        <div className="border border-slate-700 rounded p-2 text-xs">
          <div className={sectionTitleCls}>Pipeline</div>
          <div className="mb-2">
            <div className="mb-1 flex items-center justify-between text-[11px] text-slate-300">
              <span>{pipelineStatusLabel}</span>
              <span>{Math.round(progressValue)}%</span>
            </div>
            <div className="mb-1 text-[10px] text-slate-400">
              {totalEtapas > 0
                ? `${etapasConcluidas} de ${totalEtapas} etapa(s) concluida(s)`
                : "Aguardando definicao das etapas"}
            </div>
            <div className="h-2 overflow-hidden rounded-full bg-slate-800">
              <div
                className={`h-full rounded-full transition-all duration-500 ${progressBarCls} ${
                  pipelineStatus.status === "queued" ||
                  pipelineStatus.status === "running"
                    ? "animate-pulse"
                    : ""
                }`}
                style={{ width: `${progressValue}%` }}
              />
            </div>
          </div>
          <div className="overflow-auto max-h-[100px] bg-[#060e1f] p-1.5 rounded">
            {pipelineStatus.progresso.slice(-20).map((msg, i) => (
              <div key={i} className="text-slate-300 font-mono">
                {msg}
              </div>
            ))}
            {pipelineStatus.progresso.length === 0 &&
              pipelineStatus.erros.length === 0 && (
                <div className="text-slate-400 font-mono">
                  Aguardando atualizacao do pipeline...
                </div>
              )}
            {pipelineStatus.erros.map((e, i) => (
              <div key={`e${i}`} className="text-red-400 font-mono">
                {e}
              </div>
            ))}
          </div>
          <div
            className={`mt-1 text-xs font-semibold ${
              pipelineStatus.status === "done"
                ? "text-green-400"
                : pipelineStatus.status === "error"
                  ? "text-red-400"
                  : "text-yellow-400"
            }`}
          >
            {pipelineStatus.status.toUpperCase()}
          </div>
        </div>
      )}

      {/* CNPJ List */}
      <div className={sectionCls + " flex-1"}>
        <div className={sectionTitleCls}>CNPJs registrados</div>
        <div className="flex flex-col gap-0.5">
          {cnpjs.map((r) => (
            <button
              key={r.cnpj}
              onClick={() => setSelectedCnpj(r.cnpj)}
              className={`text-left px-2 py-1.5 rounded text-xs transition-colors ${
                selectedCnpj === r.cnpj
                  ? "bg-blue-700 text-white"
                  : "text-slate-300 hover:bg-slate-700"
              }`}
              title={r.razao_social ? `${r.cnpj} - ${r.razao_social}` : r.cnpj}
            >
              <div className="font-semibold">{r.cnpj}</div>
              <div className="truncate text-[11px] text-slate-400">
                {r.razao_social ?? "Razão social não disponível"}
              </div>
            </button>
          ))}
        </div>
      </div>

      {/* Files list */}
      {selectedCnpj && files.length > 0 && (
        <div className={sectionCls}>
          <div className={sectionTitleCls}>Arquivos Parquet do CNPJ</div>
          <div className="text-xs text-slate-500 mb-1 grid grid-cols-2 gap-1 font-semibold">
            <span>Arquivo</span>
            <span>Local</span>
          </div>
          <div className="overflow-y-auto max-h-[200px]">
            {files.map((f) => (
              <button
                key={f.path}
                onClick={() => setSelectedFile(f)}
                className={`w-full text-left px-1 py-0.5 rounded text-xs truncate ${
                  selectedFile?.path === f.path
                    ? "bg-blue-800 text-blue-200"
                    : "hover:bg-slate-700 text-slate-300"
                }`}
                title={f.name}
              >
                {f.name}
              </button>
            ))}
          </div>
        </div>
      )}

      {/* Status bar */}
      <div className="text-xs text-slate-500 py-1 border-t border-slate-700">
        CNPJ selecionado: {selectedCnpj ?? "—"}
      </div>
    </div>
  );
}
