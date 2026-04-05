import { useState, useEffect } from "react";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { cnpjApi, pipelineApi } from "../../api/client";
import { useAppStore } from "../../store/appStore";
import type { PipelineStatus } from "../../api/types";

const inputCls =
  "w-full bg-slate-800 border border-slate-600 rounded px-2 py-1.5 text-sm text-slate-200 placeholder-slate-500 focus:outline-none focus:border-blue-500";
const btnCls =
  "px-3 py-1.5 rounded text-xs font-medium cursor-pointer transition-colors";

export function LeftPanel() {
  const queryClient = useQueryClient();
  const { selectedCnpj, setSelectedCnpj, selectedFile, setSelectedFile } =
    useAppStore();

  const [newCnpj, setNewCnpj] = useState("");
  const [dataLimite, setDataLimite] = useState("12/03/2026");
  const [pipelineStatus, setPipelineStatus] = useState<PipelineStatus | null>(
    null,
  );
  const [polling, setPolling] = useState(false);

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

  const runPipeline = async () => {
    if (!selectedCnpj) return;
    await pipelineApi.run(selectedCnpj, [], dataLimite);
    setPolling(true);
  };

  useEffect(() => {
    if (!polling || !selectedCnpj) return;
    const id = setInterval(async () => {
      const s = await pipelineApi.status(selectedCnpj);
      setPipelineStatus(s);
      if (s.status === "done" || s.status === "error") {
        setPolling(false);
        queryClient.invalidateQueries({ queryKey: ["files", selectedCnpj] });
      }
    }, 1500);
    return () => clearInterval(id);
  }, [polling, selectedCnpj, queryClient]);

  const sectionCls = "border border-slate-700 rounded p-2 mb-3";
  const sectionTitleCls =
    "text-xs text-slate-400 font-semibold mb-2 uppercase tracking-wide";

  return (
    <div
      className="flex flex-col h-full p-2 gap-2 overflow-y-auto"
      style={{ background: "#0d1f3c", width: 260, minWidth: 260 }}
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
            onClick={() => {
              if (newCnpj.trim()) {
                addMutation.mutate(newCnpj.trim());
                setNewCnpj("");
              }
            }}
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
          >
            Extrair Tabelas Brutas
          </button>
          <button
            className={
              btnCls + " bg-slate-700 hover:bg-slate-600 text-slate-200"
            }
            onClick={runPipeline}
            disabled={!selectedCnpj || polling}
          >
            {polling ? "Processando..." : "Processamento"}
          </button>
          <button
            className={
              btnCls + " bg-slate-700 hover:bg-slate-600 text-slate-200"
            }
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
      {pipelineStatus && pipelineStatus.progresso.length > 0 && (
        <div className="border border-slate-700 rounded p-2 text-xs">
          <div className={sectionTitleCls}>Pipeline</div>
          <div
            className="overflow-auto"
            style={{
              maxHeight: 100,
              background: "#060e1f",
              padding: 6,
              borderRadius: 4,
            }}
          >
            {pipelineStatus.progresso.slice(-20).map((msg, i) => (
              <div key={i} className="text-slate-300 font-mono">
                {msg}
              </div>
            ))}
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
            >
              {r.cnpj}
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
          <div className="overflow-y-auto" style={{ maxHeight: 200 }}>
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
