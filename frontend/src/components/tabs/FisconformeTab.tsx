import { useState } from "react";
import { fisconformeApi } from "../../api/client";
import type { FisconformeConsultaResult, MalhaRecord } from "../../api/types";

// ---- Wizard steps ----
type Step = "config" | "input" | "results";

// ---- Small shared components ----
function Card({ children, className = "" }: { children: React.ReactNode; className?: string }) {
  return (
    <div
      className={`rounded-xl border border-slate-700 p-5 ${className}`}
      style={{ background: "#0d1f3c" }}
    >
      {children}
    </div>
  );
}

function Label({ children }: { children: React.ReactNode }) {
  return <label className="block text-xs text-slate-400 mb-1">{children}</label>;
}

function Input(props: React.InputHTMLAttributes<HTMLInputElement>) {
  return (
    <input
      {...props}
      className="w-full px-3 py-1.5 rounded text-xs text-white border border-slate-600 bg-slate-800 focus:outline-none focus:border-blue-500"
    />
  );
}

function PrimaryBtn({
  onClick,
  disabled,
  loading,
  children,
}: {
  onClick: () => void;
  disabled?: boolean;
  loading?: boolean;
  children: React.ReactNode;
}) {
  return (
    <button
      onClick={onClick}
      disabled={disabled || loading}
      className="px-4 py-1.5 text-xs font-medium rounded bg-blue-600 hover:bg-blue-500 text-white disabled:opacity-40 transition-colors"
    >
      {loading ? "Aguarde..." : children}
    </button>
  );
}

// ---- Step 1: DB Config ----
function DbConfigStep({ onNext }: { onNext: () => void }) {
  const [host, setHost] = useState("exa01-scan.sefin.ro.gov.br");
  const [port, setPort] = useState("1521");
  const [service, setService] = useState("sefindw");
  const [user, setUser] = useState("");
  const [pass_, setPass] = useState("");
  const [status, setStatus] = useState<{ ok: boolean; message: string } | null>(null);
  const [saving, setSaving] = useState(false);
  const [testing, setTesting] = useState(false);

  // Load existing config on mount
  useState(() => {
    fisconformeApi
      .getConfig()
      .then((cfg) => {
        if (cfg.oracle_host) setHost(cfg.oracle_host);
        if (cfg.oracle_port) setPort(cfg.oracle_port);
        if (cfg.oracle_service) setService(cfg.oracle_service);
        if (cfg.db_user) setUser(cfg.db_user);
      })
      .catch(() => {});
  });

  async function handleSave() {
    setSaving(true);
    try {
      await fisconformeApi.configurarDb({
        oracle_host: host,
        oracle_port: Number(port),
        oracle_service: service,
        db_user: user,
        db_password: pass_,
      });
      setStatus({ ok: true, message: "Configurações salvas." });
    } catch {
      setStatus({ ok: false, message: "Erro ao salvar configurações." });
    } finally {
      setSaving(false);
    }
  }

  async function handleTest() {
    setTesting(true);
    setStatus(null);
    try {
      const res = await fisconformeApi.testarConexao();
      setStatus(res);
    } catch {
      setStatus({ ok: false, message: "Erro ao testar conexão." });
    } finally {
      setTesting(false);
    }
  }

  return (
    <div className="p-6 max-w-xl mx-auto">
      <h2 className="text-sm font-semibold text-white mb-1">Configuração do Banco Oracle</h2>
      <p className="text-xs text-slate-400 mb-4">
        Credenciais armazenadas localmente em <code className="text-blue-300">C:\fisconforme\.env</code>
      </p>
      <Card className="flex flex-col gap-3">
        <div>
          <Label>Host</Label>
          <Input value={host} onChange={(e) => setHost(e.target.value)} />
        </div>
        <div className="flex gap-3">
          <div className="flex-1">
            <Label>Porta</Label>
            <Input value={port} onChange={(e) => setPort(e.target.value)} />
          </div>
          <div className="flex-1">
            <Label>Serviço</Label>
            <Input value={service} onChange={(e) => setService(e.target.value)} />
          </div>
        </div>
        <div>
          <Label>Usuário</Label>
          <Input value={user} onChange={(e) => setUser(e.target.value)} />
        </div>
        <div>
          <Label>Senha</Label>
          <Input type="password" value={pass_} onChange={(e) => setPass(e.target.value)} placeholder="••••••••" />
        </div>

        {status && (
          <div
            className={`text-xs rounded px-3 py-2 ${status.ok ? "text-emerald-300 bg-emerald-900/30" : "text-red-300 bg-red-900/30"}`}
          >
            {status.message}
          </div>
        )}

        <div className="flex gap-2 pt-1">
          <PrimaryBtn onClick={handleSave} loading={saving}>
            Salvar
          </PrimaryBtn>
          <button
            onClick={handleTest}
            disabled={testing}
            className="px-4 py-1.5 text-xs rounded border border-slate-600 text-slate-300 hover:text-white hover:border-slate-400 disabled:opacity-40 transition-colors"
          >
            {testing ? "Testando..." : "Testar conexão"}
          </button>
          <button
            onClick={onNext}
            className="ml-auto px-4 py-1.5 text-xs rounded bg-emerald-700 hover:bg-emerald-600 text-white transition-colors"
          >
            Próximo →
          </button>
        </div>
      </Card>
    </div>
  );
}

// ---- Step 2: CNPJ Input ----
function CnpjInputStep({
  onResults,
}: {
  onResults: (results: FisconformeConsultaResult[], mode: "single" | "lote") => void;
}) {
  const [mode, setMode] = useState<"single" | "lote">("lote");
  const [cnpjSingle, setCnpjSingle] = useState("");
  const [cnpjsText, setCnpjsText] = useState("");
  const [dataInicio, setDataInicio] = useState("01/2021");
  const [dataFim, setDataFim] = useState("12/2025");
  const [forcar, setForcar] = useState(false);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");

  async function handleConsultar() {
    setError("");
    setLoading(true);
    try {
      if (mode === "single") {
        const r = await fisconformeApi.consultaCadastral(cnpjSingle.trim(), dataInicio, dataFim, forcar);
        onResults([r], "single");
      } else {
        const cnpjs = cnpjsText
          .split(/[\n,;]+/)
          .map((s) => s.trim())
          .filter(Boolean);
        if (!cnpjs.length) {
          setError("Insira ao menos um CNPJ.");
          return;
        }
        const res = await fisconformeApi.consultaLote(cnpjs, dataInicio, dataFim, forcar);
        onResults(res.resultados, "lote");
      }
    } catch (exc: unknown) {
      const msg = exc instanceof Error ? exc.message : String(exc);
      setError(`Erro: ${msg}`);
    } finally {
      setLoading(false);
    }
  }

  return (
    <div className="p-6 max-w-2xl mx-auto">
      <h2 className="text-sm font-semibold text-white mb-1">Consulta de CNPJs</h2>
      <p className="text-xs text-slate-400 mb-4">
        Selecione o modo de consulta. CNPJs já consultados serão carregados do cache.
      </p>

      {/* Mode switch */}
      <div className="flex gap-2 mb-4">
        {(["single", "lote"] as const).map((m) => (
          <button
            key={m}
            onClick={() => setMode(m)}
            className={`px-3 py-1 text-xs rounded border transition-colors ${
              mode === m
                ? "border-blue-500 text-blue-300 bg-blue-900/30"
                : "border-slate-600 text-slate-400 hover:text-slate-200"
            }`}
          >
            {m === "single" ? "CNPJ único" : "Lote de CNPJs"}
          </button>
        ))}
      </div>

      <Card className="flex flex-col gap-3">
        {mode === "single" ? (
          <div>
            <Label>CNPJ</Label>
            <Input
              value={cnpjSingle}
              onChange={(e) => setCnpjSingle(e.target.value)}
              placeholder="12.345.678/0001-90"
            />
          </div>
        ) : (
          <div>
            <Label>CNPJs (um por linha, ou separados por vírgula/ponto-e-vírgula)</Label>
            <textarea
              value={cnpjsText}
              onChange={(e) => setCnpjsText(e.target.value)}
              rows={6}
              className="w-full px-3 py-1.5 rounded text-xs text-white border border-slate-600 bg-slate-800 focus:outline-none focus:border-blue-500 resize-y"
              placeholder={"12.345.678/0001-90\n98.765.432/0001-10"}
            />
          </div>
        )}

        <div className="flex gap-3">
          <div className="flex-1">
            <Label>Período início (MM/AAAA)</Label>
            <Input value={dataInicio} onChange={(e) => setDataInicio(e.target.value)} placeholder="01/2021" />
          </div>
          <div className="flex-1">
            <Label>Período fim (MM/AAAA)</Label>
            <Input value={dataFim} onChange={(e) => setDataFim(e.target.value)} placeholder="12/2025" />
          </div>
        </div>

        <label className="flex items-center gap-2 text-xs text-slate-400 cursor-pointer">
          <input
            type="checkbox"
            checked={forcar}
            onChange={(e) => setForcar(e.target.checked)}
            className="rounded"
          />
          Forçar atualização (ignorar cache)
        </label>

        {error && <div className="text-xs text-red-300 bg-red-900/30 rounded px-3 py-2">{error}</div>}

        <div className="flex justify-end">
          <PrimaryBtn onClick={handleConsultar} loading={loading}>
            Consultar
          </PrimaryBtn>
        </div>
      </Card>
    </div>
  );
}

// ---- Step 3: Results ----
function ResultsStep({
  results,
  onNova,
}: {
  results: FisconformeConsultaResult[];
  onNova: () => void;
}) {
  const [expandedCnpj, setExpandedCnpj] = useState<string | null>(results.length === 1 ? results[0].cnpj : null);

  const toggle = (cnpj: string) => setExpandedCnpj((prev) => (prev === cnpj ? null : cnpj));

  const totalMalhas = results.reduce((s, r) => s + (r.malhas?.length ?? 0), 0);
  const erros = results.filter((r) => r.error);

  return (
    <div className="p-6 overflow-y-auto" style={{ maxHeight: "calc(100vh - 100px)" }}>
      {/* Summary bar */}
      <div className="flex items-center justify-between mb-4">
        <div className="flex gap-4 text-xs text-slate-400">
          <span>
            <span className="text-white font-semibold">{results.length}</span> CNPJs consultados
          </span>
          <span>
            <span className="text-white font-semibold">{totalMalhas}</span> pendências encontradas
          </span>
          {erros.length > 0 && (
            <span className="text-red-400">
              <span className="font-semibold">{erros.length}</span> erros
            </span>
          )}
          <span className="text-slate-500">
            {results.filter((r) => r.from_cache).length} do cache
          </span>
        </div>
        <button
          onClick={onNova}
          className="px-3 py-1 text-xs rounded border border-slate-600 text-slate-300 hover:text-white transition-colors"
        >
          Nova consulta
        </button>
      </div>

      {/* Result cards */}
      <div className="flex flex-col gap-3">
        {results.map((r) => (
          <ResultCard
            key={r.cnpj}
            result={r}
            expanded={expandedCnpj === r.cnpj}
            onToggle={() => toggle(r.cnpj)}
          />
        ))}
      </div>
    </div>
  );
}

function ResultCard({
  result,
  expanded,
  onToggle,
}: {
  result: FisconformeConsultaResult;
  expanded: boolean;
  onToggle: () => void;
}) {
  const dc = result.dados_cadastrais as Record<string, unknown> | null;
  const malhas = (result.malhas ?? []) as MalhaRecord[];

  return (
    <div
      className={`rounded-xl border transition-all ${result.error ? "border-red-700" : "border-slate-700"}`}
      style={{ background: "#0d1f3c" }}
    >
      {/* Header row */}
      <button
        onClick={onToggle}
        className="w-full flex items-center justify-between px-4 py-3 text-left"
      >
        <div className="flex items-center gap-3">
          <span className="text-xs font-mono font-semibold text-white">{result.cnpj}</span>
          {dc?.RAZAO_SOCIAL !== undefined && (
            <span className="text-xs text-slate-300 truncate max-w-xs">{String(dc.RAZAO_SOCIAL ?? "")}</span>
          )}
          {result.from_cache && (
            <span className="text-xs text-slate-500 bg-slate-800 px-1.5 py-0.5 rounded">cache</span>
          )}
        </div>
        <div className="flex items-center gap-3">
          {result.error ? (
            <span className="text-xs text-red-400">{result.error}</span>
          ) : (
            <span className={`text-xs font-medium ${malhas.length > 0 ? "text-amber-400" : "text-emerald-400"}`}>
              {malhas.length > 0 ? `${malhas.length} pendência(s)` : "Sem pendências"}
            </span>
          )}
          <span className="text-slate-500 text-xs">{expanded ? "▲" : "▼"}</span>
        </div>
      </button>

      {/* Expanded content */}
      {expanded && !result.error && (
        <div className="border-t border-slate-700 px-4 py-3 flex flex-col gap-4">
          {/* Dados cadastrais */}
          {dc && (
            <div>
              <div className="text-xs text-slate-400 font-semibold mb-2">Dados Cadastrais</div>
              <div className="grid grid-cols-2 gap-x-6 gap-y-1">
                {Object.entries(dc)
                  .filter(([k]) => !["cached_at", "_FROM_PARQUET"].includes(k))
                  .map(([k, v]) => (
                    <div key={k} className="flex text-xs">
                      <span className="text-slate-500 w-40 shrink-0">{k}</span>
                      <span className="text-slate-200 truncate">{String(v ?? "")}</span>
                    </div>
                  ))}
              </div>
            </div>
          )}

          {/* Malhas */}
          {malhas.length > 0 && (
            <div>
              <div className="text-xs text-slate-400 font-semibold mb-2">
                Pendências Fiscais ({malhas.length})
              </div>
              <div className="overflow-x-auto">
                <table className="w-full text-xs border-collapse">
                  <thead>
                    <tr className="text-slate-400 border-b border-slate-700">
                      <th className="text-left py-1 pr-3">ID</th>
                      <th className="text-left py-1 pr-3">Malha</th>
                      <th className="text-left py-1 pr-3">Período</th>
                      <th className="text-left py-1 pr-3">Status</th>
                      <th className="text-left py-1">Ciência</th>
                    </tr>
                  </thead>
                  <tbody>
                    {malhas.map((m: MalhaRecord, i: number) => {
                      const v = (lo: string, up: string) => String(m[lo] ?? m[up] ?? "-");
                      const st = String(m.status_pendencia ?? m["STATUS_PENDENCIA"] ?? "");
                      return (
                        <tr key={i} className="border-b border-slate-800 hover:bg-slate-800/40">
                          <td className="py-1 pr-3 text-slate-300">{v("id_pendencia", "ID_PENDENCIA")}</td>
                          <td className="py-1 pr-3 text-slate-200">{v("titulo_malha", "TITULO_MALHA")}</td>
                          <td className="py-1 pr-3 text-slate-300">{v("periodo", "PERIODO")}</td>
                          <td className={`py-1 pr-3 ${st.includes("pendente") ? "text-amber-400" : "text-slate-300"}`}>
                            {st || "-"}
                          </td>
                          <td className="py-1 text-slate-400">{v("data_ciencia_consolidada", "DATA_CIENCIA_CONSOLIDADA")}</td>
                        </tr>
                      );
                    })}
                  </tbody>
                </table>
              </div>
            </div>
          )}

          {malhas.length === 0 && (
            <div className="text-xs text-emerald-400 bg-emerald-900/20 rounded px-3 py-2">
              Nenhuma pendência fiscal encontrada para o período selecionado.
            </div>
          )}
        </div>
      )}
    </div>
  );
}

// ---- Main Wizard ----
export function FisconformeTab() {
  const [step, setStep] = useState<Step>("config");
  const [results, setResults] = useState<FisconformeConsultaResult[]>([]);

  function handleResults(res: FisconformeConsultaResult[], _mode: "single" | "lote") {
    setResults(res);
    setStep("results");
  }

  return (
    <div className="flex flex-col h-full overflow-hidden" style={{ background: "#0a1628" }}>
      {/* Step indicator */}
      <div
        className="flex items-center gap-1 px-6 py-2 border-b border-slate-700"
        style={{ background: "#0d1f3c" }}
      >
      {(["config", "input", "results"] as Step[]).map((s) => {
          const labels: Record<Step, string> = {
            config: "1. Banco de Dados",
            input: "2. CNPJs",
            results: "3. Resultados",
          };
          const isActive = step === s;
          const isDone =
            (s === "config" && (step === "input" || step === "results")) ||
            (s === "input" && step === "results");
          return (
            <button
              key={s}
              onClick={() => (isDone || isActive ? setStep(s) : undefined)}
              className={`px-3 py-1 text-xs rounded transition-colors ${
                isActive
                  ? "bg-blue-700 text-white"
                  : isDone
                  ? "text-blue-400 hover:text-blue-200 cursor-pointer"
                  : "text-slate-600 cursor-default"
              }`}
            >
              {labels[s]}
            </button>
          );
        })}
      </div>

      {/* Step content */}
      <div className="flex-1 overflow-y-auto">
        {step === "config" && <DbConfigStep onNext={() => setStep("input")} />}
        {step === "input" && <CnpjInputStep onResults={handleResults} />}
        {step === "results" && (
          <ResultsStep results={results} onNova={() => setStep("input")} />
        )}
      </div>
    </div>
  );
}
