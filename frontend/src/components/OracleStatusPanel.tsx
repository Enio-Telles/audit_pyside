import { useCallback, useEffect, useState } from "react";
import { oracleApi } from "../api/client";
import type {
  OracleConexaoConfig,
  OracleSalvarRequest,
  OracleTestarRequest,
} from "../api/types";

type TestState = "idle" | "loading" | "ok" | "error";

interface SlotState {
  state: TestState;
  message: string;
}

interface OracleFormState {
  principal: OracleConexaoConfig;
  secundaria: OracleConexaoConfig;
}

const IDLE: SlotState = { state: "idle", message: "" };
const EMPTY_CONFIG: OracleConexaoConfig = {
  host: "",
  port: "1521",
  service: "",
  user: "",
  password: "",
  configured: false,
};

function buildPayload(form: OracleFormState): OracleSalvarRequest {
  return {
    oracle_host: form.principal.host.trim(),
    oracle_port: form.principal.port.trim() || "1521",
    oracle_service: form.principal.service.trim(),
    db_user: form.principal.user.trim(),
    db_password: form.principal.password,
    oracle_host_1: form.secundaria.host.trim(),
    oracle_port_1: form.secundaria.port.trim() || "1521",
    oracle_service_1: form.secundaria.service.trim(),
    db_user_1: form.secundaria.user.trim(),
    db_password_1: form.secundaria.password,
  };
}

function mapConfigToForm(
  config1: OracleConexaoConfig,
  config2: OracleConexaoConfig,
): OracleFormState {
  return {
    principal: { ...config1 },
    secundaria: { ...config2 },
  };
}

function toTestRequest(config: OracleConexaoConfig): OracleTestarRequest {
  return {
    host: config.host.trim(),
    port: config.port.trim() || "1521",
    service: config.service.trim(),
    user: config.user.trim(),
    password: config.password,
  };
}

function hasConnectionData(config: OracleConexaoConfig): boolean {
  return Boolean(
    config.host || config.service || config.user || config.password,
  );
}

function FormField({
  label,
  type = "text",
  value,
  placeholder,
  onChange,
}: {
  label: string;
  type?: "text" | "password";
  value: string;
  placeholder?: string;
  onChange: (value: string) => void;
}) {
  return (
    <label className="flex flex-col gap-2">
      <span className="text-[11px] font-semibold uppercase tracking-[0.18em] text-slate-400">
        {label}
      </span>
      <input
        type={type}
        value={value}
        placeholder={placeholder}
        onChange={(event) => onChange(event.target.value)}
        className="h-11 rounded-xl border border-slate-700/70 bg-slate-950/35 px-3 text-sm text-slate-100 outline-none transition focus:border-blue-400/70 focus:bg-slate-950/55"
      />
    </label>
  );
}

function ConnectionEditor({
  title,
  description,
  accentClass,
  config,
  slot,
  disabled,
  onFieldChange,
  onTest,
}: {
  title: string;
  description: string;
  accentClass: string;
  config: OracleConexaoConfig;
  slot: SlotState;
  disabled: boolean;
  onFieldChange: (field: keyof OracleConexaoConfig, value: string) => void;
  onTest: () => void;
}) {
  const statusColor: Record<TestState, string> = {
    idle: "text-slate-500",
    loading: "text-yellow-300",
    ok: "text-emerald-300",
    error: "text-red-400",
  };

  return (
    <div className="rounded-2xl border border-slate-700/60 bg-slate-950/20 p-5 shadow-[0_24px_60px_rgba(2,6,23,0.22)]">
      <div className="flex items-start justify-between gap-4">
        <div>
          <div className="flex items-center gap-2">
            <span
              className={`inline-block h-2.5 w-2.5 rounded-full ${accentClass}`}
            />
            <h3 className="text-base font-semibold text-white">{title}</h3>
          </div>
          <p className="mt-2 max-w-md text-sm leading-6 text-slate-400">
            {description}
          </p>
        </div>
        <button
          type="button"
          onClick={onTest}
          disabled={disabled}
          className="rounded-xl border border-slate-600 bg-slate-900/60 px-4 py-2 text-xs font-semibold text-slate-200 transition hover:border-blue-400 hover:text-blue-200 disabled:cursor-not-allowed disabled:opacity-50"
        >
          {slot.state === "loading" ? "Testando..." : "Testar conexao"}
        </button>
      </div>

      <div className="mt-5 grid gap-4 md:grid-cols-2">
        <FormField
          label="Host"
          value={config.host}
          placeholder="exa01-scan.sefin.ro.gov.br"
          onChange={(value) => onFieldChange("host", value)}
        />
        <FormField
          label="Porta"
          value={config.port}
          placeholder="1521"
          onChange={(value) => onFieldChange("port", value)}
        />
        <FormField
          label="Servico"
          value={config.service}
          placeholder="sefindw"
          onChange={(value) => onFieldChange("service", value)}
        />
        <FormField
          label="Usuario"
          value={config.user}
          placeholder="03002693901"
          onChange={(value) => onFieldChange("user", value)}
        />
      </div>

      <div className="mt-4 max-w-xl">
        <FormField
          label="Senha"
          type="password"
          value={config.password}
          placeholder="Digite a senha do Oracle"
          onChange={(value) => onFieldChange("password", value)}
        />
      </div>

      <div className={`mt-4 min-h-5 text-xs ${statusColor[slot.state]}`}>
        {slot.message ||
          "Preencha os dados para testar a conexao antes de salvar."}
      </div>
    </div>
  );
}

async function verificarSlot(slot: 1 | 2): Promise<SlotState> {
  try {
    const response = await fetch(`/api/oracle/verificar/${slot}`);
    const data = (await response.json()) as { ok: boolean; message: string };
    return { state: data.ok ? "ok" : "error", message: data.message };
  } catch {
    return { state: "error", message: "Sem resposta do servidor" };
  }
}

export function OracleStatusPanel() {
  const [cfg1, setCfg1] = useState<OracleConexaoConfig | null>(null);
  const [cfg2, setCfg2] = useState<OracleConexaoConfig | null>(null);
  const [form, setForm] = useState<OracleFormState>({
    principal: { ...EMPTY_CONFIG },
    secundaria: { ...EMPTY_CONFIG },
  });
  const [slot1, setSlot1] = useState<SlotState>(IDLE);
  const [slot2, setSlot2] = useState<SlotState>(IDLE);
  const [configLoading, setConfigLoading] = useState(true);
  const [showConfigModal, setShowConfigModal] = useState(false);
  const [saveState, setSaveState] = useState<SlotState>(IDLE);

  const verifyAll = useCallback(async () => {
    try {
      setConfigLoading(true);
      const data = await oracleApi.getConfig();
      setCfg1(data.conexao_1);
      setCfg2(data.conexao_2);
      setForm(mapConfigToForm(data.conexao_1, data.conexao_2));
      setConfigLoading(false);
      setSlot1({ state: "loading", message: "Verificando..." });
      setSlot2({ state: "loading", message: "Verificando..." });
      const [result1, result2] = await Promise.all([
        verificarSlot(1),
        verificarSlot(2),
      ]);
      setSlot1(result1);
      setSlot2(result2);
    } catch {
      setConfigLoading(false);
      setSlot1({ state: "error", message: "Erro ao obter configuracao" });
      setSlot2({ state: "error", message: "Erro ao obter configuracao" });
    }
  }, []);

  useEffect(() => {
    const frame = window.requestAnimationFrame(() => {
      void verifyAll();
    });

    return () => {
      window.cancelAnimationFrame(frame);
    };
  }, [verifyAll]);

  const updateField = useCallback(
    (
      connection: keyof OracleFormState,
      field: keyof OracleConexaoConfig,
      value: string,
    ) => {
      setForm((current) => {
        const nextConfig = { ...current[connection], [field]: value };
        nextConfig.configured = Boolean(
          nextConfig.user.trim() && nextConfig.password,
        );

        return {
          ...current,
          [connection]: nextConfig,
        };
      });
    },
    [],
  );

  const clearCredentials = useCallback(() => {
    setForm((current) => ({
      principal: {
        ...current.principal,
        user: "",
        password: "",
        configured: false,
      },
      secundaria: {
        ...current.secundaria,
        user: "",
        password: "",
        configured: false,
      },
    }));
    setSlot1(IDLE);
    setSlot2(IDLE);
    setSaveState({
      state: "idle",
      message: "Credenciais limpas. Salve para persistir no arquivo .env.",
    });
  }, []);

  const testCurrentConfig = useCallback(
    async (
      connection: keyof OracleFormState,
      setSlot: (slot: SlotState) => void,
    ) => {
      const payload = toTestRequest(form[connection]);
      setSlot({ state: "loading", message: "Testando conexao..." });

      if (
        !payload.host ||
        !payload.service ||
        !payload.user ||
        !payload.password
      ) {
        setSlot({
          state: "error",
          message: "Preencha host, servico, usuario e senha antes de testar.",
        });
        return;
      }

      try {
        const response = await oracleApi.testar(payload);
        setSlot({
          state: response.ok ? "ok" : "error",
          message: response.message,
        });
      } catch {
        setSlot({ state: "error", message: "Falha ao testar a conexao." });
      }
    },
    [form],
  );

  const saveConfig = useCallback(async () => {
    try {
      setSaveState({ state: "loading", message: "Salvando configuracoes..." });
      await oracleApi.salvar(buildPayload(form));
      setSaveState({
        state: "ok",
        message: "Configuracoes salvas com sucesso no .env.",
      });
      await verifyAll();
    } catch {
      setSaveState({
        state: "error",
        message: "Nao foi possivel salvar as configuracoes.",
      });
    }
  }, [form, verifyAll]);

  if (configLoading) {
    return (
      <div className="mx-auto mb-8 flex w-full max-w-6xl justify-end">
        <button
          type="button"
          disabled
          className="inline-flex items-center gap-2 rounded-xl border border-slate-700 bg-slate-900/60 px-3 py-2 text-xs font-semibold text-slate-500"
        >
          Configuracoes
        </button>
      </div>
    );
  }

  return (
    <div className="mx-auto mb-8 flex w-full max-w-6xl justify-end">
      <button
        type="button"
        onClick={() => setShowConfigModal(true)}
        className="inline-flex items-center gap-2 rounded-xl border border-slate-600 bg-slate-900/60 px-3 py-2 text-xs font-semibold text-slate-200 transition hover:border-blue-400 hover:text-white"
      >
        <svg
          width="14"
          height="14"
          viewBox="0 0 24 24"
          fill="none"
          stroke="currentColor"
          strokeWidth="1.8"
          strokeLinecap="round"
          strokeLinejoin="round"
        >
          <circle cx="12" cy="12" r="3" />
          <path d="M19.4 15a1.65 1.65 0 0 0 .33 1.82l.06.06a2 2 0 1 1-2.83 2.83l-.06-.06a1.65 1.65 0 0 0-1.82-.33 1.65 1.65 0 0 0-1 1.51V21a2 2 0 1 1-4 0v-.09a1.65 1.65 0 0 0-1-1.51 1.65 1.65 0 0 0-1.82.33l-.06.06a2 2 0 1 1-2.83-2.83l.06-.06A1.65 1.65 0 0 0 4.6 15a1.65 1.65 0 0 0-1.51-1H3a2 2 0 1 1 0-4h.09A1.65 1.65 0 0 0 4.6 9a1.65 1.65 0 0 0-.33-1.82l-.06-.06a2 2 0 1 1 2.83-2.83l.06.06A1.65 1.65 0 0 0 8.92 4.6H9a1.65 1.65 0 0 0 1-1.51V3a2 2 0 1 1 4 0v.09a1.65 1.65 0 0 0 1 1.51 1.65 1.65 0 0 0 1.82-.33l.06-.06a2 2 0 1 1 2.83 2.83l-.06.06A1.65 1.65 0 0 0 19.4 9c.36.64.56 1.36.6 2.1.04.74-.16 1.46-.6 2.1Z" />
        </svg>
        Configuracoes
      </button>

      {showConfigModal && (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-slate-950/70 px-4 py-6 backdrop-blur-sm">
          <div
            className="max-h-[90vh] w-full max-w-6xl overflow-y-auto rounded-[28px] border border-slate-700/80 px-5 py-5 shadow-[0_28px_80px_rgba(2,6,23,0.30)] backdrop-blur"
            style={{
              background:
                "linear-gradient(180deg, rgba(13,31,60,0.98) 0%, rgba(10,22,40,0.98) 100%)",
            }}
          >
            <div className="flex flex-col gap-4">
              <div className="flex flex-col gap-3 md:flex-row md:items-start md:justify-between">
                <div className="max-w-2xl">
                  <p className="text-xs font-semibold uppercase tracking-[0.22em] text-blue-200/80">
                    Configuracao do Banco de Dados
                  </p>
                  <h2 className="mt-2 text-2xl font-semibold text-white">
                    Acesso Oracle DW
                  </h2>
                  <p className="mt-2 text-sm leading-6 text-slate-400">
                    Edite as conexoes salvas no ambiente local para consulta,
                    extracao e testes do pipeline fiscal. O bloco segue a
                    referencia enviada, adaptado ao dashboard web.
                  </p>
                </div>
                <div className="flex items-start gap-3">
                  <div className="rounded-2xl border border-slate-700/60 bg-slate-950/20 px-4 py-3 text-xs text-slate-300">
                    <div className="font-semibold text-white">
                      Resumo rapido
                    </div>
                    <div className="mt-2 space-y-1">
                      <div>
                        Principal:{" "}
                        <span
                          className={
                            cfg1?.configured
                              ? "text-emerald-300"
                              : "text-slate-400"
                          }
                        >
                          {cfg1?.configured ? "Configurada" : "Pendente"}
                        </span>
                      </div>
                      <div>
                        Secundaria:{" "}
                        <span
                          className={
                            cfg2?.configured
                              ? "text-emerald-300"
                              : "text-slate-400"
                          }
                        >
                          {cfg2?.configured ? "Configurada" : "Pendente"}
                        </span>
                      </div>
                    </div>
                  </div>
                  <button
                    type="button"
                    onClick={() => setShowConfigModal(false)}
                    className="rounded-xl border border-slate-600 bg-slate-900/60 px-3 py-2 text-xs font-semibold text-slate-200 transition hover:border-slate-400 hover:text-white"
                  >
                    Fechar
                  </button>
                </div>
              </div>

              <div className="mt-2 grid gap-4 xl:grid-cols-2">
                <ConnectionEditor
                  title="Conexao principal"
                  description="Fonte principal usada no fluxo fiscal web. Ideal para o ambiente oficial do Oracle DW."
                  accentClass="bg-emerald-400"
                  config={form.principal}
                  slot={slot1}
                  disabled={
                    !hasConnectionData(form.principal) ||
                    slot1.state === "loading"
                  }
                  onFieldChange={(field, value) =>
                    updateField("principal", field, value)
                  }
                  onTest={() => {
                    void testCurrentConfig("principal", setSlot1);
                  }}
                />
                <ConnectionEditor
                  title="Conexao secundaria"
                  description="Conexao alternativa para contingencia, homologacao ou comparacao de resultados."
                  accentClass="bg-amber-300"
                  config={form.secundaria}
                  slot={slot2}
                  disabled={
                    !hasConnectionData(form.secundaria) ||
                    slot2.state === "loading"
                  }
                  onFieldChange={(field, value) =>
                    updateField("secundaria", field, value)
                  }
                  onTest={() => {
                    void testCurrentConfig("secundaria", setSlot2);
                  }}
                />
              </div>

              <div className="mt-2 flex flex-col gap-3 rounded-2xl border border-slate-700/60 bg-slate-950/20 px-4 py-4 lg:flex-row lg:items-center lg:justify-between">
                <div className="min-h-5 text-sm">
                  {saveState.message ? (
                    <span
                      className={
                        saveState.state === "ok"
                          ? "text-emerald-300"
                          : saveState.state === "error"
                            ? "text-red-400"
                            : "text-slate-400"
                      }
                    >
                      {saveState.message}
                    </span>
                  ) : (
                    <span className="text-slate-500">
                      Use os testes individuais para validar a conexao antes de
                      salvar.
                    </span>
                  )}
                </div>
                <div className="flex flex-col gap-3 sm:flex-row">
                  <button
                    type="button"
                    onClick={clearCredentials}
                    className="rounded-xl border border-slate-600 bg-slate-900/60 px-4 py-3 text-sm font-semibold text-slate-200 transition hover:border-slate-400 hover:text-white"
                  >
                    Limpar credenciais
                  </button>
                  <button
                    type="button"
                    onClick={() => {
                      void saveConfig();
                    }}
                    disabled={saveState.state === "loading"}
                    className="rounded-xl bg-gradient-to-r from-sky-600 to-blue-400 px-5 py-3 text-sm font-semibold text-slate-950 transition hover:brightness-110 disabled:cursor-not-allowed disabled:opacity-60"
                  >
                    {saveState.state === "loading"
                      ? "Salvando..."
                      : "Salvar configuracoes"}
                  </button>
                </div>
              </div>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
