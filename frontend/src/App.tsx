import { lazy, Suspense } from "react";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { useAppStore } from "./store/appStore";
import { LeftPanel } from "./components/layout/LeftPanel";
import { LandingPage } from "./components/LandingPage";
import { useUrlSync } from "./hooks/useUrlSync";

const ConsultaTab = lazy(() =>
  import("./components/tabs/ConsultaTab").then((m) => ({ default: m.ConsultaTab }))
);
const ConsultaSqlTab = lazy(() =>
  import("./components/tabs/ConsultaSqlTab").then((m) => ({ default: m.ConsultaSqlTab }))
);
const AgregacaoTab = lazy(() =>
  import("./components/tabs/AgregacaoTab").then((m) => ({ default: m.AgregacaoTab }))
);
const ConversaoTab = lazy(() =>
  import("./components/tabs/ConversaoTab").then((m) => ({ default: m.ConversaoTab }))
);
const EstoqueTab = lazy(() =>
  import("./components/tabs/EstoqueTab").then((m) => ({ default: m.EstoqueTab }))
);
const LogsTab = lazy(() =>
  import("./components/tabs/LogsTab").then((m) => ({ default: m.LogsTab }))
);
const FisconformeTab = lazy(() =>
  import("./components/tabs/FisconformeTab").then((m) => ({ default: m.FisconformeTab }))
);
const RessarcimentoTab = lazy(() =>
  import("./components/tabs/RessarcimentoTab").then((m) => ({ default: m.RessarcimentoTab }))
);

const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      retry: 1,
      staleTime: 30_000,
      gcTime: 5 * 60_000,
      refetchOnWindowFocus: false,
    },
  },
});

const TABS = [
  { id: "consulta", label: "Consulta" },
  { id: "sql", label: "Consulta SQL" },
  { id: "agregacao", label: "Agregacao" },
  { id: "conversao", label: "Conversao" },
  { id: "ressarcimento", label: "Ressarcimento ST" },
  { id: "estoque", label: "Estoque" },
  { id: "logs", label: "Logs" },
];

function MainContent() {
  useUrlSync();
  const {
    activeTab,
    setActiveTab,
    leftPanelVisible,
    toggleLeftPanel,
    selectedFile,
    selectedCnpj,
    appMode,
    setAppMode,
  } = useAppStore();

  if (appMode === null) {
    return <LandingPage onSelect={setAppMode} />;
  }

  if (appMode === "fisconforme") {
    return (
      <div
        className="flex h-screen overflow-hidden"
        style={{ background: "#0a1628" }}
      >
        <div className="flex flex-col flex-1 overflow-hidden">
          <div
            className="flex items-center justify-between px-3 py-1 border-b border-slate-700"
            style={{ background: "#0d1f3c", minHeight: 32 }}
          >
            <span className="text-xs text-slate-400 font-semibold">
              Fisconforme — Análise em Lote
            </span>
            <button
              onClick={() => setAppMode(null)}
              className="text-xs text-blue-400 hover:text-blue-200 px-2 py-1 rounded bg-slate-800 border border-slate-700"
            >
              ← Voltar ao Início
            </button>
          </div>
          <div className="flex-1 overflow-hidden">
            <Suspense
              fallback={
                <div className="flex items-center justify-center h-full text-slate-500 text-sm">
                  Carregando...
                </div>
              }
            >
              <FisconformeTab />
            </Suspense>
          </div>
        </div>
      </div>
    );
  }

  return (
    <div
      className="flex h-screen overflow-hidden"
      style={{ background: "#0a1628" }}
    >
      {/* Left panel */}
      {leftPanelVisible && <LeftPanel />}

      {/* Main area */}
      <div className="flex flex-col flex-1 overflow-hidden">
        {/* Top bar */}
        <div
          className="flex items-center justify-between px-3 py-1 border-b border-slate-700"
          style={{ background: "#0d1f3c", minHeight: 32 }}
        >
          <div className="text-xs text-slate-400">
            {selectedCnpj && (
              <>
                CNPJ: {selectedCnpj}
                {selectedFile && <> | Arquivo: {selectedFile.name}</>}
              </>
            )}
          </div>
          <button
            onClick={toggleLeftPanel}
            className="text-xs text-blue-400 hover:text-blue-200 px-2 py-1 rounded bg-slate-800 border border-slate-700"
          >
            {leftPanelVisible
              ? "<< Ocultar Painel Lateral"
              : ">> Mostrar Painel Lateral"}
          </button>
          <button
            onClick={() => setAppMode(null)}
            className="text-xs text-slate-400 hover:text-slate-200 px-2 py-1 rounded bg-slate-800 border border-slate-700 ml-2"
          >
            ← Início
          </button>
        </div>

        {/* Tab bar */}
        <div
          className="flex items-center gap-1 px-3 pt-2 border-b border-slate-700"
          style={{ background: "#0a1628" }}
        >
          {TABS.map((t) => (
            <button
              key={t.id}
              onClick={() => setActiveTab(t.id)}
              className={`px-4 py-1.5 text-xs font-medium rounded-t transition-colors border-t border-l border-r ${
                activeTab === t.id
                  ? "border-slate-600 text-white"
                  : "border-transparent text-slate-400 hover:text-slate-200"
              }`}
              style={activeTab === t.id ? { background: "#0f1b33" } : {}}
            >
              {t.label}
            </button>
          ))}
        </div>

        {/* Tab content */}
        <div
          className="flex-1 overflow-hidden"
          style={{ background: "#0a1628" }}
        >
          <Suspense
            fallback={
              <div className="flex items-center justify-center h-full text-slate-500 text-sm">
                Carregando...
              </div>
            }
          >
            {activeTab === "consulta" && <ConsultaTab />}
            {activeTab === "sql" && <ConsultaSqlTab />}
            {activeTab === "agregacao" && <AgregacaoTab />}
            {activeTab === "conversao" && <ConversaoTab />}
            {activeTab === "ressarcimento" && <RessarcimentoTab />}
            {activeTab === "estoque" && <EstoqueTab />}
            {activeTab === "logs" && <LogsTab />}
          </Suspense>
        </div>
      </div>
    </div>
  );
}

export default function App() {
  return (
    <QueryClientProvider client={queryClient}>
      <MainContent />
    </QueryClientProvider>
  );
}
