import type { AppMode } from "../store/appStore";

interface LandingPageProps {
  onSelect: (mode: AppMode) => void;
}

export function LandingPage({ onSelect }: LandingPageProps) {
  return (
    <div
      className="flex flex-col items-center justify-center h-screen"
      style={{ background: "#0a1628" }}
    >
      {/* Logo / title */}
      <div className="mb-10 text-center">
        <div className="text-3xl font-bold text-white tracking-wide mb-1">
          Fiscal Parquet Analyzer
        </div>
        <div className="text-sm text-slate-400">Selecione o módulo de análise</div>
      </div>

      {/* Cards */}
      <div className="flex gap-6">
        {/* Audit Card */}
        <button
          onClick={() => onSelect("audit")}
          className="group flex flex-col items-center gap-4 p-8 rounded-2xl border border-slate-700 hover:border-blue-500 transition-all duration-200"
          style={{ background: "#0d1f3c", width: 280 }}
        >
          <div
            className="flex items-center justify-center rounded-full"
            style={{ width: 64, height: 64, background: "#1a3558" }}
          >
            <svg width="32" height="32" viewBox="0 0 24 24" fill="none" stroke="#60a5fa" strokeWidth="1.8">
              <path d="M9 11l3 3L22 4" strokeLinecap="round" strokeLinejoin="round" />
              <path d="M21 12v7a2 2 0 01-2 2H5a2 2 0 01-2-2V5a2 2 0 012-2h11" strokeLinecap="round" strokeLinejoin="round" />
            </svg>
          </div>
          <div className="text-center">
            <div className="text-white font-semibold text-base mb-1">Análise CNPJ</div>
            <div className="text-xs text-slate-400 leading-relaxed">
              Consulta, pipeline ETL, movimentação de estoque e cálculos fiscais para um CNPJ
            </div>
          </div>
          <span className="text-xs text-blue-400 group-hover:text-blue-300 mt-1">
            Abrir →
          </span>
        </button>

        {/* Fisconforme Card */}
        <button
          onClick={() => onSelect("fisconforme")}
          className="group flex flex-col items-center gap-4 p-8 rounded-2xl border border-slate-700 hover:border-emerald-500 transition-all duration-200"
          style={{ background: "#0d1f3c", width: 280 }}
        >
          <div
            className="flex items-center justify-center rounded-full"
            style={{ width: 64, height: 64, background: "#0d2d1f" }}
          >
            <svg width="32" height="32" viewBox="0 0 24 24" fill="none" stroke="#34d399" strokeWidth="1.8">
              <rect x="3" y="3" width="7" height="7" rx="1" strokeLinecap="round" strokeLinejoin="round" />
              <rect x="14" y="3" width="7" height="7" rx="1" strokeLinecap="round" strokeLinejoin="round" />
              <rect x="3" y="14" width="7" height="7" rx="1" strokeLinecap="round" strokeLinejoin="round" />
              <rect x="14" y="14" width="7" height="7" rx="1" strokeLinecap="round" strokeLinejoin="round" />
            </svg>
          </div>
          <div className="text-center">
            <div className="text-white font-semibold text-base mb-1">Análise Lote CNPJ</div>
            <div className="text-xs text-slate-400 leading-relaxed">
              Extração de dados cadastrais e malhas fiscais para múltiplos CNPJs com cache compartilhado
            </div>
          </div>
          <span className="text-xs text-emerald-400 group-hover:text-emerald-300 mt-1">
            Abrir →
          </span>
        </button>
      </div>
    </div>
  );
}
