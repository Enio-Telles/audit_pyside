import type { AppMode } from "../store/appStore";
import { OracleStatusPanel } from "./OracleStatusPanel";

interface LandingPageProps {
  onSelect: (mode: AppMode) => void;
}

export function LandingPage({ onSelect }: LandingPageProps) {
  return (
    <div
      className="flex min-h-screen flex-col items-center justify-center px-4 py-10"
      style={{ background: "#0a1628" }}
    >
      <div className="mb-8 text-center">
        <div className="mb-1 text-3xl font-bold tracking-wide text-white">
          Fiscal Parquet Analyzer
        </div>
        <div className="text-sm text-slate-400">
          Selecione o modulo de analise
        </div>
      </div>

      <OracleStatusPanel />

      <div className="flex flex-wrap justify-center gap-6">
        <button
          onClick={() => onSelect("audit")}
          className="group flex flex-col items-center gap-4 rounded-2xl border border-slate-700 p-8 transition-all duration-200 hover:border-blue-500"
          style={{ background: "#0d1f3c", width: 280 }}
        >
          <div
            className="flex items-center justify-center rounded-full"
            style={{ width: 64, height: 64, background: "#1a3558" }}
          >
            <svg
              width="32"
              height="32"
              viewBox="0 0 24 24"
              fill="none"
              stroke="#60a5fa"
              strokeWidth="1.8"
            >
              <path
                d="M9 11l3 3L22 4"
                strokeLinecap="round"
                strokeLinejoin="round"
              />
              <path
                d="M21 12v7a2 2 0 01-2 2H5a2 2 0 01-2-2V5a2 2 0 012-2h11"
                strokeLinecap="round"
                strokeLinejoin="round"
              />
            </svg>
          </div>
          <div className="text-center">
            <div className="mb-1 text-base font-semibold text-white">
              Analise CNPJ
            </div>
            <div className="text-xs leading-relaxed text-slate-400">
              Consulta, pipeline ETL, movimentacao de estoque e calculos fiscais
              para um CNPJ
            </div>
          </div>
          <span className="mt-1 text-xs text-blue-400 group-hover:text-blue-300">
            Abrir -&gt;
          </span>
        </button>

        <button
          onClick={() => onSelect("fisconforme")}
          className="group flex flex-col items-center gap-4 rounded-2xl border border-slate-700 p-8 transition-all duration-200 hover:border-emerald-500"
          style={{ background: "#0d1f3c", width: 280 }}
        >
          <div
            className="flex items-center justify-center rounded-full"
            style={{ width: 64, height: 64, background: "#0d2d1f" }}
          >
            <svg
              width="32"
              height="32"
              viewBox="0 0 24 24"
              fill="none"
              stroke="#34d399"
              strokeWidth="1.8"
            >
              <rect
                x="3"
                y="3"
                width="7"
                height="7"
                rx="1"
                strokeLinecap="round"
                strokeLinejoin="round"
              />
              <rect
                x="14"
                y="3"
                width="7"
                height="7"
                rx="1"
                strokeLinecap="round"
                strokeLinejoin="round"
              />
              <rect
                x="3"
                y="14"
                width="7"
                height="7"
                rx="1"
                strokeLinecap="round"
                strokeLinejoin="round"
              />
              <rect
                x="14"
                y="14"
                width="7"
                height="7"
                rx="1"
                strokeLinecap="round"
                strokeLinejoin="round"
              />
            </svg>
          </div>
          <div className="text-center">
            <div className="mb-1 text-base font-semibold text-white">
              Analise Lote CNPJ
            </div>
            <div className="text-xs leading-relaxed text-slate-400">
              Extracao de dados cadastrais e malhas fiscais para multiplos CNPJs
              com cache compartilhado
            </div>
          </div>
          <span className="mt-1 text-xs text-emerald-400 group-hover:text-emerald-300">
            Abrir -&gt;
          </span>
        </button>
      </div>
    </div>
  );
}
