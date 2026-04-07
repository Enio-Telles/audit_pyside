import { create } from "zustand";
import type {
  FilterItem,
  HighlightRule,
  ParquetFile,
  PipelineStatus,
} from "../api/types";

export type AppMode = "audit" | "fisconforme" | null;

interface AppStore {
  // App mode (null = landing page)
  appMode: AppMode;
  setAppMode: (mode: AppMode) => void;

  // CNPJ selection
  selectedCnpj: string | null;
  setSelectedCnpj: (cnpj: string | null) => void;

  // File selection
  selectedFile: ParquetFile | null;
  setSelectedFile: (file: ParquetFile | null) => void;

  // Active tab
  activeTab: string;
  setActiveTab: (tab: string) => void;

  // Consulta tab state — filters
  consultaFilters: FilterItem[];
  addConsultaFilter: (f: FilterItem) => void;
  removeConsultaFilter: (idx: number) => void;
  clearConsultaFilters: () => void;
  consultaVisibleCols: string[];
  setConsultaVisibleCols: (cols: string[]) => void;
  consultaPage: number;
  setConsultaPage: (p: number) => void;

  // Consulta tab state — sort
  consultaSort: { col: string; desc: boolean } | null;
  setConsultaSort: (s: { col: string; desc: boolean } | null) => void;
  // Multi-sort: all active sorts (primary first)
  consultaSortList: { col: string; desc: boolean }[];
  setConsultaSortList: (list: { col: string; desc: boolean }[]) => void;
  addConsultaSortColumn: (col: string, desc?: boolean) => void;
  removeConsultaSortColumn: (col: string) => void;

  // Consulta tab state — inline column filters (server-side)
  consultaColumnFilters: Record<string, string>;
  setConsultaColumnFilter: (col: string, val: string) => void;
  clearConsultaColumnFilters: () => void;

  // Consulta tab state — hidden columns
  consultaHiddenCols: Set<string>;
  setConsultaHiddenCol: (col: string, visible: boolean) => void;
  resetConsultaHiddenCols: () => void;

  // Consulta tab state — highlight rules
  consultaHighlightRules: HighlightRule[];
  addConsultaHighlightRule: (r: HighlightRule) => void;
  removeConsultaHighlightRule: (i: number) => void;

  // Left panel visibility
  leftPanelVisible: boolean;
  toggleLeftPanel: () => void;

  // Pipeline monitor
  pipelineWatchCnpj: string | null;
  pipelineStatus: PipelineStatus | null;
  pipelinePolling: boolean;
  startPipelineMonitor: (cnpj: string, status: PipelineStatus | null) => void;
  updatePipelineStatus: (status: PipelineStatus | null) => void;
  stopPipelineMonitor: () => void;
}

export const useAppStore = create<AppStore>((set) => ({
  appMode: null,
  setAppMode: (mode) => set({ appMode: mode }),

  selectedCnpj: null,
  setSelectedCnpj: (cnpj) => {
    if (cnpj !== null) localStorage.setItem("audit_cnpj", cnpj);
    else localStorage.removeItem("audit_cnpj");
    set({
      selectedCnpj: cnpj,
      selectedFile: null,
      consultaPage: 1,
      consultaFilters: [],
      consultaVisibleCols: [],
      consultaSort: null,
      consultaSortList: [],
      consultaColumnFilters: {},
      consultaHiddenCols: new Set<string>(),
    });
  },

  selectedFile: null,
  setSelectedFile: (file) =>
    set({
      selectedFile: file,
      consultaPage: 1,
      consultaFilters: [],
      consultaVisibleCols: [],
      consultaSort: null,
      consultaSortList: [],
      consultaColumnFilters: {},
      consultaHiddenCols: new Set<string>(),
    }),

  activeTab: ((): string => {
    try {
      return localStorage.getItem("audit_tab") ?? "consulta";
    } catch {
      return "consulta";
    }
  })(),
  setActiveTab: (tab) => {
    try { localStorage.setItem("audit_tab", tab); } catch { /* noop */ }
    set({ activeTab: tab });
  },

  consultaFilters: [],
  addConsultaFilter: (f) =>
    set((s) => ({ consultaFilters: [...s.consultaFilters, f] })),
  removeConsultaFilter: (idx) =>
    set((s) => ({
      consultaFilters: s.consultaFilters.filter((_, i) => i !== idx),
    })),
  clearConsultaFilters: () => set({ consultaFilters: [] }),

  consultaVisibleCols: [],
  setConsultaVisibleCols: (cols) => set({ consultaVisibleCols: cols }),

  consultaPage: 1,
  setConsultaPage: (p) => set({ consultaPage: p }),

  consultaSort: null,
  setConsultaSort: (s) => set((prev) => ({
    consultaSort: s,
    consultaSortList: s ? [s, ...prev.consultaSortList.filter((x) => x.col !== s.col)] : [],
  })),

  consultaSortList: [],
  setConsultaSortList: (list) => set({ consultaSortList: list, consultaSort: list[0] ?? null }),
  addConsultaSortColumn: (col, desc = false) =>
    set((s) => {
      const filtered = s.consultaSortList.filter((x) => x.col !== col);
      const next = [...filtered, { col, desc }];
      return { consultaSortList: next, consultaSort: next[0] ?? null };
    }),
  removeConsultaSortColumn: (col) =>
    set((s) => {
      const next = s.consultaSortList.filter((x) => x.col !== col);
      return { consultaSortList: next, consultaSort: next[0] ?? null };
    }),

  consultaColumnFilters: {},
  setConsultaColumnFilter: (col, val) =>
    set((s) => ({
      consultaColumnFilters: { ...s.consultaColumnFilters, [col]: val },
    })),
  clearConsultaColumnFilters: () => set({ consultaColumnFilters: {} }),

  consultaHiddenCols: new Set<string>(),
  setConsultaHiddenCol: (col, visible) =>
    set((s) => {
      const next = new Set(s.consultaHiddenCols);
      if (visible) next.delete(col);
      else next.add(col);
      return { consultaHiddenCols: next };
    }),
  resetConsultaHiddenCols: () => set({ consultaHiddenCols: new Set<string>() }),

  consultaHighlightRules: [],
  addConsultaHighlightRule: (r) =>
    set((s) => ({
      consultaHighlightRules: [...s.consultaHighlightRules, r],
    })),
  removeConsultaHighlightRule: (i) =>
    set((s) => ({
      consultaHighlightRules: s.consultaHighlightRules.filter(
        (_, idx) => idx !== i,
      ),
    })),

  leftPanelVisible: true,
  toggleLeftPanel: () =>
    set((s) => ({ leftPanelVisible: !s.leftPanelVisible })),

  pipelineWatchCnpj: null,
  pipelineStatus: null,
  pipelinePolling: false,
  startPipelineMonitor: (cnpj, status) =>
    set({
      pipelineWatchCnpj: cnpj,
      pipelineStatus: status,
      pipelinePolling: true,
    }),
  updatePipelineStatus: (status) =>
    set({
      pipelineStatus: status,
      pipelinePolling:
        status?.status === "done" || status?.status === "error" ? false : true,
    }),
  stopPipelineMonitor: () =>
    set({
      pipelinePolling: false,
    }),
}));
