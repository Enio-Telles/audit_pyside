import { create } from 'zustand';
import type { FilterItem, ParquetFile, HighlightRule } from '../api/types';

export type AppMode = 'audit' | 'fisconforme' | null;

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
}

export const useAppStore = create<AppStore>((set) => ({
  appMode: null,
  setAppMode: (mode) => set({ appMode: mode }),

  selectedCnpj: null,
  setSelectedCnpj: (cnpj) =>
    set({
      selectedCnpj: cnpj,
      selectedFile: null,
      consultaPage: 1,
      consultaFilters: [],
      consultaVisibleCols: [],
      consultaSort: null,
      consultaColumnFilters: {},
      consultaHiddenCols: new Set<string>(),
    }),

  selectedFile: null,
  setSelectedFile: (file) =>
    set({
      selectedFile: file,
      consultaPage: 1,
      consultaFilters: [],
      consultaVisibleCols: [],
      consultaSort: null,
      consultaColumnFilters: {},
      consultaHiddenCols: new Set<string>(),
    }),

  activeTab: 'consulta',
  setActiveTab: (tab) => set({ activeTab: tab }),

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
  setConsultaSort: (s) => set({ consultaSort: s }),

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
  resetConsultaHiddenCols: () =>
    set({ consultaHiddenCols: new Set<string>() }),

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
}));
