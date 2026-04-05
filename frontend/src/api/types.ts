export interface CNPJRecord {
  cnpj: string;
  added_at: string;
  last_run_at: string | null;
}

export interface ParquetFile {
  name: string;
  path: string;
  size: number;
}

export interface FilterItem {
  column: string;
  operator: string;
  value: string;
}

export interface PageResult {
  total_rows: number;
  page: number;
  page_size: number;
  total_pages: number;
  columns: string[];
  all_columns: string[];
  rows: Record<string, unknown>[];
}

export interface PipelineStatus {
  status: 'idle' | 'queued' | 'running' | 'done' | 'error';
  progresso: string[];
  erros: string[];
}

export interface SqlFile {
  name: string;
  path: string;
}

// ---- Fisconforme types ----
export interface DadosCadastrais {
  cnpj: string;
  razao_social: string;
  municipio: string;
  uf: string;
  situacao: string;
  regime: string;
  cached_at?: string;
  [key: string]: unknown;
}

export interface MalhaRecord {
  id_pendencia?: string | number;
  id_notificacao?: string | number | null;
  malhas_id?: number;
  titulo_malha?: string;
  periodo?: string;
  status_pendencia?: string;
  status_notificacao?: string | null;
  data_ciencia_consolidada?: string | null;
  // Oracle returns uppercase column names
  [key: string]: unknown;
}

export interface FisconformeCacheStats {
  total_cnpjs_cached: number;
  cnpjs: string[];
}

export interface FisconformeConsultaResult {
  cnpj: string;
  dados_cadastrais: Record<string, unknown> | null;
  malhas: MalhaRecord[];
  from_cache: boolean;
  error?: string;
}

export const FILTER_OPERATORS = [
  'contem',
  'igual',
  'comeca_com',
  'termina_com',
  'maior',
  'maior_igual',
  'menor',
  'menor_igual',
  'e_nulo',
  'nao_e_nulo',
] as const;

export type FilterOperator = (typeof FILTER_OPERATORS)[number];

export type HighlightRuleOperator =
  | 'igual'
  | 'contem'
  | 'maior'
  | 'menor'
  | 'e_nulo'
  | 'nao_e_nulo';

export interface HighlightRule {
  type: 'row' | 'column';
  column: string;
  operator: HighlightRuleOperator;
  /** Empty value = unconditional (for column type) */
  value: string;
  color: string;
  label?: string;
}
