export interface CNPJRecord {
  cnpj: string;
  razao_social: string | null;
  nome_fantasia: string | null;
  added_at: string;
  last_run_at: string | null;
}

export interface ParquetFile {
  name: string;
  path: string;
  size: number;
}

export interface StorageSectionInfo {
  bytes: number;
  count: number;
}

export interface StorageSummary {
  cnpj: string;
  parquet: StorageSectionInfo;
  agregacao: StorageSectionInfo;
  conversao: StorageSectionInfo;
  total_bytes: number;
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
  status: "idle" | "queued" | "running" | "done" | "error";
  progresso: string[];
  erros: string[];
  percentual?: number;
  etapas_concluidas?: number;
  total_etapas?: number;
  etapa_atual?: string | null;
  item_atual?: string | null;
}

export interface SqlFile {
  name: string;
  path: string;
}

export type RessarcimentoStatusCalculo =
  | "ok"
  | "pendente_conversao"
  | "parcial_pos_2022";

export interface RessarcimentoResumo {
  ready: boolean;
  prerequisitos: Record<string, boolean>;
  faltantes: string[];
  qtd_itens: number;
  pendencias_conversao: number;
  parciais_pos_2022: number;
  itens_com_st_calc: number;
  itens_com_fronteira: number;
  cobertura_pre_2023: number;
  cobertura_pos_2023: number;
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

export interface GerarNotificacaoRequest {
  cnpj: string;
  dsf: string;
  dsf_id?: string;
  auditor: string;
  cargo_titulo: string;
  matricula: string;
  contato: string;
  orgao_origem: string;
  output_dir?: string;
  pdf_base64?: string;
}

export interface GerarNotificacaoResponse {
  conteudo: string;
  nome_arquivo: string;
  salvo_em?: string;
}

export interface AuditorConfig {
  auditor: string;
  cargo_titulo: string;
  matricula: string;
  contato: string;
  orgao_origem: string;
}

export interface FisconformeDsfSummary {
  id: string;
  dsf: string;
  referencia: string;
  auditor: string;
  cargo_titulo: string;
  orgao_origem: string;
  output_dir: string;
  cnpjs: string[];
  cnpjs_count: number;
  data_inicio: string;
  data_fim: string;
  updated_at: string;
  created_at: string;
  pdf_file_name: string;
  pdf_disponivel: boolean;
}

export interface FisconformeDsfRecord extends FisconformeDsfSummary {
  matricula: string;
  contato: string;
  forcar_atualizacao: boolean;
}

export interface FisconformeDsfRequest {
  id?: string;
  dsf: string;
  referencia: string;
  cnpjs: string[];
  data_inicio: string;
  data_fim: string;
  forcar_atualizacao: boolean;
  auditor: string;
  cargo_titulo: string;
  matricula: string;
  contato: string;
  orgao_origem: string;
  output_dir: string;
  pdf_file_name?: string;
  pdf_base64?: string;
}

export interface FisconformeConsultaResult {
  cnpj: string;
  dados_cadastrais: Record<string, unknown> | null;
  malhas: MalhaRecord[];
  from_cache: boolean;
  error?: string;
}

export const FILTER_OPERATORS = [
  "contem",
  "igual",
  "comeca_com",
  "termina_com",
  "maior",
  "maior_igual",
  "menor",
  "menor_igual",
  "e_nulo",
  "nao_e_nulo",
] as const;

// ---- Oracle types ----
export interface OracleConexaoConfig {
  host: string;
  port: string;
  service: string;
  user: string;
  password: string;
  configured: boolean;
}

export interface OracleSalvarRequest {
  oracle_host: string;
  oracle_port: string;
  oracle_service: string;
  db_user: string;
  db_password: string;
  oracle_host_1: string;
  oracle_port_1: string;
  oracle_service_1: string;
  db_user_1: string;
  db_password_1: string;
}

export interface OracleConfigResponse {
  conexao_1: OracleConexaoConfig;
  conexao_2: OracleConexaoConfig;
}

export interface OracleTestarRequest {
  host: string;
  port: string;
  service: string;
  user: string;
  password: string;
}

export interface OracleTestarResponse {
  ok: boolean;
  message: string;
  tempo_ms: number;
}

export type FilterOperator = (typeof FILTER_OPERATORS)[number];

export type HighlightRuleOperator =
  | "igual"
  | "contem"
  | "maior"
  | "menor"
  | "e_nulo"
  | "nao_e_nulo";

export interface HighlightRule {
  type: "row" | "column";
  column: string;
  operator: HighlightRuleOperator;
  /** Empty value = unconditional (for column type) */
  value: string;
  color: string;
  label?: string;
}
