import axios from "axios";
import type {
  CNPJRecord,
  ParquetFile,
  PageResult,
  FilterItem,
  PipelineStatus,
  SqlFile,
  FisconformeConsultaResult,
  FisconformeCacheStats,
  GerarNotificacaoRequest,
  GerarNotificacaoResponse,
  AuditorConfig,
  FisconformeDsfRecord,
  FisconformeDsfRequest,
  FisconformeDsfSummary,
  OracleConfigResponse,
  OracleSalvarRequest,
  OracleTestarRequest,
  OracleTestarResponse,
} from "./types";

const api = axios.create({ baseURL: "/api" });

// ---- CNPJ ----
export const cnpjApi = {
  list: () => api.get<CNPJRecord[]>("/cnpj").then((r) => r.data),
  add: (cnpj: string) =>
    api.post<CNPJRecord>("/cnpj", { cnpj }).then((r) => r.data),
  remove: (cnpj: string) => api.delete(`/cnpj/${cnpj}`).then((r) => r.data),
  listFiles: (cnpj: string) =>
    api.get<ParquetFile[]>(`/cnpj/${cnpj}/files`).then((r) => r.data),
  getSchema: (cnpj: string, path: string) =>
    api
      .get<{ columns: string[] }>(`/cnpj/${cnpj}/schema`, { params: { path } })
      .then((r) => r.data),
};

// ---- Parquet Query ----
export const parquetApi = {
  query: (payload: {
    path: string;
    filters: FilterItem[];
    visible_columns: string[];
    page: number;
    page_size: number;
    sort_by?: string;
    sort_desc?: boolean;
  }) => api.post<PageResult>("/parquet/query", payload).then((r) => r.data),
};

// ---- Pipeline ----
export const pipelineApi = {
  run: (payload: {
    cnpj: string;
    consultas?: string[];
    tabelas?: string[];
    data_limite?: string;
    incluir_extracao?: boolean;
    incluir_processamento?: boolean;
  }) =>
    api
      .post("/pipeline/run", payload)
      .then((r) => r.data),
  status: (cnpj: string) =>
    api.get<PipelineStatus>(`/pipeline/status/${cnpj}`).then((r) => r.data),
};

// ---- Estoque ----
export const estoqueApi = {
  movEstoque: (cnpj: string, page = 1, page_size = 500) =>
    api
      .get<PageResult>(`/estoque/${cnpj}/mov_estoque`, {
        params: { page, page_size },
      })
      .then((r) => r.data),
  tabelaMensal: (cnpj: string, page = 1, page_size = 500) =>
    api
      .get<PageResult>(`/estoque/${cnpj}/tabela_mensal`, {
        params: { page, page_size },
      })
      .then((r) => r.data),
  tabelaAnual: (cnpj: string, page = 1, page_size = 500) =>
    api
      .get<PageResult>(`/estoque/${cnpj}/tabela_anual`, {
        params: { page, page_size },
      })
      .then((r) => r.data),
  idAgrupados: (cnpj: string, page = 1, page_size = 500) =>
    api
      .get<PageResult>(`/estoque/${cnpj}/id_agrupados`, {
        params: { page, page_size },
      })
      .then((r) => r.data),
  fatoresConversao: (cnpj: string, page = 1, page_size = 500) =>
    api
      .get<PageResult>(`/estoque/${cnpj}/fatores_conversao`, {
        params: { page, page_size },
      })
      .then((r) => r.data),
  updateFator: (
    cnpj: string,
    id_agrupado: string,
    id_produtos: string,
    fator?: number,
    unid_ref?: string,
  ) =>
    api
      .patch<{
        ok: boolean;
      }>(`/estoque/${cnpj}/fatores_conversao`, {
        id_agrupado,
        id_produtos,
        fator,
        unid_ref,
      })
      .then((r) => r.data),
  batchUpdateUnidRef: (cnpj: string, id_agrupado: string, unid_ref: string) =>
    api
      .patch<{
        ok: boolean;
      }>(`/estoque/${cnpj}/fatores_conversao/batch_unid_ref`, {
        id_agrupado,
        unid_ref,
      })
      .then((r) => r.data),
};

// ---- Aggregation ----
export const aggregationApi = {
  tabelaAgrupada: (cnpj: string, page = 1, page_size = 300) =>
    api
      .get<PageResult>(`/aggregation/${cnpj}/tabela_agrupada`, {
        params: { page, page_size },
      })
      .then((r) => r.data),
  merge: (cnpj: string, id_agrupado_destino: string, ids_origem: string[]) =>
    api
      .post<{
        ok: boolean;
      }>("/aggregation/merge", { cnpj, id_agrupado_destino, ids_origem })
      .then((r) => r.data),
};

// ---- SQL ----
export const sqlApi = {
  listFiles: () => api.get<SqlFile[]>("/sql/files").then((r) => r.data),
  readFile: (path: string) =>
    api
      .get<{ content: string }>("/sql/file", { params: { path } })
      .then((r) => r.data),
  execute: (sql: string, cnpj?: string, params?: Record<string, string>) =>
    api
      .post<{
        rows: Record<string, unknown>[];
        count: number;
      }>("/sql/execute", { sql, cnpj, params })
      .then((r) => r.data),
};

// ---- Fisconforme ----
export const fisconformeApi = {
  getConfig: () =>
    api
      .get<{
        oracle_host: string;
        oracle_port: string;
        oracle_service: string;
        db_user: string;
        configured: boolean;
      }>("/fisconforme/config")
      .then((r) => r.data),
  configurarDb: (payload: {
    oracle_host: string;
    oracle_port: number;
    oracle_service: string;
    db_user: string;
    db_password: string;
  }) =>
    api
      .post<{ ok: boolean }>("/fisconforme/configurar-db", payload)
      .then((r) => r.data),
  testarConexao: () =>
    api
      .get<{ ok: boolean; message: string }>("/fisconforme/testar-conexao")
      .then((r) => r.data),
  consultaCadastral: (
    cnpj: string,
    data_inicio: string,
    data_fim: string,
    forcar = false,
  ) =>
    api
      .post<FisconformeConsultaResult>("/fisconforme/consulta-cadastral", {
        cnpj,
        data_inicio,
        data_fim,
        forcar_atualizacao: forcar,
      })
      .then((r) => r.data),
  consultaLote: (
    cnpjs: string[],
    data_inicio: string,
    data_fim: string,
    forcar = false,
  ) =>
    api
      .post<{
        total: number;
        resultados: FisconformeConsultaResult[];
      }>("/fisconforme/consulta-lote", {
        cnpjs,
        data_inicio,
        data_fim,
        forcar_atualizacao: forcar,
      })
      .then((r) => r.data),
  cacheStats: () =>
    api
      .get<FisconformeCacheStats>("/fisconforme/cache/stats")
      .then((r) => r.data),
  limparCache: (cnpj: string) =>
    api
      .delete<{
        ok: boolean;
        removidos: string[];
      }>(`/fisconforme/cache/${cnpj}`)
      .then((r) => r.data),
  getAuditorConfig: () =>
    api.get<AuditorConfig>("/fisconforme/auditor-config").then((r) => r.data),
  salvarAuditorConfig: (payload: AuditorConfig) =>
    api
      .post<{ ok: boolean }>("/fisconforme/auditor-config", payload)
      .then((r) => r.data),
  listDsfs: () =>
    api
      .get<{ items: FisconformeDsfSummary[] }>("/fisconforme/dsfs")
      .then((r) => r.data.items),
  getDsf: (id: string) =>
    api
      .get<FisconformeDsfRecord>(`/fisconforme/dsfs/${id}`)
      .then((r) => r.data),
  salvarDsf: (payload: FisconformeDsfRequest) =>
    api
      .post<FisconformeDsfRecord>("/fisconforme/dsfs", payload)
      .then((r) => r.data),
  gerarNotificacao: (payload: GerarNotificacaoRequest) =>
    api
      .post<GerarNotificacaoResponse>(
        "/fisconforme/gerar-notificacao-v2",
        payload,
      )
      .then((r) => r.data),
  gerarNotificacoesLote: (payload: {
    cnpjs: string[];
    dsf: string;
    dsf_id?: string;
    auditor: string;
    cargo_titulo: string;
    matricula: string;
    contato: string;
    orgao_origem: string;
    output_dir?: string;
    pdf_base64?: string;
  }) =>
    api.post("/fisconforme/gerar-notificacoes-lote", payload, {
      responseType: "blob",
    }),
};

// ---- Oracle ----
export const oracleApi = {
  getConfig: () =>
    api.get<OracleConfigResponse>("/oracle/config").then((r) => r.data),
  testar: (payload: OracleTestarRequest) =>
    api
      .post<OracleTestarResponse>("/oracle/testar", payload)
      .then((r) => r.data),
  salvar: (payload: OracleSalvarRequest) =>
    api.post<{ ok: boolean }>("/oracle/salvar", payload).then((r) => r.data),
};

export default api;
