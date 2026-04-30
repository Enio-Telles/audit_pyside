/* global window */
// data.js — mock fiscal data for the prototype

const CNPJS = [
  { id: "84654326000394", razao: "Atacadao Industrial LTDA", periodo: "2024-Q4", uf: "RO", parquets: 47 },
  { id: "37671507000187", razao: "Distribuidora Norte Comercial", periodo: "2024-Q4", uf: "RO", parquets: 41 },
  { id: "63614176000153", razao: "Cosmeticos Lavanda LTDA", periodo: "2024-Q3", uf: "RO", parquets: 38 },
  { id: "12880843000931", razao: "Heineken Distribuicao SA", periodo: "2024-Q4", uf: "AM", parquets: 52 },
];

const PARQUETS = [
  { name: "c100_xml",         loc: "C:\\audit_pyside\\dados\\CNPJ\\84654326000394\\c100_xml_84654326000394.parquet",         rows: 18420, size: "4.2 MB" },
  { name: "c170_xml",         loc: "C:\\audit_pyside\\dados\\CNPJ\\84654326000394\\c170_xml_84654326000394.parquet",         rows: 92380, size: "21 MB" },
  { name: "c170_efd",         loc: "C:\\audit_pyside\\dados\\CNPJ\\84654326000394\\c170_efd_84654326000394.parquet",         rows: 88112, size: "19 MB" },
  { name: "c170_consolidado", loc: "C:\\audit_pyside\\dados\\CNPJ\\84654326000394\\c170_consol_84654326000394.parquet",      rows: 89004, size: "20 MB" },
  { name: "c176",             loc: "C:\\audit_pyside\\dados\\CNPJ\\84654326000394\\c176_xml_84654326000394.parquet",         rows: 74220, size: "15 MB" },
  { name: "c176_mensal",      loc: "C:\\audit_pyside\\dados\\CNPJ\\84654326000394\\c176_mensal_84654326000394.parquet",      rows: 12440, size: "2.8 MB" },
  { name: "c190",             loc: "C:\\audit_pyside\\dados\\CNPJ\\84654326000394\\c190_84654326000394.parquet",             rows: 56120, size: "11 MB" },
  { name: "cte",              loc: "C:\\audit_pyside\\dados\\CNPJ\\84654326000394\\cte_84654326000394.parquet",              rows: 4220, size: "0.9 MB" },
  { name: "dados_cadastrais", loc: "C:\\audit_pyside\\dados\\CNPJ\\84654326000394\\dadcad_84654326000394.parquet",            rows: 1, size: "8 KB" },
  { name: "e111",             loc: "C:\\audit_pyside\\dados\\CNPJ\\84654326000394\\e111_84654326000394.parquet",             rows: 320, size: "42 KB" },
  { name: "nfce_emissao",     loc: "C:\\audit_pyside\\dados\\CNPJ\\84654326000394\\nfce_84654326000394.parquet",             rows: 312441, size: "62 MB" },
  { name: "nfce_evento",      loc: "C:\\audit_pyside\\dados\\CNPJ\\84654326000394\\nfce_evt_84654326000394.parquet",         rows: 18920, size: "3.7 MB" },
  { name: "nfe_entrada",      loc: "C:\\audit_pyside\\dados\\CNPJ\\84654326000394\\nfe_ent_84654326000394.parquet",          rows: 2188, size: "0.6 MB" },
  { name: "nfe_saida",        loc: "C:\\audit_pyside\\dados\\CNPJ\\84654326000394\\nfe_sai_84654326000394.parquet",          rows: 1944, size: "0.5 MB" },
  { name: "analises/produtos_final",   loc: "C:\\audit_pyside\\dados\\CNPJ\\84654326000394\\analises\\produtos_final_84654326000394.parquet", rows: 1325, size: "0.4 MB", group: "Analises" },
  { name: "analises/c176_agrupado",    loc: "C:\\audit_pyside\\dados\\CNPJ\\84654326000394\\analises\\c176_agrupado.parquet", rows: 312, size: "120 KB", group: "Analises" },
  { name: "analises/fatores_conversao",loc: "C:\\audit_pyside\\dados\\CNPJ\\84654326000394\\analises\\fatores.parquet",      rows: 1325, size: "44 KB", group: "Analises" },
  { name: "analises/mov_estoque",      loc: "C:\\audit_pyside\\dados\\CNPJ\\84654326000394\\analises\\mov_estoque.parquet",  rows: 30005, size: "5.1 MB", group: "Analises" },
];

// Camada blocks for similarity tab — colour by bloco_id*137.508 % 360
const SIM_ROWS = [
  { id: "1042", bloco: 14, desc: "CERVEJA HEINEKEN LATA 350ML",       ncm: "22030000", cest: "0302100", gtin: "7891001", un: "UN",  camada: 0, motivo: "GTIN_IGUAL",      score: 100 },
  { id: "1107", bloco: 14, desc: "CERV HEINEKEN 350ML LATA",          ncm: "22030000", cest: "0302100", gtin: "7891001", un: "UN",  camada: 0, motivo: "GTIN_IGUAL",      score: 100 },
  { id: "2811", bloco: 15, desc: "CERVEJA HEINEKEN LONG NECK 330ML",  ncm: "22030000", cest: "0302100", gtin: "7891999", un: "UN",  camada: 1, motivo: "NCM+CEST+UNID",   score: 87 },
  { id: "1604", bloco: 16, desc: "PINCEL (E) 2\" ROMA 301 005",       ncm: "96034090", cest: "—",       gtin: "7893946177249", un: "UN", camada: 1, motivo: "NCM+CEST+UNID", score: 78 },
  { id: "1622", bloco: 16, desc: "PINCEL ROMA Nº 2 - 301-005",        ncm: "96034090", cest: "—",       gtin: "7893946177249", un: "UN", camada: 1, motivo: "NCM+CEST+UNID", score: 74 },
  { id: "0782", bloco: 17, desc: "CAMARA 275X80 TORTUG",              ncm: "40131010", cest: "1600800", gtin: "SEM GTIN", un: "UN", camada: 1, motivo: "NCM+CEST+UNID",   score: 71 },
  { id: "0812", bloco: 17, desc: "CAMARA AR LT 1417",                  ncm: "40131010", cest: "1600800", gtin: "SEM GTIN", un: "UN", camada: 1, motivo: "NCM+CEST+UNID",   score: 68 },
  { id: "0689", bloco: 18, desc: "CA 17.525 TR 220A TT CA",            ncm: "40139000", cest: "—",       gtin: "SEM GTIN", un: "UN", camada: 2, motivo: "NCM+UNID",        score: 62 },
  { id: "1669", bloco: 19, desc: "PROTETOR CARRETEIRO",                ncm: "40129010", cest: "—",       gtin: "—",        un: "UN", camada: 2, motivo: "NCM+UNID",        score: 58 },
  { id: "0045", bloco: 20, desc: "17.5-25/20 TT COMPL",                ncm: "40118090", cest: "1600200", gtin: "SEM GTIN", un: "UN", camada: 1, motivo: "NCM+CEST+UNID",   score: 75 },
  { id: "0181", bloco: 21, desc: "205/60R16H 04PR LH41",               ncm: "40111000", cest: "1600100", gtin: "10000000000687", un: "UN", camada: 0, motivo: "GTIN_IGUAL", score: 100 },
  { id: "1299", bloco: 22, desc: "LAMPADA LED PERA A60 12W BRANCA KIAN", ncm: "85395000", cest: "0900500", gtin: "7899710006555", un: "UN", camada: 0, motivo: "GTIN_IGUAL", score: 100 },
  { id: "9901", bloco: 23, desc: "BISCOITO RECHEADO MORANGO 100G",     ncm: "19053100", cest: "—",       gtin: "—",        un: "PCT", camada: 2, motivo: "NCM+UNID",        score: 71 },
  { id: "9902", bloco: 23, desc: "BISCOITO RECHEADO CHOCOLATE 100G",   ncm: "19053190", cest: "—",       gtin: "—",        un: "PCT", camada: 2, motivo: "NCM+UNID",        score: 66 },
  { id: "5511", bloco: 24, desc: "CAFE TORRADO MOIDO 250G",            ncm: "—",        cest: "—",       gtin: "—",        un: "—",  camada: 5, motivo: "DESC_TOKENS",     score: 62 },
  { id: "5522", bloco: 24, desc: "CAFE TORRADO E MOIDO PACOTE 250G",    ncm: "—",        cest: "—",       gtin: "—",        un: "—",  camada: 5, motivo: "DESC_TOKENS",     score: 58 },
  { id: "5533", bloco: 24, desc: "CAFE 250G TORRADO MOIDO",             ncm: "—",        cest: "—",       gtin: "—",        un: "—",  camada: 5, motivo: "DESC_TOKENS",     score: 55 },
  { id: "8801", bloco: 25, desc: "DESENGORD REAX LIMAO 500 ML",         ncm: "34022000", cest: "1900100", gtin: "7891234567", un: "UN",  camada: 1, motivo: "NCM+CEST+UNID",   score: 81 },
  { id: "8802", bloco: 25, desc: "DESENGORDURANTE REAX LIMAO 500ML",    ncm: "34022000", cest: "1900100", gtin: "7891234567", un: "UN",  camada: 1, motivo: "NCM+CEST+UNID",   score: 78 },
  { id: "7710", bloco: 26, desc: "DEMOLICAO DO BALCAO",                 ncm: "—",        cest: "—",       gtin: "—",        un: "UN", camada: 4, motivo: "ISOLADO",         score: 0 },
  { id: "9320", bloco: 27, desc: "COPO DESC TOTALPLAST 180ML 100UND",   ncm: "39241000", cest: "1000100", gtin: "7898001", un: "PCT", camada: 1, motivo: "NCM+CEST+UNID",   score: 84 },
  { id: "9321", bloco: 27, desc: "COPO DESCARTAVEL TOTALPLAST 180ML",   ncm: "39241000", cest: "1000100", gtin: "7898001", un: "PCT", camada: 1, motivo: "NCM+CEST+UNID",   score: 80 },
];

const SIM_STATS = {
  n_linhas: 1247,
  n_blocos: 312,
  por_camada: { 0: 42, 1: 187, 2: 63, 3: 14, 4: 935, 5: 6 },
  executou_ms: 1432,
};

// Conversao tab
const CONVERSAO_ROWS = [
  { id_agr: "id_descricao_984", id_prod: "id_descricao_984", desc: "DESENGORD REAX LIMAO 500 ML",   lista: "DESENGORD REAX LIMAO 500 ML",        unid: "UNID", unid_ref: "CX",  fator: 1.00, preco: 8.75 },
  { id_agr: "id_descricao_984", id_prod: "id_descricao_984", desc: "DESENGORD REAX LIMAO 500 ML",   lista: "DESENGORD REAX LIMAO 500 ML",        unid: "CX",   unid_ref: "CX",  fator: 1.00, preco: 8.75 },
  { id_agr: "id_descricao_982", id_prod: "id_descricao_982", desc: "DESCARGA PNEUS",                lista: "DESCARGA PNEUS",                      unid: "UN",   unid_ref: "1",   fator: null, preco: 450.00 },
  { id_agr: "id_descricao_982", id_prod: "id_descricao_982", desc: "DESCARGA PNEUS",                lista: "DESCARGA PNEUS",                      unid: "1",    unid_ref: "1",   fator: 1.00, preco: 450.00 },
  { id_agr: "id_descricao_977", id_prod: "id_descricao_977", desc: "DEMOLICAO DO BALCAO",           lista: "DEMOLICAO DO BALCAO",                 unid: "UN",   unid_ref: "1",   fator: null, preco: 350.00 },
  { id_agr: "id_descricao_977", id_prod: "id_descricao_977", desc: "DEMOLICAO DO BALCAO",           lista: "DEMOLICAO DO BALCAO",                 unid: "1",    unid_ref: "1",   fator: 1.00, preco: 350.00 },
  { id_agr: "id_descricao_966", id_prod: "id_descricao_966", desc: "COPO DESCARTAVEL COPOBRAS 180 ML 25X100 BR", lista: "COPO DESCARTAVEL COPOBRAS 180 ML 25X100 BR", unid: "CX", unid_ref: "5", fator: 0.88, preco: 85.00 },
  { id_agr: "id_descricao_966", id_prod: "id_descricao_966", desc: "COPO DESCARTAVEL COPOBRAS 180 ML 25X100 BR", lista: "COPO DESCARTAVEL COPOBRAS 180 ML 25X100 BR", unid: "5",  unid_ref: "5", fator: 1.00, preco: 85.00 },
  { id_agr: "id_descricao_960", id_prod: "id_descricao_960", desc: "COPO DESC TOTALPLAST 180 ML 100 UND", lista: "COPO DESC TOTALPLAST 180 ML 100 UND",       unid: "UNID", unid_ref: "CX",  fator: null, preco: 23.36 },
  { id_agr: "id_descricao_960", id_prod: "id_descricao_960", desc: "COPO DESC TOTALPLAST 180 ML 100 UND", lista: "COPO DESC TOTALPLAST 180 ML 100 UND",       unid: "CX",   unid_ref: "CX",  fator: 1.00, preco: 23.36 },
  { id_agr: "id_descricao_955", id_prod: "id_descricao_955", desc: "COPO DESC CRISTALCOPO 180ML 100 UN",  lista: "COPO DESC CRISTALCOPO 180ML 100 UN",        unid: "PT",   unid_ref: "11", fator: null, preco: 4.50 },
  { id_agr: "id_descricao_955", id_prod: "id_descricao_955", desc: "COPO DESC CRISTALCOPO 180ML 100 UN",  lista: "COPO DESC CRISTALCOPO 180ML 100 UN",        unid: "11",   unid_ref: "11", fator: 1.00, preco: 4.50 },
  { id_agr: "id_descricao_929", id_prod: "id_descricao_929", desc: "COMPLEMENTO DE ICMS",                  lista: "COMPLEMENTO DE ICMS",                       unid: "UN",   unid_ref: "PC", fator: 1.00, preco: null },
  { id_agr: "id_descricao_929", id_prod: "id_descricao_929", desc: "COMPLEMENTO DE ICMS",                  lista: "COMPLEMENTO DE ICMS",                       unid: "PC",   unid_ref: "PC", fator: 1.00, preco: null },
  { id_agr: "id_descricao_905", id_prod: "id_descricao_905", desc: "CIMENTO VIPAFIX",                       lista: "CIMENTO VIPAFIX",                            unid: "UN",   unid_ref: "1",  fator: 1.00, preco: null },
  { id_agr: "id_descricao_904", id_prod: "id_descricao_904", desc: "CIMENTO TODAS OBRAS ITAU 50 KG",         lista: "CIMENTO TODAS OBRAS ITAU 50 KG",             unid: "UN",   unid_ref: "SACO 5", fator: 1.00, preco: null },
  { id_agr: "id_descricao_904", id_prod: "id_descricao_904", desc: "CIMENTO TODAS OBRAS ITAU 50 KG",         lista: "CIMENTO TODAS OBRAS ITAU 50 KG",             unid: "SACO 5", unid_ref: "SACO 5", fator: 1.00, preco: null },
  { id_agr: "id_descricao_892", id_prod: "id_descricao_892", desc: "CHA LEAO FUZE CAMOMILA 15 GR",            lista: "CHA LEAO FUZE CAMOMILA 15 GR",               unid: "UNID", unid_ref: "CX", fator: null, preco: 4.36 },
];

// Estoque mov rows (mov_estoque)
const ESTOQUE_ROWS = (() => {
  const rows = [];
  const items = [
    "DESINF MINUANO 2L LAVANDA",
    "DESINF MINUANO 2L FLORAL",
    "DESINF MINUANO 2L EUCALIPTO",
    "DESINF KALIPTO 5L LAVANDA",
    "DESINF KALIPTO 5L FLORAL",
  ];
  let ord = 30005;
  for (let i = 0; i < items.length * 6 && ord > 29980; i++) {
    const it = items[Math.floor(i / 6) % items.length];
    const phase = i % 6;
    let tipo = "";
    let nsu = "";
    let chv = "";
    let mod = "";
    let ser = "";
    let nu = "";
    if (phase === 0) tipo = "0 - ESTOQUE INICIAL";
    else if (phase === 1) tipo = "1 - ENTRADA";
    else if (phase === 2) tipo = "0 - ESTOQUE INICIAL";
    else if (phase === 3) tipo = "3 - ESTOQUE FINAL";
    else if (phase === 4) tipo = "1 - ENTRADA";
    else tipo = "0 - ESTOQUE INICIAL";
    if (tipo === "1 - ENTRADA") {
      nsu = "361.451.486";
      chv = "11200884308980000931550010000570671903690427";
      mod = "55";
      ser = "001";
      nu = "57067";
    }
    rows.push({
      ord: ord--,
      tipo,
      fonte: tipo === "1 - ENTRADA" ? "c170" : "gerado",
      desc: it,
      nsu, chv, mod, ser, nu,
    });
  }
  return rows;
})();

// Logs sample
const LOG_LINES = [
  { ts: "13:42:18.412", lvl: "INFO",  src: "main",        msg: "Iniciando audit_pyside v0.4.0" },
  { ts: "13:42:18.503", lvl: "INFO",  src: "config",      msg: "Carregando configuracao de C:\\audit_pyside\\.env" },
  { ts: "13:42:18.612", lvl: "INFO",  src: "oracle",      msg: "Conexao 1 (Principal) -> exa01-scan.sefin.ro.gov.br:1521/sefindw" },
  { ts: "13:42:18.901", lvl: "INFO",  src: "oracle",      msg: "Conexao OK em 125 ms (sefindw, usuario 03002693901)" },
  { ts: "13:42:19.014", lvl: "WARN",  src: "oracle",      msg: "Conexao 2 (Secundaria) ORA-01017: credencial invalida; modo somente-leitura" },
  { ts: "13:42:21.220", lvl: "INFO",  src: "registry",    msg: "Registrado CNPJ 84654326000394 (47 parquets, 92.4 MB)" },
  { ts: "13:42:25.110", lvl: "INFO",  src: "parquet",     msg: "Lendo c176_xml_84654326000394.parquet (74.2k linhas, 15 MB)" },
  { ts: "13:42:25.808", lvl: "INFO",  src: "agregacao",   msg: "Aplicando reducao por descricao_padrao (74220 -> 1325 linhas)" },
  { ts: "13:42:27.401", lvl: "INFO",  src: "similaridade",msg: "Particionamento fiscal: 312 blocos, 4 camadas, 1.43s" },
  { ts: "13:42:31.122", lvl: "WARN",  src: "conversao",   msg: "12 produtos sem fator_conversao definido (unid != unid_ref)" },
  { ts: "13:42:34.910", lvl: "INFO",  src: "estoque",     msg: "Movimentacao gerada: 30005 linhas, periodo 2024-01..2024-12" },
  { ts: "13:42:38.224", lvl: "ERROR", src: "estoque",     msg: "Falha ao calcular saldo_estoque_anual para id_descricao_977 (descricao manual)" },
  { ts: "13:42:39.501", lvl: "INFO",  src: "exporter",    msg: "Exportando aba_resumo_global -> resumo_84654326000394.xlsx" },
  { ts: "13:42:40.030", lvl: "INFO",  src: "exporter",    msg: "Concluido em 528 ms (3.1 MB, 7 abas)" },
];

window.AUDIT_DATA = { CNPJS, PARQUETS, SIM_ROWS, SIM_STATS, CONVERSAO_ROWS, ESTOQUE_ROWS, LOG_LINES };

function auditAppendLog(lvl, src, msg) {
  const now = new Date();
  const ts = now.toTimeString().slice(0, 8);
  window.AUDIT_DATA.LOG_LINES = [
    { ts, lvl, src, msg },
    ...(window.AUDIT_DATA.LOG_LINES || []),
  ].slice(0, 200);
  window.dispatchEvent(new CustomEvent("audit:data-reload"));
}

// Cliente REST — usa o servidor app_react.py quando disponivel.
// Todos os metodos retornam Promise; erros propagam normalmente.
window.AUDIT_API = {

  async ordenarSimilaridade({ rows, metodo, thresholds, opcoes }) {
    const res = await fetch("/api/similaridade/ordenar", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ rows, metodo, thresholds, opcoes }),
    });
    if (!res.ok) {
      const txt = await res.text();
      throw new Error(`HTTP ${res.status}: ${txt}`);
    }
    return res.json();
  },

  async listarCnpjs() {
    const res = await fetch("/api/cnpjs");
    if (!res.ok) return null;
    return res.json();
  },

  async listarParquets(cnpj) {
    const res = await fetch(`/api/parquets?cnpj=${encodeURIComponent(cnpj)}`);
    if (!res.ok) return null;
    return res.json();
  },

  async status() {
    const res = await fetch("/api/status");
    if (!res.ok) return null;
    return res.json();
  },

  async extrairCnpj({ cnpj, data_limite, consultas }) {
    const res = await fetch("/api/extrair", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ cnpj, data_limite, consultas }),
    });
    if (!res.ok) {
      const txt = await res.text();
      throw new Error(`HTTP ${res.status}: ${txt}`);
    }
    return res.json();
  },

  async processarCnpj({ cnpj, tabelas }) {
    const res = await fetch("/api/processar", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ cnpj, tabelas }),
    });
    if (!res.ok) {
      const txt = await res.text();
      throw new Error(`HTTP ${res.status}: ${txt}`);
    }
    return res.json();
  },

  // Carrega produtos, conversao, estoque e logs de um CNPJ.
  // Atualiza window.AUDIT_DATA in-place; retorna true se bem-sucedido.
  async carregarDadosCnpj(cnpj) {
    const res = await fetch(`/api/dados?cnpj=${encodeURIComponent(cnpj)}`);
    if (!res.ok) return false;
    const d = await res.json();
    if (d.produtos?.length) {
      window.AUDIT_DATA.SIM_ROWS  = d.produtos;
      window.AUDIT_DATA.SIM_STATS = d.stats;
    }
    if (d.conversao?.length) window.AUDIT_DATA.CONVERSAO_ROWS = d.conversao;
    if (d.estoque?.length)   window.AUDIT_DATA.ESTOQUE_ROWS   = d.estoque;
    if (d.logs?.length)      window.AUDIT_DATA.LOG_LINES      = d.logs;
    return true;
  },
};

window.auditAppendLog = auditAppendLog;

// Carrega CNPJs e parquets reais do servidor ao iniciar.
// Quando bem-sucedido, atualiza window.AUDIT_DATA e dispara "audit:data-reload"
// para que o App React re-renderize com dados reais.
(async function _initFromServer() {
  try {
    const cnpjsReal = await window.AUDIT_API.listarCnpjs();
    if (!Array.isArray(cnpjsReal) || !cnpjsReal.length || cnpjsReal[0]?.error) return;

    // API ja retorna razao/uf/periodo do reg_0000. Usa mock como fallback para campos ausentes.
    const mockById = Object.fromEntries(CNPJS.map(c => [c.id, c]));
    window.AUDIT_DATA.CNPJS = cnpjsReal.map(c => ({
      id:      c.id,
      razao:   c.razao    || mockById[c.id]?.razao   || c.id,
      periodo: c.periodo  || mockById[c.id]?.periodo || "—",
      uf:      c.uf       || mockById[c.id]?.uf      || "—",
      parquets: c.parquets || 0,
    }));

    const priCnpj = window.AUDIT_DATA.CNPJS[0]?.id;
    if (priCnpj) {
      // Carrega parquets e dados de analise em paralelo
      const [pqs] = await Promise.all([
        window.AUDIT_API.listarParquets(priCnpj),
        window.AUDIT_API.carregarDadosCnpj(priCnpj),
      ]);
      if (Array.isArray(pqs) && pqs.length && !pqs[0]?.error) {
        window.AUDIT_DATA.PARQUETS = pqs;
      }
    }

    window.dispatchEvent(new CustomEvent("audit:data-reload"));
  } catch {
    // servidor offline — dados mock em uso, nenhuma acao necessaria
  }
})();
