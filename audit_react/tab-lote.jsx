/* global React */
// =========================================================
// Aba Analise Lote CNPJ — Wizard 5 etapas
// (Banco de Dados, CNPJs, Auditor/DSF, Periodo, Processamento)
// =========================================================
const { useState: useStateLote, useMemo: useMemoLote } = React;

const STEPS = [
  { id: 1, key: "banco",     title: "Banco de Dados",  hint: "Validar acesso ao banco e pré-requisitos." },
  { id: 2, key: "cnpjs",     title: "CNPJs",           hint: "Carregue de arquivo TXT ou cole a lista." },
  { id: 3, key: "auditor",   title: "Auditor / DSF",   hint: "Cadastre o responsável e a referência DSF." },
  { id: 4, key: "periodo",   title: "Período",         hint: "Recorte temporal e revisão." },
  { id: 5, key: "process",   title: "Processamento",   hint: "Execute o lote e acompanhe o log." },
];

function LoteTab() {
  const [step, setStep] = useStateLote(2);
  const [lote, setLote] = useStateLote({
    cnpjs: [],          // [{ id, razao, mun, uf, situacao, origem, status }]
    cnpjPaste: "",
    fileName: "",
    auditor: { nome: "", matricula: "", contato: "", orgao: "", titulo: "Auditor" },
    perfilSalvo: "— Novo preenchimento —",
    dsf: { numero: "", ano: new Date().getFullYear(), pdf: "" },
    periodo: { ini: "01/2021", fim: "12/2025" },
    progresso: { feitos: 0, total: 0, pct: 0 },
    log: [],
    rodando: false,
  });

  const stepIdx = STEPS.findIndex(s => s.id === step);
  const canPrev = stepIdx > 0;
  const canNext = stepIdx < STEPS.length - 1;
  const advance = () => canNext && setStep(STEPS[stepIdx + 1].id);
  const back = () => canPrev && setStep(STEPS[stepIdx - 1].id);

  const stepStatus = (id) => {
    if (id < step) return "done";
    if (id === step) return "active";
    return "pending";
  };

  return (
    <div className="lote-shell">
      {/* === LEFT: stepper + resumo === */}
      <aside className="lote-stepper">
        <div className="lote-stepper-head">
          <div className="lote-stepper-brand">
            <span className="serif">SEFIN</span> <em className="serif" style={{ color: "var(--accent)", fontStyle: "italic" }}>/ RO</em>
          </div>
          <div className="lote-stepper-sub">Pipeline Fisconforme</div>
          <div className="lote-stepper-tag mono">Notificações não atendidas</div>
        </div>

        <ol className="lote-step-list">
          {STEPS.map(s => {
            const status = stepStatus(s.id);
            return (
              <li key={s.id}
                className={`lote-step lote-step-${status}`}
                onClick={() => setStep(s.id)}>
                <span className="lote-step-num mono">{s.id}.</span>
                <span className="lote-step-title">{s.title}</span>
                {status === "done" && (
                  <svg className="lote-step-check" width="12" height="12" viewBox="0 0 16 16" fill="none">
                    <path d="M3 8.5l3.5 3.5L13 5" stroke="currentColor" strokeWidth="2"
                      strokeLinecap="round" strokeLinejoin="round"/>
                  </svg>
                )}
              </li>
            );
          })}
        </ol>

        <div className="lote-summary">
          <div className="lote-summary-title">Resumo operacional</div>
          <div className="lote-summary-grid">
            <div><div className="lote-summary-k">CNPJs</div><div className="lote-summary-v mono">{lote.cnpjs.length}</div></div>
            <div><div className="lote-summary-k">DSF</div><div className="lote-summary-v mono">{lote.dsf.numero || "—"}</div></div>
            <div><div className="lote-summary-k">Auditor</div><div className="lote-summary-v">{lote.auditor.nome || "—"}</div></div>
            <div><div className="lote-summary-k">Período</div><div className="lote-summary-v mono">{lote.periodo.ini} a {lote.periodo.fim}</div></div>
          </div>
          <div className="lote-summary-prev">
            <div className="lote-summary-k">Prévia</div>
            <div className="lote-summary-v">
              {lote.cnpjs.length === 0 ? "Nenhum CNPJ carregado." :
               `${lote.cnpjs.length} contribuinte(s) prontos.`}
            </div>
          </div>
        </div>

        <div className="lote-stepper-foot mono">
          v2.0 · integrado ao Sistema PySiSDE
        </div>
      </aside>

      {/* === RIGHT: step content === */}
      <main className="lote-main">
        <div className="lote-main-scroll">
          {step === 1 && <StepBanco/>}
          {step === 2 && <StepCNPJs lote={lote} setLote={setLote}/>}
          {step === 3 && <StepAuditor lote={lote} setLote={setLote}/>}
          {step === 4 && <StepPeriodo lote={lote} setLote={setLote}/>}
          {step === 5 && <StepProcessamento lote={lote} setLote={setLote}/>}
        </div>

        <footer className="lote-footbar">
          <div className="lote-footbar-meta mono">
            Etapa {step} de {STEPS.length} · {STEPS[stepIdx].hint}
          </div>
          <div className="lote-footbar-actions">
            <button className="btn" disabled={!canPrev} onClick={back}>Anterior</button>
            {canNext ? (
              <button className="btn primary" onClick={advance}>
                {step === 1 && "Confirmar e seguir"}
                {step === 2 && "Confirmar CNPJs e seguir"}
                {step === 3 && "Confirmar auditor e seguir"}
                {step === 4 && "Preparar processamento"}
              </button>
            ) : (
              <button className="btn primary lote-run-btn">
                {lote.rodando ? "Executando…" : "Iniciar processamento"}
              </button>
            )}
          </div>
        </footer>
      </main>
    </div>
  );
}

// =========================================================
// Step components
// =========================================================
function StepBanco() {
  return (
    <>
      <StepHeader title="Banco de Dados"
        sub="Confirme a conexão Oracle ativa e os pré-requisitos antes de selecionar contribuintes."/>
      <div className="lote-card">
        <div className="lote-card-title">Status da conexão</div>
        <div className="status-grid">
          <div className="status-row">
            <div className="status-label"><span className="dot success"/><strong>Oracle Principal</strong></div>
            <div className="status-value success mono">conectado · 125 ms</div>
          </div>
          <div className="status-row">
            <div className="status-label"><span className="dot danger"/><strong>Oracle Secundário</strong></div>
            <div className="status-value danger mono">ORA-01017 · credencial inválida</div>
          </div>
          <div className="status-row">
            <div className="status-label"><span className="dot success"/><strong>Cache local</strong></div>
            <div className="status-value success mono">8.4 GB livres · /audit_pyside/cache</div>
          </div>
        </div>
      </div>
      <div className="lote-card">
        <div className="lote-card-title">Pré-requisitos</div>
        <ul className="lote-checklist">
          <li className="ok">Tabelas EFD (c170, c176, e520) acessíveis</li>
          <li className="ok">Tabelas NFe (nfe_cab, nfe_det) acessíveis</li>
          <li className="ok">Diretório de saída gravável</li>
          <li className="warn">Conexão secundária somente-leitura</li>
        </ul>
      </div>
    </>
  );
}

function StepCNPJs({ lote, setLote }) {
  const adicionar = () => {
    const tokens = lote.cnpjPaste.split(/[\s,;]+/).filter(t => t && /^[0-9]{8,14}$/.test(t.replace(/\D/g, "")));
    if (!tokens.length) return;
    const novos = tokens.map((t, i) => ({
      id: t.replace(/\D/g, "").padStart(14, "0"),
      razao: ["LOJA CENTRAL LTDA", "DISTRIBUIDORA DO NORTE SA", "COMERCIAL VALE", "PROD. PORTO VELHO ME"][i % 4],
      mun: ["Porto Velho", "Ji-Paraná", "Ariquemes", "Cacoal"][i % 4] + "/RO",
      situacao: "ATIVA",
      origem: "MANUAL",
      status: "Pendente",
    }));
    setLote({ ...lote, cnpjs: [...lote.cnpjs, ...novos], cnpjPaste: "" });
  };

  const adicionarMock = () => {
    const seed = ["84654326000394", "37671507000187", "63614176000153", "12880843000931", "55993270000180"];
    setLote({ ...lote, cnpjPaste: seed.join("\n") });
  };

  return (
    <>
      <StepHeader title="Seleção de CNPJs"
        sub="Os contribuintes são exibidos em grade estruturada, com colunas separadas para facilitar leitura, ordenação e evitar colisão de textos longos."/>

      <div className="lote-card">
        <div className="lote-card-title">Carregar de arquivo</div>
        <div className="lote-fileload">
          <input className="input muted" readOnly
            value={lote.fileName || "Nenhum arquivo selecionado"}/>
          <button className="btn primary"
            onClick={() => setLote({ ...lote, fileName: "lote_2025_q4.txt" })}>
            Selecionar arquivo TXT
          </button>
        </div>
      </div>

      <div className="lote-card">
        <div className="lote-card-title">Adicionar CNPJs</div>
        <textarea
          className="lote-textarea mono"
          placeholder="Cole ou digite CNPJs separados por linha, vírgula, ponto-e-vírgula ou espaço."
          value={lote.cnpjPaste}
          onChange={e => setLote({ ...lote, cnpjPaste: e.target.value })}
          rows={8}/>
        <div className="lote-card-actions">
          <button className="btn ghost sm" onClick={adicionarMock}>Inserir exemplo</button>
          <div style={{ flex: 1 }}/>
          <button className="btn primary" onClick={adicionar}>Adicionar todos</button>
        </div>
      </div>

      <div className="lote-card">
        <div className="lote-card-title">
          Contribuintes para processamento
          <span className="lote-card-meta mono">{lote.cnpjs.length} carregado(s)</span>
        </div>
        <div className="lote-card-sub">A grade mantém tooltip completo por linha e evita misturar CNPJ com razão social na mesma célula.</div>

        <div className="data-table-shell" style={{ marginTop: 8 }}>
          <div className="data-table-scroll" style={{ maxHeight: 260 }}>
            <table className="data-table">
              <thead>
                <tr>
                  <th style={{ width: 40 }}>#</th>
                  <th>CNPJ</th>
                  <th>Razão Social</th>
                  <th>Município/UF</th>
                  <th>Situação</th>
                  <th>Origem</th>
                  <th>Status</th>
                </tr>
              </thead>
              <tbody>
                {lote.cnpjs.length === 0 ? (
                  <tr><td colSpan={7} className="lote-table-empty">
                    Nenhum CNPJ carregado. Cole acima ou selecione um arquivo TXT.
                  </td></tr>
                ) : lote.cnpjs.map((c, i) => (
                  <tr key={c.id + i}>
                    <td className="col-num">{i + 1}</td>
                    <td className="mono">{c.id}</td>
                    <td>{c.razao}</td>
                    <td>{c.mun}</td>
                    <td><span className="pill success">{c.situacao}</span></td>
                    <td className="mono" style={{ fontSize: 10 }}>{c.origem}</td>
                    <td><span className="pill">{c.status}</span></td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      </div>
    </>
  );
}

function StepAuditor({ lote, setLote }) {
  const set = (k, v) => setLote({ ...lote, auditor: { ...lote.auditor, [k]: v } });
  const setDsf = (k, v) => setLote({ ...lote, dsf: { ...lote.dsf, [k]: v } });
  return (
    <>
      <StepHeader title="Dados do Auditor e DSF"
        sub="Os campos foram reorganizados em blocos adaptativos para manter legibilidade em larguras menores, sem caixas comprimidas nem rótulos sobrepostos."/>

      <div className="lote-card">
        <div className="lote-card-title">Configurações salvas</div>
        <div className="form-field">
          <div className="form-label">Perfil salvo</div>
          <select className="select"
            value={lote.perfilSalvo}
            onChange={e => setLote({ ...lote, perfilSalvo: e.target.value })}>
            <option>— Novo preenchimento —</option>
            <option>GEFIS · auditor padrão</option>
            <option>GEFIS · plantão notificações</option>
          </select>
        </div>
      </div>

      <div className="lote-card">
        <div className="lote-card-title">Dados do auditor</div>
        <div className="form-grid">
          <Field label="Nome do auditor" placeholder="Ex: João da Silva"
            value={lote.auditor.nome} onChange={v => set("nome", v)}/>
          <Field label="Matrícula" placeholder="Ex: 300201625"
            value={lote.auditor.matricula} onChange={v => set("matricula", v)}/>
          <Field label="Contato" placeholder="Ex: auditor@sefin.ro.gov.br"
            value={lote.auditor.contato} onChange={v => set("contato", v)}/>
          <Field label="Órgão" placeholder="Ex: GEFIS"
            value={lote.auditor.orgao} onChange={v => set("orgao", v)}/>
          <Field label="Título" value={lote.auditor.titulo} onChange={v => set("titulo", v)}/>
        </div>
      </div>

      <div className="lote-card">
        <div className="lote-card-title">Documento de Solicitação Fiscal (DSF)</div>
        <div className="form-grid">
          <Field label="Número DSF" placeholder="Ex: 2025/00482"
            value={lote.dsf.numero} onChange={v => setDsf("numero", v)}/>
          <Field label="Ano" value={String(lote.dsf.ano)} onChange={v => setDsf("ano", v)}/>
          <div className="form-field">
            <div className="form-label">PDF DSF</div>
            <div className="lote-fileload">
              <input className="input muted" readOnly
                value={lote.dsf.pdf || "Nenhum arquivo anexado"}/>
              <button className="btn"
                onClick={() => setDsf("pdf", "DSF_2025_00482.pdf")}>Anexar PDF</button>
            </div>
          </div>
        </div>
      </div>
    </>
  );
}

function StepPeriodo({ lote, setLote }) {
  const set = (k, v) => setLote({ ...lote, periodo: { ...lote.periodo, [k]: v } });
  const a = lote.auditor;
  return (
    <>
      <StepHeader title="Período de Análise"
        sub="O resumo executivo foi convertido em grade de metadados, mais estável visualmente e mais clara para revisar antes do processamento."/>

      <div className="lote-card">
        <div className="lote-card-title">Recorte temporal</div>
        <div className="form-grid" style={{ gridTemplateColumns: "1fr 1fr" }}>
          <Field label="Data inicial" value={lote.periodo.ini} onChange={v => set("ini", v)}/>
          <Field label="Data final"   value={lote.periodo.fim} onChange={v => set("fim", v)}/>
        </div>
      </div>

      <div className="lote-card">
        <div className="lote-card-title">Resumo da execução</div>
        <div className="lote-meta-grid">
          <Meta k="CNPJs válidos"  v={lote.cnpjs.length} mono/>
          <Meta k="Auditor"        v={a.nome || "—"}/>
          <Meta k="Matrícula"      v={a.matricula || "—"} mono/>
          <Meta k="DSF"            v={lote.dsf.numero || "—"} mono/>
          <Meta k="Órgão"          v={a.orgao || "—"}/>
          <Meta k="PDF DSF"        v={lote.dsf.pdf || "—"} mono/>
          <Meta k="Período"        v={`${lote.periodo.ini} a ${lote.periodo.fim}`} mono/>
          <Meta k="Contato"        v={a.contato || "—"}/>
        </div>
      </div>
    </>
  );
}

function StepProcessamento({ lote, setLote }) {
  const total = lote.cnpjs.length;
  const feitos = lote.progresso.feitos;
  const pct = total ? Math.round((feitos / total) * 100) : 0;

  const iniciar = () => {
    setLote({
      ...lote,
      rodando: true,
      progresso: { feitos: 0, total, pct: 0 },
      log: [
        { ts: ts(), lvl: "INFO", msg: `Lote iniciado · ${total} CNPJ(s) · período ${lote.periodo.ini} a ${lote.periodo.fim}` },
        { ts: ts(), lvl: "INFO", msg: `Auditor: ${lote.auditor.nome || "—"} (mat. ${lote.auditor.matricula || "—"})` },
      ],
    });
  };
  const cancelar = () => setLote({ ...lote, rodando: false });

  return (
    <>
      <StepHeader title="Processamento"
        sub="A etapa final foi reorganizada com log e resultados em splitter vertical, sem limites fixos que comprimam o conteúdo."/>

      <div className="lote-info-banner">
        <span className="pill info">Informação</span>
        <span>Pronto para processar <strong className="mono">{total}</strong> CNPJ(s) no período <strong className="mono">{lote.periodo.ini} a {lote.periodo.fim}</strong>.</span>
      </div>

      <div className="lote-card">
        <div className="lote-card-title">Acompanhamento</div>
        <div className="lote-track">
          <div className="lote-track-row">
            <strong>{lote.rodando ? `Processando ${feitos + 1} de ${total}…` : `Pronto para iniciar o lote com ${total} CNPJ(s).`}</strong>
            <div style={{ display: "flex", gap: 6 }}>
              <button className="btn danger" disabled={!lote.rodando} onClick={cancelar}>Cancelar</button>
              <button className="btn">Abrir pasta de saída</button>
            </div>
          </div>
          <div className="lote-progress-track">
            <div className="lote-progress-bar" style={{ width: pct + "%" }}/>
          </div>
          <div className="mono lote-progress-meta">{feitos} / {total || 1} ({pct}%)</div>
        </div>
        <div style={{ marginTop: 8 }}>
          <button className="btn primary" disabled={lote.rodando || total === 0} onClick={iniciar}>
            {lote.rodando ? "Em execução…" : "Iniciar processamento"}
          </button>
        </div>
      </div>

      <div className="lote-card">
        <div className="lote-card-title">Log de execução</div>
        <div className="lote-log mono">
          {lote.log.length === 0 ? (
            <div className="lote-log-empty">— sem registros, aguarde início —</div>
          ) : lote.log.map((l, i) => (
            <div key={i} className={`log-line lvl-${l.lvl}`}>
              <span className="log-ts">{l.ts}</span>
              <span className={`log-lvl lvl-${l.lvl}`}>{l.lvl}</span>
              <span className="log-src">lote</span>
              <span className="log-msg">{l.msg}</span>
            </div>
          ))}
        </div>
      </div>

      <div className="lote-card">
        <div className="lote-card-title">Resultados</div>
        <div className="placeholder-table compact">
          <div className="serif" style={{ fontSize: 14, marginBottom: 4 }}>aguardando execução</div>
          <div className="mono" style={{ fontSize: 11 }}>os arquivos gerados (xlsx, pdf, log) aparecerão aqui</div>
        </div>
      </div>
    </>
  );
}

// =========================================================
// Helpers
// =========================================================
function StepHeader({ title, sub }) {
  return (
    <header className="lote-step-header">
      <h1 className="serif lote-step-h1">{title}</h1>
      <p className="lote-step-sub">{sub}</p>
    </header>
  );
}
function Field({ label, value, onChange, placeholder }) {
  return (
    <div className="form-field">
      <div className="form-label">{label}</div>
      <input className="input"
        value={value} placeholder={placeholder}
        onChange={e => onChange(e.target.value)}/>
    </div>
  );
}
function Meta({ k, v, mono }) {
  return (
    <div className="lote-meta-cell">
      <div className="lote-meta-k">{k}</div>
      <div className={`lote-meta-v ${mono ? "mono" : ""}`}>{v || "—"}</div>
    </div>
  );
}
function ts() {
  const d = new Date();
  return `${String(d.getHours()).padStart(2,"0")}:${String(d.getMinutes()).padStart(2,"0")}:${String(d.getSeconds()).padStart(2,"0")}`;
}

window.LoteTab = LoteTab;
