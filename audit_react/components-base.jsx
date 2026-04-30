/* global React */
const { useState, useMemo, useEffect } = React;

// =========================================================
// Shared building blocks
// =========================================================

function blocoColor(blocoId) {
  const hue = (blocoId * 137.508) % 360;
  return `hsl(${hue.toFixed(1)}, 56%, 58%)`;
}

function scoreClass(s) {
  if (s == null) return "";
  if (s >= 80) return "high";
  if (s >= 65) return "med";
  return "low";
}

function CamadaBadge({ c }) {
  return <span className={`camada-badge camada-${c}`}>{c}</span>;
}

function Pill({ kind, children }) {
  return <span className={`pill ${kind || ""}`}>{children}</span>;
}

function Icon({ name, size = 14 }) {
  // tiny inline SVG icon set
  const paths = {
    chevR: <polyline points="9 6 15 12 9 18" />,
    chevD: <polyline points="6 9 12 15 18 9" />,
    chevL: <polyline points="15 6 9 12 15 18" />,
    search: <><circle cx="11" cy="11" r="7" /><line x1="21" y1="21" x2="16.5" y2="16.5" /></>,
    cmd: <path d="M9 6a3 3 0 1 0 0 6h6a3 3 0 1 0 0-6 3 3 0 0 0-3 3v6a3 3 0 1 1-3 3 3 3 0 0 1 0-6h6a3 3 0 1 1 3 3" />,
    folder: <path d="M3 7a2 2 0 0 1 2-2h4l2 2h8a2 2 0 0 1 2 2v9a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2z" />,
    plus: <><line x1="12" y1="5" x2="12" y2="19"/><line x1="5" y1="12" x2="19" y2="12"/></>,
    play: <polygon points="6 4 20 12 6 20 6 4"/>,
    refresh: <><polyline points="23 4 23 10 17 10"/><polyline points="1 20 1 14 7 14"/><path d="M3.51 9A9 9 0 0 1 18.36 5.64L23 10M1 14l4.64 4.36A9 9 0 0 0 20.49 15"/></>,
    download: <><path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"/><polyline points="7 10 12 15 17 10"/><line x1="12" y1="15" x2="12" y2="3"/></>,
    trash: <><polyline points="3 6 5 6 21 6"/><path d="M19 6l-1 14a2 2 0 0 1-2 2H8a2 2 0 0 1-2-2L5 6"/><path d="M10 11v6M14 11v6"/></>,
    check: <polyline points="20 6 9 17 4 12"/>,
    x: <><line x1="18" y1="6" x2="6" y2="18"/><line x1="6" y1="6" x2="18" y2="18"/></>,
    db: <><ellipse cx="12" cy="5" rx="9" ry="3"/><path d="M3 5v14a9 3 0 0 0 18 0V5"/><path d="M3 12a9 3 0 0 0 18 0"/></>,
    settings: <><circle cx="12" cy="12" r="3"/><path d="M19.4 15a1.65 1.65 0 0 0 .33 1.82l.06.06a2 2 0 1 1-2.83 2.83l-.06-.06a1.65 1.65 0 0 0-1.82-.33 1.65 1.65 0 0 0-1 1.51V21a2 2 0 0 1-4 0v-.09A1.65 1.65 0 0 0 9 19.4a1.65 1.65 0 0 0-1.82.33l-.06.06a2 2 0 1 1-2.83-2.83l.06-.06a1.65 1.65 0 0 0 .33-1.82 1.65 1.65 0 0 0-1.51-1H3a2 2 0 0 1 0-4h.09A1.65 1.65 0 0 0 4.6 9a1.65 1.65 0 0 0-.33-1.82l-.06-.06a2 2 0 1 1 2.83-2.83l.06.06a1.65 1.65 0 0 0 1.82.33H9a1.65 1.65 0 0 0 1-1.51V3a2 2 0 0 1 4 0v.09a1.65 1.65 0 0 0 1 1.51 1.65 1.65 0 0 0 1.82-.33l.06-.06a2 2 0 1 1 2.83 2.83l-.06.06a1.65 1.65 0 0 0-.33 1.82V9a1.65 1.65 0 0 0 1.51 1H21a2 2 0 0 1 0 4h-.09a1.65 1.65 0 0 0-1.51 1z"/></>,
    edit: <><path d="M11 4H4a2 2 0 0 0-2 2v14a2 2 0 0 0 2 2h14a2 2 0 0 0 2-2v-7"/><path d="M18.5 2.5a2.121 2.121 0 0 1 3 3L12 15l-4 1 1-4 9.5-9.5z"/></>,
    list: <><line x1="8" y1="6" x2="21" y2="6"/><line x1="8" y1="12" x2="21" y2="12"/><line x1="8" y1="18" x2="21" y2="18"/><line x1="3" y1="6" x2="3.01" y2="6"/><line x1="3" y1="12" x2="3.01" y2="12"/><line x1="3" y1="18" x2="3.01" y2="18"/></>,
    grid: <><rect x="3" y="3" width="7" height="7"/><rect x="14" y="3" width="7" height="7"/><rect x="14" y="14" width="7" height="7"/><rect x="3" y="14" width="7" height="7"/></>,
    box: <><path d="M21 16V8a2 2 0 0 0-1-1.73l-7-4a2 2 0 0 0-2 0l-7 4A2 2 0 0 0 3 8v8a2 2 0 0 0 1 1.73l7 4a2 2 0 0 0 2 0l7-4A2 2 0 0 0 21 16z"/><polyline points="3.27 6.96 12 12.01 20.73 6.96"/><line x1="12" y1="22.08" x2="12" y2="12"/></>,
    file: <><path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z"/><polyline points="14 2 14 8 20 8"/></>,
    code: <><polyline points="16 18 22 12 16 6"/><polyline points="8 6 2 12 8 18"/></>,
    layers: <><polygon points="12 2 2 7 12 12 22 7 12 2"/><polyline points="2 17 12 22 22 17"/><polyline points="2 12 12 17 22 12"/></>,
    columns: <><path d="M12 3v18"/><path d="M3 3h18v18H3z"/></>,
    target: <><circle cx="12" cy="12" r="10"/><circle cx="12" cy="12" r="6"/><circle cx="12" cy="12" r="2"/></>,
    save: <><path d="M19 21H5a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h11l5 5v11a2 2 0 0 1-2 2z"/><polyline points="17 21 17 13 7 13 7 21"/><polyline points="7 3 7 8 15 8"/></>,
    star: <polygon points="12 2 15.09 8.26 22 9.27 17 14.14 18.18 21.02 12 17.77 5.82 21.02 7 14.14 2 9.27 8.91 8.26 12 2"/>,
    info: <><circle cx="12" cy="12" r="10"/><line x1="12" y1="16" x2="12" y2="12"/><line x1="12" y1="8" x2="12.01" y2="8"/></>,
    warn: <><path d="M10.29 3.86L1.82 18a2 2 0 0 0 1.71 3h16.94a2 2 0 0 0 1.71-3L13.71 3.86a2 2 0 0 0-3.42 0z"/><line x1="12" y1="9" x2="12" y2="13"/><line x1="12" y1="17" x2="12.01" y2="17"/></>,
    alert: <><circle cx="12" cy="12" r="10"/><line x1="12" y1="8" x2="12" y2="12"/><line x1="12" y1="16" x2="12.01" y2="16"/></>,
    wifi: <><path d="M5 12.55a11 11 0 0 1 14.08 0"/><path d="M1.42 9a16 16 0 0 1 21.16 0"/><path d="M8.53 16.11a6 6 0 0 1 6.95 0"/><line x1="12" y1="20" x2="12.01" y2="20"/></>,
  };
  return (
    <svg width={size} height={size} viewBox="0 0 24 24" fill="none"
      stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"
      style={{ flexShrink: 0 }}>
      {paths[name]}
    </svg>
  );
}

// =========================================================
// AppHeader — global brand bar
// =========================================================
function AppHeader({ activeCnpj, period, onTogglePanel, panelOpen }) {
  return (
    <header className="app-header">
      <div className="brand">
        <span className="brand-mark">audit_pyside</span>
        <span className="brand-rest">/ fiscal parquet analyzer</span>
      </div>
      <div className="header-meta">
        <span><strong>CNPJ</strong> {activeCnpj || "—"}</span>
        <span><strong>periodo</strong> {period || "—"}</span>
        <span><strong>v</strong> 0.4.0</span>
        <button className="btn ghost sm" onClick={onTogglePanel} title="Alternar painel lateral">
          <Icon name={panelOpen ? "chevL" : "chevR"} size={12} />
          {panelOpen ? "Ocultar painel" : "Mostrar painel"}
        </button>
      </div>
    </header>
  );
}

// =========================================================
// LeftPanel — CNPJ list + Parquet files + recommended flow
// =========================================================
function LeftPanel({ cnpjs, parquets, activeCnpj, setActiveCnpj, onAction }) {
  const [cnpjQuery, setCnpjQuery] = useState(activeCnpj || "");
  const [dataLimite, setDataLimite] = useState("2026-04-29");
  const [openGroups, setOpenGroups] = useState({ Analises: true });

  useEffect(() => {
    if (activeCnpj && !cnpjQuery) {
      setCnpjQuery(activeCnpj);
    }
  }, [activeCnpj, cnpjQuery]);

  const filteredCnpjs = cnpjs.filter(c =>
    !cnpjQuery || c.id.includes(cnpjQuery) || c.razao.toLowerCase().includes(cnpjQuery.toLowerCase())
  );

  const grouped = useMemo(() => {
    const g = { _root: [] };
    parquets.forEach(p => {
      const key = p.group || "_root";
      g[key] = g[key] || [];
      g[key].push(p);
    });
    return g;
  }, [parquets]);

  return (
    <aside className="left-panel">
      <div className="left-section">
        <div className="section-label">CPF / CNPJ</div>
        <div className="cnpj-input-row">
          <input className="input" placeholder="Digite CPF ou CNPJ"
            value={cnpjQuery} onChange={e => setCnpjQuery(e.target.value)} />
          <button className="btn primary sm" onClick={() => onAction("extrair", { cnpj: cnpjQuery || activeCnpj, dataLimite })}>
            <Icon name="plus" size={11}/> Extrair
          </button>
        </div>

        <div className="left-controls">
          <div className="left-control-row">
            <label className="left-control-label">Data limite EFD</label>
            <input className="input" type="date" value={dataLimite} onChange={e => setDataLimite(e.target.value)} />
          </div>
          <div className="left-control-grid">
            <button className="btn sm" onClick={() => onAction("extrair", { cnpj: cnpjQuery || activeCnpj, dataLimite })}>Extrair tabelas</button>
            <button className="btn sm" onClick={() => onAction("processar", { cnpj: cnpjQuery || activeCnpj })}>Processamento</button>
            <button className="btn sm" onClick={() => onAction("atualizar", { cnpj: activeCnpj })}>Atualizar lista</button>
            <button className="btn sm"><Icon name="folder" size={11}/> Abrir pasta</button>
            <button className="btn sm danger">Apagar dados</button>
            <button className="btn sm danger">Apagar CNPJ</button>
          </div>
          <button className="btn danger reset-all">
            <Icon name="trash" size={12}/> Apagar TUDO (Reset Geral)
          </button>
        </div>

        <div className="cnpj-list">
          {filteredCnpjs.map(c => (
            <button key={c.id}
              className={`cnpj-row ${c.id === activeCnpj ? "is-active" : ""}`}
              onClick={() => setActiveCnpj(c.id)}>
              <span className="mono cnpj-id">{c.id}</span>
              <span className="cnpj-meta">
                <span className="cnpj-razao">{c.razao}</span>
                <span className="cnpj-tags">
                  <Pill>{c.uf}</Pill>
                  <Pill>{c.periodo}</Pill>
                  <span className="mono cnpj-pq">{c.parquets} pq</span>
                </span>
              </span>
            </button>
          ))}
        </div>
      </div>

      <div className="left-section parquet-section">
        <div className="section-label">Arquivos Parquet do CNPJ</div>
        <div className="parquet-list">
          {Object.entries(grouped).map(([group, items]) => (
            <div key={group} className="parquet-group">
              {group !== "_root" && (
                <button className="parquet-group-header"
                  onClick={() => setOpenGroups(o => ({...o, [group]: !o[group]}))}>
                  <Icon name={openGroups[group] ? "chevD" : "chevR"} size={11}/>
                  <span>{group}</span>
                  <span className="mono group-count">{items.length}</span>
                </button>
              )}
              {(group === "_root" || openGroups[group]) && items.map(p => (
                <div key={p.name} className="parquet-row">
                  <div className="parquet-name mono">
                    {p.name.replace(/^analises\//, "")}
                  </div>
                  <div className="parquet-meta mono">
                    {p.rows == null ? "?" : p.rows.toLocaleString("pt-BR")} · {p.size}
                  </div>
                </div>
              ))}
            </div>
          ))}
        </div>
      </div>

      <div className="left-section flow-hint">
        <div className="section-label">Fluxo recomendado</div>
        <ol className="flow-list">
          <li>Selecionar CNPJ acima.</li>
          <li>Abrir tabela na aba <strong>Consulta</strong>.</li>
          <li>Filtrar e selecionar colunas; exportar se preciso.</li>
          <li>Para agregar, partir da tabela desagregada e ir em <strong>Agregação</strong>.</li>
          <li>Use <strong>Similaridade</strong> para ordenar candidatos antes de marcar.</li>
        </ol>
      </div>
    </aside>
  );
}

// =========================================================
// TabBar
// =========================================================
function TabBar({ tabs, active, setActive }) {
  return (
    <div className="tab-bar" role="tablist">
      {tabs.map(t => (
        <button key={t.id} role="tab" aria-selected={active === t.id}
          className={`tab ${active === t.id ? "is-active" : ""}`}
          onClick={() => setActive(t.id)}>
          {t.icon && <Icon name={t.icon} size={12}/>}
          <span>{t.label}</span>
          {t.badge && <span className="tab-badge mono">{t.badge}</span>}
        </button>
      ))}
    </div>
  );
}

// expose
Object.assign(window, {
  React_Hooks: { useState, useMemo, useEffect },
  blocoColor, scoreClass, CamadaBadge, Pill, Icon,
  AppHeader, LeftPanel, TabBar
});
