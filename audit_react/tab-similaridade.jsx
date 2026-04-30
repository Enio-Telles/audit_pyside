/* global React, window, blocoColor, scoreClass, CamadaBadge, Pill, Icon */
const { useState: useStateSim, useMemo: useMemoSim } = React;
const TableUtils = window.AUDIT_TABLE_UTILS;

const similaridadeColumns = [
  { key: "select", label: "", width: 32, filter: false },
  { key: "bloco", label: "Bloco", width: 78, numeric: true },
  { key: "desc", label: "Descricao", className: "col-desc" },
  { key: "ncm", label: "NCM", className: "col-fiscal" },
  { key: "cest", label: "CEST", className: "col-fiscal" },
  { key: "gtin", label: "GTIN", className: "col-fiscal" },
  { key: "un", label: "UN", width: 70 },
  { key: "camada", label: "Camada", width: 90, numeric: true },
  { key: "motivo", label: "Motivo", className: "col-motivo" },
  { key: "score", label: "Score", width: 82, numeric: true, align: "right" },
];

const similaridadeProfiles = {
  Completo: {
    visible: null,
    order: ["select", "bloco", "desc", "ncm", "cest", "gtin", "un", "camada", "motivo", "score"],
  },
  Fiscal: {
    visible: ["select", "desc", "ncm", "cest", "gtin", "un", "motivo", "score"],
    order: ["select", "desc", "ncm", "cest", "gtin", "un", "motivo", "score"],
  },
  "Revisao manual": {
    visible: ["select", "bloco", "desc", "camada", "motivo", "score"],
    order: ["select", "bloco", "desc", "camada", "motivo", "score"],
  },
  Score: {
    visible: ["select", "score", "motivo", "camada", "bloco", "desc"],
    order: ["select", "score", "motivo", "camada", "bloco", "desc"],
  },
};

// =========================================================
// Similaridade & Agregacao
// Aderente ao mockup contractual (sidebar metodo + tabela + detail panel)
// =========================================================
function SimilaridadeTab({ rows, stats }) {
  const [metodo, setMetodo] = useStateSim("particionamento");
  const [thresholds, setThresholds] = useStateSim({ c1: 50, c2: 65, c3: 80, c5: 70 });
  const [opts, setOpts] = useStateSim({ camada_desc: true, canon: false });
  const [query, setQuery] = useStateSim("");
  const [columnFilters, setColumnFilters] = useStateSim({});
  const [profileName, setProfileName] = useStateSim("Completo");
  const [selected, setSelected] = useStateSim(new Set(["1042", "1107"]));
  const [marked, setMarked] = useStateSim(new Set());
  const [ignored, setIgnored] = useStateSim(new Set());
  const [loading, setLoading] = useStateSim(false);
  // rowsLocal e statsLocal: iniciam com props (mock) e sao substituidos pela resposta da API
  const [rowsLocal, setRowsLocal] = useStateSim(rows);
  const [statsLocal, setStatsLocal] = useStateSim(stats);
  const [erroApi, setErroApi] = useStateSim(null);

  const visibleColumns = useMemoSim(() => (
    TableUtils.applyColumnProfile(
      similaridadeColumns,
      similaridadeProfiles[profileName] || similaridadeProfiles.Completo
    )
  ), [profileName]);

  const normalizedColumnFilters = useMemoSim(() => (
    Object.fromEntries(
      Object.entries(columnFilters).map(([key, value]) => [
        key,
        normalizeColumnFilter(key, value),
      ])
    )
  ), [columnFilters]);

  const filtered = useMemoSim(() => {
    const byQuery = rowsLocal.filter(r => TableUtils.rowMatchesFlexibleQuery(r, query));
    return TableUtils.applyColumnFilters(byQuery, normalizedColumnFilters);
  }, [rowsLocal, query, normalizedColumnFilters]);

  // Sort: ignored to bottom, otherwise stable
  const sorted = useMemoSim(() => {
    return [...filtered].sort((a, b) => {
      const ai = ignored.has(a.bloco) ? 1 : 0;
      const bi = ignored.has(b.bloco) ? 1 : 0;
      return ai - bi;
    });
  }, [filtered, ignored]);

  const selectedRow = useMemoSim(() => {
    if (selected.size === 0) return null;
    const id = [...selected][0];
    return rowsLocal.find(r => r.id === id);
  }, [selected, rowsLocal]);

  const blockMembers = useMemoSim(() => {
    if (!selectedRow) return [];
    return rowsLocal.filter(r => r.bloco === selectedRow.bloco);
  }, [selectedRow, rowsLocal]);

  function toggleSelect(id, ev) {
    setSelected(prev => {
      const next = new Set(prev);
      if (ev?.shiftKey || ev?.metaKey || ev?.ctrlKey) {
        if (next.has(id)) next.delete(id); else next.add(id);
      } else {
        next.clear();
        next.add(id);
      }
      return next;
    });
  }

  function updateColumnFilter(key, value) {
    setColumnFilters(prev => {
      const next = { ...prev };
      if (!value) delete next[key];
      else next[key] = value;
      return next;
    });
  }

  async function recalc() {
    setLoading(true);
    setErroApi(null);
    try {
      const payload = {
        rows: rowsLocal.map(r => ({
          id:   r.id,
          desc: r.desc,
          ncm:  r.ncm,
          cest: r.cest,
          gtin: r.gtin,
          un:   r.un,
        })),
        metodo,
        thresholds: {
          camada_1: thresholds.c1,
          camada_2: thresholds.c2,
          camada_3: thresholds.c3,
          camada_5: thresholds.c5,
        },
        opcoes: { camada_desc: opts.camada_desc, canon: opts.canon },
      };
      const result = await window.AUDIT_API.ordenarSimilaridade(payload);
      setRowsLocal(result.rows.map(r => ({
        id:     String(r.id     ?? ""),
        bloco:  r.bloco         ?? 0,
        desc:   r.desc          ?? r.descr_padrao ?? "",
        ncm:    r.ncm           ?? r.ncm_padrao   ?? "—",
        cest:   r.cest          ?? r.cest_padrao  ?? "—",
        gtin:   r.gtin          ?? r.gtin_padrao  ?? "—",
        un:     r.un            ?? r.unid_padrao  ?? "—",
        camada: r.camada        ?? 0,
        motivo: r.motivo        ?? "—",
        score:  r.score         ?? 0,
      })));
      const s = result.stats;
      setStatsLocal({
        n_linhas:    s.n_linhas,
        n_blocos:    s.n_blocos,
        por_camada:  Object.fromEntries(
          Object.entries(s.por_camada).map(([k, v]) => [Number(k), v])
        ),
        executou_ms: s.executou_ms,
      });
    } catch (err) {
      setErroApi(String(err));
      setTimeout(() => setErroApi(null), 6000);
      // fallback: simula recalculo com dados locais para nao travar a UI
      setTimeout(() => setLoading(false), 400);
      return;
    }
    setLoading(false);
  }

  return (
    <div className="sim-shell">
      {/* === LEFT: method sidebar === */}
      <aside className="sim-sidebar">
        <div className="method-group">
          <div className="section-label">método</div>
          {[
            { id: "composto", title: "Composto", pill: "legacy", desc: "Score ponderado por descrição, NCM, CEST, GTIN. Útil quando a base tem ruído nos identificadores." },
            { id: "particionamento", title: "Particionamento fiscal", pill: "novo", desc: "Agrupa por GTIN, NCM, CEST e UNIDADE primeiro. Texto entra apenas dentro de cada partição." },
            { id: "apenas_descricao", title: "Apenas descrição", pill: "cuidado", desc: "Inverted index sobre tokens. Identificadores fiscais ignorados — revisão manual essencial." },
          ].map(m => (
            <button key={m.id}
              className={`method-option ${metodo === m.id ? "is-active" : ""}`}
              onClick={() => setMetodo(m.id)}>
              <div className="method-title">
                {m.title} <span className="pill">{m.pill}</span>
              </div>
              <div className="method-desc">{m.desc}</div>
            </button>
          ))}
        </div>

        {metodo === "particionamento" && (
          <div className="method-group">
            <div className="section-label">thresholds</div>
            {[
              { k: "c1", label: <><span className="ncm-piece">NCM</span> + CEST + UN</> },
              { k: "c2", label: <><span className="ncm-piece">NCM</span> + UN</> },
              { k: "c3", label: <><span className="ncm-piece">NCM₄</span> + UN</> },
              { k: "c5", label: "texto puro" },
            ].map(t => (
              <div className="threshold-row" key={t.k}>
                <div className="threshold-label mono">{t.label}</div>
                <input className="threshold-input mono" type="number"
                  min="0" max="100" value={thresholds[t.k]}
                  onChange={e => setThresholds(s => ({...s, [t.k]: +e.target.value}))} />
              </div>
            ))}
          </div>
        )}

        {metodo === "apenas_descricao" && (
          <div className="method-group">
            <div className="warn-banner is-visible">
              <Icon name="warn" size={12}/>
              <div>
                <strong>Identificadores fiscais ignorados.</strong>
                <div>Revisão manual essencial — score acima de {thresholds.c5} é apenas indicativo.</div>
              </div>
            </div>
          </div>
        )}

        <div className="method-group">
          <div className="section-label">opções</div>
          <label className="toggle-row">
            <span className={`toggle ${opts.camada_desc ? "is-on" : ""}`}
              onClick={() => setOpts(o => ({...o, camada_desc: !o.camada_desc}))}/>
            <span className="toggle-text">
              <span className="toggle-title">Camada de descrição</span>
              <span className="toggle-desc">Para itens sem NCM, agrupa por texto</span>
            </span>
          </label>
          <label className="toggle-row">
            <span className={`toggle ${opts.canon ? "is-on" : ""}`}
              onClick={() => setOpts(o => ({...o, canon: !o.canon}))}/>
            <span className="toggle-text">
              <span className="toggle-title">Canonizar unidades</span>
              <span className="toggle-desc">Trata 0,5L = 500ML, 1KG = 1000G</span>
            </span>
          </label>
        </div>

        <button className="run-button" onClick={recalc} disabled={loading}>
          <span>{loading ? "Calculando…" : "Recalcular blocos"}</span>
          <span className="shortcut mono">⌘R</span>
        </button>

        {erroApi && (
          <div className="warn-banner is-visible" style={{marginTop: 8}}>
            <Icon name="alert" size={12}/>
            <div><strong>Erro na API:</strong><div style={{fontSize: 11}}>{erroApi}</div></div>
          </div>
        )}

        {marked.size > 0 && (
          <div className="marked-banner">
            <span><strong>{marked.size}</strong> linhas marcadas</span>
            <button className="btn primary sm">Agregar selecionados</button>
          </div>
        )}
      </aside>

      {/* === CENTER: data panel === */}
      <main className="sim-main">
        <div className="toolbar">
          <div className="title-block">
            <h1 className="page-title serif">
              Similaridade <span className="ampersand">&</span> agregação
            </h1>
            <div className="page-subtitle mono">
              {statsLocal.n_linhas.toLocaleString("pt-BR")} itens · {statsLocal.n_blocos} blocos · 4 camadas ativas · execução {(statsLocal.executou_ms / 1000).toFixed(1)}s
            </div>
          </div>
          <div className="toolbar-actions">
            <button className="btn"><Icon name="download" size={12}/> Exportar XLSX</button>
            <select className="select" value={profileName} onChange={e => setProfileName(e.target.value)}>
              {Object.keys(similaridadeProfiles).map(name => <option key={name}>{name}</option>)}
            </select>
            <button className="btn"><Icon name="columns" size={12}/> Colunas</button>
            <button className="btn"><Icon name="save" size={12}/> Salvar perfil</button>
          </div>
        </div>

        <div className="stats-strip">
          {[
            { v: statsLocal.por_camada[0], label: "camada 0 · gtin", cls: "" },
            { v: statsLocal.por_camada[1], label: "camada 1 · ncm+cest+un", cls: "" },
            { v: statsLocal.por_camada[2], label: "camada 2 · ncm+un", cls: "" },
            { v: statsLocal.por_camada[3], label: "camada 3 · ncm₄+un", cls: "" },
            { v: statsLocal.por_camada[5], label: "camada 5 · texto", cls: "dim" },
            { v: statsLocal.por_camada[4], label: "isolados", cls: "dim" },
          ].map((s, i) => (
            <div className="stat" key={i}>
              <div className={`stat-value mono ${s.cls}`}>{s.v}</div>
              <div className="stat-label">{s.label}</div>
            </div>
          ))}
        </div>

        <div className="data-table-shell">
          <div className="data-table-controls">
            <div className="search-wrap">
              <Icon name="search" size={12}/>
              <input className="input search-input"
                placeholder={'Busca flexivel: heineken -long ncm:22030000 score>=80 "lata 350"'}
                value={query} onChange={e => setQuery(e.target.value)} />
            </div>
            <span className="filter-chip">camada ≤ 3</span>
            <span className="filter-chip">bloco_size &gt; 1</span>
            <span className="filter-chip">score ≥ {thresholds.c1}</span>
            <div style={{flex: 1}}/>
            <span className="mono dim-text">{countActiveFilters(query, columnFilters)} filtros | {sorted.length} de {rowsLocal.length}</span>
          </div>
          <div className="data-table-scroll">
            <table className="data-table">
              <thead>
                <tr>
                  {visibleColumns.map(column => (
                    <th key={column.key} style={{
                      width: column.width,
                      textAlign: column.align || "left",
                    }}>
                      {column.key === "select" ? <input type="checkbox" className="checkbox"/> : column.label}
                    </th>
                  ))}
                </tr>
                <tr className="column-filter-row">
                  {visibleColumns.map(column => (
                    <th key={`${column.key}-filter`}>
                      {column.filter === false ? null : (
                        <input className="column-filter-input"
                          placeholder={column.numeric ? ">= 80" : `Filtrar ${column.label}`}
                          value={columnFilters[column.key] || ""}
                          onChange={e => updateColumnFilter(column.key, e.target.value)}
                          onClick={e => e.stopPropagation()} />
                      )}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {sorted.map(r => (
                  <tr key={r.id}
                    className={`${selected.has(r.id) ? "is-selected" : ""} ${ignored.has(r.bloco) ? "is-ignored" : ""}`}
                    onClick={(e) => toggleSelect(r.id, e)}>
                    {visibleColumns.map(column => renderSimilarityCell(column, r, { marked, setMarked }))}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      </main>

      {/* === RIGHT: detail panel === */}
      <aside className="sim-detail">
        {!selectedRow ? (
          <div className="detail-empty">
            <Icon name="target" size={28}/>
            <p>Selecione uma linha para ver o bloco fiscal e ações de revisão.</p>
          </div>
        ) : (
          <>
            <div className="detail-block-header">
              <div className="detail-bloco-marker" style={{ background: blocoColor(selectedRow.bloco) }}/>
              <div className="detail-block-title">
                <div className="detail-block-num mono">bloco #{selectedRow.bloco} · camada {selectedRow.camada}</div>
                <div className="detail-block-name serif">{summarizeBlock(blockMembers)}</div>
              </div>
            </div>

            <div className="detail-section">
              <div className="section-label">chave fiscal</div>
              <div className="fiscal-key">
                {selectedRow.gtin && selectedRow.gtin !== "—" && selectedRow.gtin !== "SEM GTIN" && (
                  <div className="key-pair">
                    <span className="key-name mono">GTIN</span>
                    <span className="key-value mono">{selectedRow.gtin}</span>
                  </div>
                )}
                {selectedRow.ncm !== "—" && (
                  <div className="key-pair">
                    <span className="key-name mono">NCM</span>
                    <span className="key-value mono">{selectedRow.ncm}</span>
                  </div>
                )}
                {selectedRow.cest !== "—" && (
                  <div className="key-pair">
                    <span className="key-name mono">CEST</span>
                    <span className="key-value mono">{selectedRow.cest}</span>
                  </div>
                )}
                <div className="key-pair">
                  <span className="key-name mono">UNIDADE</span>
                  <span className="key-value mono">{selectedRow.un}</span>
                </div>
              </div>
            </div>

            <div className="detail-section">
              <div className="section-label">{blockMembers.length} {blockMembers.length === 1 ? "item" : "itens"} neste bloco</div>
              <ul className="member-list">
                {blockMembers.map(m => (
                  <li key={m.id} className="member-item"
                    onClick={() => toggleSelect(m.id)}>
                    <span className="member-desc">{m.desc}</span>
                    <span className="member-id mono">id {m.id}</span>
                  </li>
                ))}
              </ul>
            </div>

            <div className="detail-section">
              <div className="section-label">motivo & score</div>
              <div className="fiscal-key">
                <div className="key-pair">
                  <span className="key-name mono">motivo</span>
                  <span className="key-value mono">{selectedRow.motivo}</span>
                </div>
                <div className="key-pair">
                  <span className="key-name mono">score composto</span>
                  <span className={`key-value mono ${scoreClass(selectedRow.score)}`}>{selectedRow.score}</span>
                </div>
              </div>
            </div>

            <div className="action-bar">
              <button className="btn primary"
                onClick={() => setMarked(p => {
                  const n = new Set(p);
                  blockMembers.forEach(m => n.add(m.id));
                  return n;
                })}>
                Marcar para agregar
              </button>
              <button className="btn ghost"
                onClick={() => setIgnored(p => {
                  const n = new Set(p);
                  n.has(selectedRow.bloco) ? n.delete(selectedRow.bloco) : n.add(selectedRow.bloco);
                  return n;
                })}>
                {ignored.has(selectedRow.bloco) ? "Desfazer ignorar" : "Ignorar bloco"}
              </button>
            </div>

            {metodo === "apenas_descricao" && (
              <div className="warn-banner is-visible" style={{marginTop: 16}}>
                <Icon name="warn" size={12}/>
                <div>
                  <strong>Camada 5 ativa.</strong>
                  <div>identificadores fiscais não foram consultados.</div>
                </div>
              </div>
            )}
          </>
        )}
      </aside>
    </div>
  );
}

function summarizeBlock(members) {
  if (!members.length) return "—";
  const first = members[0].desc;
  // simple heuristic: take first 3 strong tokens
  const tokens = first.split(/\s+/).filter(t => t.length > 2 && !/^\d+$/.test(t)).slice(0, 3);
  return tokens.join(" ").toLowerCase().replace(/\b\w/g, c => c.toUpperCase());
}

function normalizeColumnFilter(key, value) {
  const numericColumns = new Set(["bloco", "camada", "score"]);
  const text = String(value || "").trim();
  if (!text) return { value: "" };
  if (numericColumns.has(key)) {
    const match = text.match(/^(>=|<=|>|<|=)?\s*(-?\d+(?:[.,]\d+)?)$/);
    if (match) {
      return {
        operator: match[1] || "=",
        value: match[2],
      };
    }
  }
  return { operator: "contains", value: text };
}

function countActiveFilters(query, filters) {
  return (String(query || "").trim() ? 1 : 0) +
    Object.values(filters || {}).filter(v => String(v || "").trim()).length;
}

function renderSimilarityCell(column, row, ctx) {
  if (column.key === "select") {
    return (
      <td key={column.key}>
        <input type="checkbox" className="checkbox"
          checked={ctx.marked.has(row.id)}
          onChange={e => {
            e.stopPropagation();
            ctx.setMarked(prev => {
              const next = new Set(prev);
              next.has(row.id) ? next.delete(row.id) : next.add(row.id);
              return next;
            });
          }}
          onClick={e => e.stopPropagation()}/>
      </td>
    );
  }
  if (column.key === "bloco") {
    return (
      <td key={column.key} className="col-bloco">
        <span className="bloco-marker" style={{ background: blocoColor(row.bloco) }}/>
        {row.bloco}
      </td>
    );
  }
  if (column.key === "un") {
    return <td key={column.key}><span className="unit-badge">{row.un}</span></td>;
  }
  if (column.key === "camada") {
    return <td key={column.key} className="col-camada"><CamadaBadge c={row.camada}/></td>;
  }
  if (column.key === "score") {
    return <td key={column.key} className={`col-score ${scoreClass(row.score)}`}>{row.score || "â€”"}</td>;
  }
  return (
    <td key={column.key} className={column.className || ""}>
      {row[column.key] ?? "â€”"}
    </td>
  );
}

window.SimilaridadeTab = SimilaridadeTab;
