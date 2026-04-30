/* global React, Icon, Pill, scoreClass, DataGrid */
const { useState: useS2, useMemo: useM2 } = React;

const agregacaoColumns = [
  {
    key: "__select",
    label: "Visto",
    width: 48,
    filter: false,
    render: (row) => row.__select,
  },
  { key: "id_agrupado", label: "id_agrupado", className: "col-fiscal" },
  { key: "id_descricao", label: "id_descricao", className: "col-fiscal" },
  { key: "descr_padrao", label: "descr_padrao", className: "col-desc" },
  { key: "ncm_padrao", label: "ncm_padrao", className: "col-fiscal" },
  { key: "cest_padrao", label: "cest_padrao", className: "col-fiscal" },
  { key: "gtin_padrao", label: "gtin_padrao", className: "col-fiscal" },
  { key: "unidade", label: "unidade", width: 80 },
  { key: "lista_ncm", label: "lista_ncm", className: "col-fiscal" },
  { key: "lista_cest", label: "lista_cest", className: "col-fiscal" },
  { key: "motivo", label: "motivo", className: "col-motivo" },
  { key: "score", label: "score", numeric: true, align: "right", className: "col-score" },
];

const agregadasColumns = [
  { key: "id_agrupado", label: "id_agrupado", className: "col-fiscal" },
  { key: "descr_padrao", label: "descr_padrao", className: "col-desc" },
  { key: "ncm_padrao", label: "ncm_padrao", className: "col-fiscal" },
  { key: "cest_padrao", label: "cest_padrao", className: "col-fiscal" },
  { key: "gtin_padrao", label: "gtin_padrao", className: "col-fiscal" },
  { key: "n_itens", label: "n_itens", numeric: true, align: "right", className: "col-num" },
  { key: "fonte", label: "fonte", filter: false, render: () => <Pill kind="info">manual</Pill> },
];

const agregacaoProfiles = {
  Completo: {
    visible: null,
    order: ["__select", "id_agrupado", "id_descricao", "descr_padrao", "ncm_padrao", "cest_padrao", "gtin_padrao", "unidade", "lista_ncm", "lista_cest", "motivo", "score"],
  },
  Fiscal: {
    visible: ["__select", "descr_padrao", "ncm_padrao", "cest_padrao", "gtin_padrao", "unidade", "score"],
    order: ["__select", "descr_padrao", "ncm_padrao", "cest_padrao", "gtin_padrao", "unidade", "score"],
  },
  "Revisao manual": {
    visible: ["__select", "id_agrupado", "descr_padrao", "motivo", "score"],
    order: ["__select", "id_agrupado", "descr_padrao", "motivo", "score"],
  },
  Chaves: {
    visible: ["__select", "id_agrupado", "id_descricao", "ncm_padrao", "cest_padrao", "gtin_padrao"],
    order: ["__select", "id_agrupado", "id_descricao", "ncm_padrao", "cest_padrao", "gtin_padrao"],
  },
};

const conversaoColumns = [
  { key: "__idx", label: "#", width: 52, numeric: true, align: "right", className: "col-num dim-text" },
  { key: "id_agr", label: "id_agrupado", className: "col-fiscal" },
  { key: "id_prod", label: "id_produtos", className: "col-fiscal" },
  { key: "desc", label: "descr_padrao", className: "col-desc" },
  { key: "lista", label: "lista_descricoes_produto", className: "col-desc dim-text" },
  { key: "unid", label: "unid", width: 76, render: row => <span className="unit-badge">{row.unid}</span> },
  { key: "unid_ref", label: "unid_ref", width: 92, render: row => <span className="unit-badge accent">{row.unid_ref}</span> },
  { key: "fator", label: "fator", numeric: true, align: "right", className: "col-num", render: row => row.fator == null ? "-" : row.fator.toFixed(2).replace(".", ",") },
  { key: "preco", label: "preco_medio", numeric: true, align: "right", className: "col-num", render: row => row.preco == null ? "-" : row.preco.toFixed(2).replace(".", ",") },
];

const conversaoProfiles = {
  Completo: { visible: null, order: conversaoColumns.map(column => column.key) },
  Operacional: {
    visible: ["id_agr", "desc", "unid", "unid_ref", "fator", "preco"],
    order: ["id_agr", "desc", "unid", "unid_ref", "fator", "preco"],
  },
  Chaves: {
    visible: ["id_agr", "id_prod", "desc", "unid", "unid_ref"],
    order: ["id_agr", "id_prod", "desc", "unid", "unid_ref"],
  },
};

const estoqueColumns = [
  { key: "ord", label: "ordem_op.", width: 96, numeric: true, align: "right", className: "col-num mono" },
  { key: "tipo", label: "Tipo_operacao", className: "mono" },
  { key: "fonte", label: "fonte", render: row => <Pill kind={String(row.tipo || "").includes("ENTRADA") ? "info" : ""}>{row.fonte}</Pill> },
  { key: "desc", label: "Descr_item", className: "col-desc" },
  { key: "nsu", label: "nsu", className: "col-fiscal" },
  { key: "chv", label: "Chv_nfe", className: "col-fiscal" },
  { key: "mod", label: "mod", className: "col-fiscal" },
  { key: "ser", label: "Ser", className: "col-fiscal" },
  { key: "nu", label: "nu", className: "col-fiscal" },
];

const estoqueProfiles = {
  Completo: { visible: null, order: estoqueColumns.map(column => column.key) },
  Documento: {
    visible: ["ord", "tipo", "fonte", "nsu", "chv", "mod", "ser", "nu"],
    order: ["ord", "tipo", "fonte", "nsu", "chv", "mod", "ser", "nu"],
  },
  Produto: {
    visible: ["ord", "tipo", "fonte", "desc"],
    order: ["ord", "tipo", "fonte", "desc"],
  },
};

const consultaColumns = [
  { key: "name", label: "artefato", className: "col-fiscal" },
  { key: "group", label: "grupo", width: 120 },
  { key: "rows", label: "linhas", numeric: true, align: "right", className: "col-num" },
  { key: "size", label: "tamanho", width: 100, align: "right", className: "mono" },
  { key: "status", label: "status", width: 140 },
];

const consultaProfiles = {
  Completo: { visible: null, order: consultaColumns.map(column => column.key) },
  Operacional: {
    visible: ["name", "group", "rows", "size", "status"],
    order: ["name", "group", "rows", "size", "status"],
  },
};

// =========================================================
// Aba Agregacao classica — tabela agrupada + linhas agregadas
// =========================================================
function AgregacaoTab({ rows }) {
  const [matchMode, setMatchMode] = useS2("ncm_cest");
  const [selected, setSelected] = useS2(new Set());
  const [agregadas] = useS2(rows.slice(0, 3).map(r => r.id));

  const toggle = (id) => setSelected(prev => {
    const next = new Set(prev);
    next.has(id) ? next.delete(id) : next.add(id);
    return next;
  });

  const tableRows = useM2(() => rows.map(row => ({
    ...row,
    id_agrupado: `id_descricao_${row.id}`,
    id_descricao: row.id,
    descr_padrao: row.desc,
    ncm_padrao: row.ncm,
    cest_padrao: row.cest,
    gtin_padrao: row.gtin,
    unidade: row.un || "UN",
    lista_ncm: row.ncm,
    lista_cest: row.cest,
    motivo: row.motivo || "manual",
    __select: (
      <input type="checkbox" className="checkbox"
        checked={selected.has(row.id)}
        onChange={() => toggle(row.id)}
        onClick={event => event.stopPropagation()} />
    ),
  })), [rows, selected]);

  const agregadasRows = useM2(() => rows
    .filter(row => agregadas.includes(row.id))
    .map(row => ({
      id_agrupado: `id_descricao_${row.id}`,
      descr_padrao: row.desc,
      ncm_padrao: row.ncm,
      cest_padrao: row.cest,
      gtin_padrao: row.gtin,
      n_itens: 2,
      fonte: "manual",
    })), [rows, agregadas]);

  return (
    <div className="agreg-shell">
      <SectionPanel
        title="Tabela Agrupada Filtravel"
        subtitle="Selecione linhas para agregar"
        actions={
          <>
            <button className="btn">Abrir tabela agrupada</button>
            <button className="btn">Agregar Descricoes ({selected.size})</button>
            <button className="btn primary">Reprocessar</button>
          </>
        }>
        <div className="filter-bar">
          <div className="seg-control">
            <button className={matchMode === "ncm_cest" ? "is-active" : ""} onClick={() => setMatchMode("ncm_cest")}>NCM+CEST iguais</button>
            <button className={matchMode === "ncm_cest_gtin" ? "is-active" : ""} onClick={() => setMatchMode("ncm_cest_gtin")}>NCM+CEST+GTIN iguais</button>
          </div>
          <button className="btn primary">Destacar</button>
          <span className="mono dim-text">busca flexivel e filtros por coluna no cabecalho da tabela</span>
        </div>

        <DataGrid
          tableId="agregacao-manual"
          rows={tableRows}
          columns={agregacaoColumns}
          profiles={agregacaoProfiles}
          defaultProfile="Completo"
          rowKey="id"
          selectedIds={selected}
          onRowClick={(row) => toggle(row.id)}
          rowClassName={(row) => selected.has(row.id) ? "is-selected" : ""}
          maxHeight={420}
          emptyText="Nenhum item encontrado para agregacao manual." />
      </SectionPanel>

      <SectionPanel
        title="Linhas Agregadas"
        subtitle="Mesma Tabela de Referencia"
        actions={
          <>
            <button className="btn">Reverter agrupamento</button>
            <button className="btn">Desfazer selecao</button>
          </>
        }>
        <div className="filter-bar">
          <div className="seg-control">
            <button className="is-active">NCM+CEST iguais</button>
            <button>NCM+CEST+GTIN iguais</button>
          </div>
          <button className="btn primary">Destacar</button>
          <span className="mono dim-text">mesmos perfis e filtros aplicados na tabela de referencia</span>
        </div>

        <DataGrid
          tableId="agregacao-linhas-agregadas"
          rows={agregadasRows}
          columns={agregadasColumns}
          profiles={agregacaoProfiles}
          defaultProfile="Fiscal"
          rowKey="id_agrupado"
          maxHeight={260}
          emptyText="Nenhuma linha agregada." />
      </SectionPanel>
    </div>
  );
}

// =========================================================
// Aba Conversao
// =========================================================
function ConversaoTab({ rows }) {
  const [showUniq, setShowUniq] = useS2(false);
  const [filterAgr, setFilterAgr] = useS2("");
  const [filterDesc, setFilterDesc] = useS2("");
  const [selectedRow, setSelectedRow] = useS2(null);
  const [novaUnidRef, setNovaUnidRef] = useS2("");

  const visible = rows.filter(r =>
    (!filterAgr || r.id_agr.includes(filterAgr)) &&
    (!filterDesc || r.desc.toLowerCase().includes(filterDesc.toLowerCase()))
  );

  return (
    <div className="conversao-shell">
      <div className="conversao-toolbar">
        <button className="btn"><Icon name="refresh" size={11}/> Recarregar</button>
        <label className="checkbox-label">
          <input type="checkbox" className="checkbox" checked={showUniq}
            onChange={e => setShowUniq(e.target.checked)}/>
          Mostrar itens de unidade única
        </label>
        <div style={{flex: 1}}/>
        <button className="btn primary">Recalcular fatores</button>
        <select className="select" style={{maxWidth: 130}}><option>Padrão</option></select>
        <button className="btn">Perfil</button>
        <button className="btn">Salvar perfil</button>
        <button className="btn">Colunas</button>
        <button className="btn">Destacar</button>
        <button className="btn"><Icon name="download" size={11}/> Importar Excel</button>
      </div>

      <div className="filter-bar">
        <select className="select" style={{maxWidth: 220}}>
          <option>Filtrar id_agrupado</option>
        </select>
        <input className="input" placeholder="Filtrar descr_padrao"
          value={filterDesc} onChange={e => setFilterDesc(e.target.value)}/>
      </div>

      <div className="alterar-unidref">
        <div className="section-label">Alterar Unidade de Referência do Produto Selecionado</div>
        <div className="alterar-unidref-row">
          <span className="mono dim-text">
            {selectedRow ? `→ ${selectedRow.desc}` : "Nenhum produto selecionado"}
          </span>
          <span className="mono dim-text">→</span>
          <span className="mono">Nova unid_ref:</span>
          <select className="select" style={{width: 100}}
            value={novaUnidRef} onChange={e => setNovaUnidRef(e.target.value)}>
            <option value="">—</option>
            <option>UN</option><option>CX</option><option>KG</option><option>L</option><option>PCT</option>
          </select>
          <button className="btn" disabled={!selectedRow || !novaUnidRef}>Aplicar a todos os itens</button>
        </div>
      </div>

      <div className="data-table-shell" style={{flex: 1, minHeight: 0}}>
        <div className="data-table-scroll">
          <table className="data-table">
            <thead>
              <tr>
                <th style={{width: 40}}>#</th>
                <th>id_agrupado</th>
                <th>id_produtos</th>
                <th>descr_padrao</th>
                <th>lista_descricoes_produto</th>
                <th>unid</th>
                <th>unid_ref</th>
                <th style={{textAlign: "right"}}>fator</th>
                <th style={{textAlign: "right"}}>preco_medio</th>
              </tr>
            </thead>
            <tbody>
              {visible.map((r, i) => (
                <tr key={i}
                  className={selectedRow === r ? "is-selected" : ""}
                  onClick={() => setSelectedRow(r)}>
                  <td className="col-num dim-text">{i + 1}</td>
                  <td className="col-fiscal">{r.id_agr}</td>
                  <td className="col-fiscal">{r.id_prod}</td>
                  <td className="col-desc">{r.desc}</td>
                  <td className="col-desc dim-text">{r.lista}</td>
                  <td><span className="unit-badge">{r.unid}</span></td>
                  <td><span className="unit-badge accent">{r.unid_ref}</span></td>
                  <td className={`col-num ${r.fator == null ? "muted" : ""}`}>
                    {r.fator == null ? "—" : r.fator.toFixed(2).replace(".", ",")}
                  </td>
                  <td className="col-num">
                    {r.preco == null ? "—" : r.preco.toFixed(2).replace(".", ",")}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>

      <div className="status-strip">
        <Icon name="info" size={11}/>
        Carregando fatores de conversão · {visible.length} de {rows.length} linhas
      </div>
    </div>
  );
}

// =========================================================
// Aba Estoque
// =========================================================
function EstoqueTab({ rows }) {
  const [subTab, setSubTab] = useS2("mov");
  const subTabs = [
    { id: "mov", label: "Tabela mov_estoque", count: 30005 },
    { id: "mensal", label: "Tabela mensal", count: 1875 },
    { id: "anual", label: "Tabela anual", count: 156 },
    { id: "periodos", label: "Tabela períodos", count: 12 },
    { id: "resumo", label: "Resumo Global", count: null },
    { id: "selecionados", label: "Produtos selecionados", count: 1325 },
    { id: "agrupados", label: "id_agrupados", count: 1325 },
  ];

  return (
    <div className="estoque-shell">
      <div className="sub-tab-bar">
        {subTabs.map(t => (
          <button key={t.id}
            className={`sub-tab ${subTab === t.id ? "is-active" : ""}`}
            onClick={() => setSubTab(t.id)}>
            <span>{t.label}</span>
            {t.count != null && <span className="mono sub-tab-count">{t.count.toLocaleString("pt-BR")}</span>}
          </button>
        ))}
      </div>

      <div className="estoque-meta">
        <div className="section-label" style={{margin: 0}}>Tabela: mov_estoque</div>
      </div>

      <div className="filter-bar">
        <select className="select" style={{maxWidth: 200}}><option>Filtrar id_agrupado</option></select>
        <input className="input" placeholder="Filtrar descrição"/>
        <input className="input mono" placeholder="Filtrar NCM"/>
        <select className="select" style={{maxWidth: 100}}><option>Todos</option></select>
        <input className="input" placeholder="Busca geral…"/>
        <button className="btn primary">Exportar Excel</button>
      </div>

      <div className="filter-bar">
        <select className="select" style={{maxWidth: 130}}><option>Data</option></select>
        <select className="select" style={{maxWidth: 130}}><option>Dt_doc</option></select>
        <select className="select" style={{maxWidth: 140}}><option>Data inicial</option></select>
        <select className="select" style={{maxWidth: 130}}><option>Data final</option></select>
        <input className="input mono" placeholder="Número"/>
        <select className="select" style={{maxWidth: 170}}><option>saldo_estoque_anual</option></select>
        <input className="input mono" placeholder="Min numérico"/>
        <input className="input mono" placeholder="Max numérico"/>
        <select className="select" style={{maxWidth: 110}}><option>Padrão</option></select>
        <button className="btn">Perfil</button>
        <button className="btn">Colunas</button>
        <button className="btn primary">Destacar</button>
      </div>

      <div className="estoque-status mono dim-text">
        Movimentações: 30.005 de 30.005 linhas · Filtros ativos: nenhum
      </div>

      <div className="data-table-shell" style={{flex: 1, minHeight: 0}}>
        <div className="data-table-scroll">
          <table className="data-table">
            <thead>
              <tr>
                <th style={{width: 80}}>ordem_op.</th>
                <th>Tipo_operacao</th>
                <th>fonte</th>
                <th>Descr_item</th>
                <th>Descr_compl</th>
                <th>nsu</th>
                <th>Chv_nfe</th>
                <th>mod</th>
                <th>Ser</th>
                <th>nu</th>
              </tr>
            </thead>
            <tbody>
              {rows.map((r, i) => {
                const isEntrada = r.tipo.includes("ENTRADA");
                const isInicial = r.tipo.includes("INICIAL");
                const isFinal = r.tipo.includes("FINAL");
                let cls = "";
                if (isEntrada) cls = "row-entrada";
                else if (isInicial) cls = "row-inicial";
                else if (isFinal) cls = "row-final";
                return (
                  <tr key={i} className={cls}>
                    <td className="col-num mono">{r.ord.toLocaleString("pt-BR")}</td>
                    <td className="mono">{r.tipo}</td>
                    <td><Pill kind={isEntrada ? "info" : ""}>{r.fonte}</Pill></td>
                    <td className="col-desc">{r.desc}</td>
                    <td/>
                    <td className="col-fiscal">{r.nsu}</td>
                    <td className="col-fiscal">{r.chv}</td>
                    <td className="col-fiscal">{r.mod}</td>
                    <td className="col-fiscal">{r.ser}</td>
                    <td className="col-fiscal">{r.nu}</td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
}

// =========================================================
// Aba Configuracoes
// =========================================================
function ConfiguracoesTab() {
  return (
    <div className="config-shell">
      <SectionPanel title="Status da Conexão Oracle" actions={
        <button className="btn primary"><Icon name="wifi" size={11}/> Verificar Conexões</button>
      }>
        <div className="status-grid">
          <div className="status-row">
            <div className="status-label">
              <span className="dot success"/>
              <strong>Conexão 1 — Principal</strong>
            </div>
            <div className="status-value success">
              <Icon name="check" size={11}/> Conexão OK <span className="mono">(125 ms)</span>
            </div>
          </div>
          <div className="status-row">
            <div className="status-label">
              <span className="dot danger"/>
              <strong>Conexão 2 — Secundária</strong>
            </div>
            <div className="status-value danger">
              <Icon name="x" size={11}/> ORA-01017: credencial inválida; logon negado
            </div>
          </div>
        </div>
      </SectionPanel>

      <SectionPanel title="Conexão Oracle 1 — Principal">
        <FormGrid>
          <FormField label="Host" value="exa01-scan.sefin.ro.gov.br"/>
          <FormField label="Porta" value="1521" mono small/>
          <FormField label="Serviço" value="sefindw"/>
          <FormField label="Usuário" value="03002693901"/>
          <FormField label="Senha" value="••••••••••••" type="password"/>
          <FormField label="Teste" custom={<button className="btn primary">Testar Conexão</button>} note="—"/>
        </FormGrid>
      </SectionPanel>

      <SectionPanel title="Conexão Oracle 2 — Secundária">
        <FormGrid>
          <FormField label="Host" value="exacc-x10-sefinscan.sefin.ro.gov.br"/>
          <FormField label="Porta" value="1521" mono small/>
          <FormField label="Serviço" value="svc.bi.users"/>
          <FormField label="Usuário" value="your_db_user_here" muted/>
          <FormField label="Senha" value="••••••••••••••••••••••••••" type="password"/>
          <FormField label="Teste" custom={<button className="btn">Testar Conexão</button>} note="—"/>
        </FormGrid>
      </SectionPanel>

      <SectionPanel title="Configurações do Aplicativo">
        <FormGrid>
          <FormField label="Nível de log" custom={
            <select className="select"><option>INFO</option><option>WARN</option><option>DEBUG</option></select>
          }/>
          <FormField label="Cache" custom={
            <label className="checkbox-label">
              <input type="checkbox" className="checkbox" defaultChecked/> Ativar cache
            </label>
          }/>
          <FormField label="TTL do cache (s)" value="3600" mono/>
          <FormField label="Tema do dashboard" custom={
            <select className="select"><option>dark</option><option>light</option></select>
          }/>
        </FormGrid>
      </SectionPanel>
    </div>
  );
}

// =========================================================
// Aba Consulta — abrir parquet, filtrar, exportar
// =========================================================
function ConsultaTab({ parquets }) {
  const [open, setOpen] = useS2(parquets[4]);
  return (
    <div className="consulta-shell">
      <SectionPanel title="Parquets disponíveis" subtitle="Clique para abrir como tabela ativa">
        <div className="parquet-cards">
          {parquets.slice(0, 8).map(p => (
            <button key={p.name}
              className={`parquet-card ${open?.name === p.name ? "is-active" : ""}`}
              onClick={() => setOpen(p)}>
              <div className="parquet-card-name mono">{p.name}</div>
              <div className="parquet-card-meta mono">
                {p.rows == null ? "?" : p.rows.toLocaleString("pt-BR")} linhas · {p.size}
              </div>
            </button>
          ))}
        </div>
      </SectionPanel>

      <SectionPanel
        title={`Tabela ativa: ${open?.name || "—"}`}
        subtitle={open?.loc}
        actions={
          <>
            <button className="btn">Colunas (47/47)</button>
            <button className="btn">Salvar perfil</button>
            <button className="btn primary"><Icon name="download" size={11}/> Exportar</button>
          </>
        }>
        <div className="filter-bar">
          <input className="input" placeholder="Busca global…"/>
          <span className="filter-chip">item ≠ null</span>
          <span className="filter-chip">data ≥ 2024-01-01</span>
        </div>
        <div className="placeholder-table">
          <Icon name="db" size={48}/>
          <div className="serif" style={{fontSize: 22, marginTop: 12}}>Pronto para abrir</div>
          <div className="dim-text" style={{marginTop: 4}}>
            {open?.rows == null ? "?" : open.rows.toLocaleString("pt-BR")} linhas serão carregadas via Polars.
          </div>
          <button className="btn primary" style={{marginTop: 16}}><Icon name="play" size={11}/> Carregar tabela</button>
        </div>
      </SectionPanel>
    </div>
  );
}

// =========================================================
// Aba Consulta SQL
// =========================================================
function ConsultaSQLTab() {
  const [sql, setSql] = useS2(
`-- C176 mensal: mercadorias com ST por mês
SELECT
    cnpj,
    ncm_padrao,
    cest_padrao,
    EXTRACT(MONTH FROM dt_doc) AS mes,
    SUM(qtde) AS qtde_total,
    SUM(vl_item) AS vl_total
FROM c176_xml
WHERE cnpj = :cnpj
  AND dt_doc BETWEEN :di AND :df
GROUP BY cnpj, ncm_padrao, cest_padrao, EXTRACT(MONTH FROM dt_doc)
ORDER BY mes, ncm_padrao;`);
  return (
    <div className="sql-shell">
      <div className="sql-toolbar">
        <select className="select" style={{maxWidth: 220}}>
          <option>Conexão 1 — Principal (sefindw)</option>
          <option>Conexão 2 — Secundária</option>
          <option>Polars local (parquet)</option>
        </select>
        <select className="select" style={{maxWidth: 200}}>
          <option>c176_mensal.sql</option>
          <option>c170.sql</option>
          <option>NFe.sql</option>
          <option>fronteira.sql</option>
        </select>
        <div style={{flex: 1}}/>
        <button className="btn"><Icon name="save" size={11}/> Salvar como…</button>
        <button className="btn primary"><Icon name="play" size={11}/> Executar (F5)</button>
      </div>
      <div className="sql-editor">
        <div className="sql-gutter mono">
          {sql.split("\n").map((_, i) => <div key={i}>{i + 1}</div>)}
        </div>
        <textarea className="sql-textarea mono"
          value={sql} onChange={e => setSql(e.target.value)} spellCheck={false}/>
      </div>
      <div className="sql-results">
        <div className="section-label">resultado · 1.247 linhas · 384 ms</div>
        <div className="placeholder-table compact">
          <Icon name="code" size={32}/>
          <div className="dim-text" style={{marginTop: 8, fontSize: 12}}>
            Execute a query para ver os resultados aqui.
          </div>
        </div>
      </div>
    </div>
  );
}

// =========================================================
// Aba NFe Entrada
// =========================================================
function NFeEntradaTab() {
  return (
    <div className="placeholder-tab">
      <SectionPanel title="NFe — Documentos de Entrada" subtitle="2.188 documentos · período 2024-Q4">
        <div className="filter-bar">
          <input className="input" placeholder="Filtrar emitente"/>
          <input className="input mono" placeholder="CNPJ emitente"/>
          <input className="input" placeholder="Filtrar produto"/>
          <button className="btn primary"><Icon name="download" size={11}/> Exportar</button>
        </div>
        <div className="placeholder-table">
          <Icon name="file" size={36}/>
          <div className="serif" style={{fontSize: 22, marginTop: 12}}>2.188 NFe de entrada</div>
          <div className="dim-text">Selecione filtros e carregue para visualizar itens.</div>
        </div>
      </SectionPanel>
    </div>
  );
}

// =========================================================
// Aba Analise Lote CNPJ
// =========================================================
function LoteTab() {
  const cnpjs = ["84654326000394", "37671507000187", "63614176000153", "12880843000931"];
  return (
    <div className="placeholder-tab">
      <SectionPanel title="Análise em Lote por CNPJ" subtitle="Processa múltiplos CNPJs em paralelo"
        actions={
          <>
            <button className="btn">Adicionar CNPJ</button>
            <button className="btn primary"><Icon name="play" size={11}/> Iniciar lote</button>
          </>
        }>
        <div className="lote-list">
          {cnpjs.map((c, i) => (
            <div key={c} className="lote-row">
              <span className="mono">{c}</span>
              <span className="lote-progress">
                <span className="lote-bar" style={{width: `${[100, 72, 45, 0][i]}%`}}/>
              </span>
              <span className={`mono lote-status ${i === 0 ? "success" : i === 3 ? "muted" : "info"}`}>
                {i === 0 ? "Concluído" : i === 3 ? "Aguardando" : `${[100, 72, 45, 0][i]}%`}
              </span>
            </div>
          ))}
        </div>
      </SectionPanel>
    </div>
  );
}

// =========================================================
// Aba Logs
// =========================================================
function LogsTab({ lines }) {
  const [level, setLevel] = useS2("ALL");
  const visible = lines.filter(l => level === "ALL" || l.lvl === level);
  return (
    <div className="logs-shell">
      <div className="logs-toolbar">
        <div className="seg-control">
          {["ALL", "INFO", "WARN", "ERROR"].map(l => (
            <button key={l}
              className={level === l ? "is-active" : ""}
              onClick={() => setLevel(l)}>{l}</button>
          ))}
        </div>
        <input className="input" placeholder="Filtrar mensagem ou source…" style={{maxWidth: 320}}/>
        <div style={{flex: 1}}/>
        <button className="btn"><Icon name="refresh" size={11}/> Atualizar</button>
        <button className="btn"><Icon name="download" size={11}/> Salvar log</button>
      </div>
      <div className="logs-stream">
        {visible.map((l, i) => (
          <div key={i} className={`log-line lvl-${l.lvl}`}>
            <span className="log-ts mono">{l.ts}</span>
            <span className={`log-lvl mono lvl-${l.lvl}`}>{l.lvl.padEnd(5)}</span>
            <span className="log-src mono">{l.src.padEnd(13)}</span>
            <span className="log-msg">{l.msg}</span>
          </div>
        ))}
      </div>
    </div>
  );
}

// =========================================================
// Helpers
// =========================================================
function SectionPanel({ title, subtitle, actions, children }) {
  return (
    <section className="panel">
      <header className="panel-header">
        <div className="panel-title-block">
          <h3 className="panel-title">{title}</h3>
          {subtitle && <div className="panel-subtitle dim-text">{subtitle}</div>}
        </div>
        {actions && <div className="panel-actions">{actions}</div>}
      </header>
      <div className="panel-body">{children}</div>
    </section>
  );
}

function FormGrid({ children }) {
  return <div className="form-grid">{children}</div>;
}

function FormField({ label, value, type, mono, small, muted, note, custom }) {
  return (
    <div className={`form-field ${small ? "small" : ""}`}>
      <label className="form-label">{label}</label>
      {custom ? custom : (
        <input className={`input ${mono ? "mono" : ""} ${muted ? "muted" : ""}`}
          type={type || "text"} defaultValue={value}/>
      )}
      {note && <span className="form-note dim-text">{note}</span>}
    </div>
  );
}

Object.assign(window, {
  AgregacaoTab, ConversaoTab, EstoqueTab, ConfiguracoesTab,
  ConsultaTab, ConsultaSQLTab, NFeEntradaTab, LoteTab, LogsTab,
  SectionPanel
});
