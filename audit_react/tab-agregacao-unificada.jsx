/* global React, AgregacaoTab, SimilaridadeTab */
// Aba unificada: agregação clássica (filtros NCM/CEST) + similaridade (particionamento)
// servem ao mesmo proposito — agrupar produtos equivalentes.
const { useState: useStateAU } = React;

function AgregacaoUnificadaTab({ rows, stats }) {
  const [modo, setModo] = useStateAU("similaridade");
  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 14, flex: 1, minHeight: 0 }}>
      <div className="modo-bar">
        <div className="modo-bar-left">
          <span className="modo-bar-label">modo de agregação</span>
          <div className="seg-control modo-seg">
            <button
              className={modo === "similaridade" ? "is-active" : ""}
              onClick={() => setModo("similaridade")}>
              Particionamento por similaridade
            </button>
            <button
              className={modo === "classico" ? "is-active" : ""}
              onClick={() => setModo("classico")}>
              Filtros NCM / CEST (clássico)
            </button>
          </div>
        </div>
        <div className="modo-bar-hint mono">
          {modo === "similaridade"
            ? "agrupa por blocos fiscais com camadas e thresholds — recomendado"
            : "filtragem manual + reprocessamento de descrições padrão"}
        </div>
      </div>

      <div style={{ flex: 1, minHeight: 0, display: "flex", flexDirection: "column" }}>
        {modo === "similaridade"
          ? <SimilaridadeTab rows={rows} stats={stats}/>
          : <AgregacaoTab rows={rows}/>}
      </div>
    </div>
  );
}

window.AgregacaoUnificadaTab = AgregacaoUnificadaTab;
