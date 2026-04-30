/* global React, window */
const { useMemo: useMemoGrid, useState: useStateGrid, useEffect: useEffectGrid } = React;

function DataGrid({
  tableId,
  rows,
  columns,
  profiles,
  defaultProfile = "Completo",
  rowKey = "id",
  selectedIds = new Set(),
  onRowClick,
  rowClassName,
  maxHeight,
  emptyText = "Nenhum registro encontrado.",
}) {
  const utils = window.AUDIT_TABLE_UTILS;
  const profileStorageKey = `audit.tableProfile.${tableId}`;
  const [query, setQuery] = useStateGrid("");
  const [columnFilters, setColumnFilters] = useStateGrid({});
  const [profileName, setProfileName] = useStateGrid(() => {
    try {
      const saved = window.localStorage?.getItem(profileStorageKey);
      return saved && profiles?.[saved] ? saved : defaultProfile;
    } catch {
      return defaultProfile;
    }
  });
  const [columnPanelOpen, setColumnPanelOpen] = useStateGrid(false);
  const [manualVisible, setManualVisible] = useStateGrid(null);
  const [sortState, setSortState] = useStateGrid({ key: null, direction: "asc" });

  useEffectGrid(() => {
    try {
      window.localStorage?.setItem(profileStorageKey, profileName);
    } catch {
      /* localStorage can be unavailable in restricted contexts. */
    }
    setManualVisible(null);
  }, [profileName, profileStorageKey]);

  const activeProfile = profiles?.[profileName] || profiles?.[defaultProfile] || {};
  const profileColumns = useMemoGrid(
    () => utils.applyColumnProfile(columns, activeProfile),
    [utils, columns, activeProfile]
  );
  const visibleColumns = useMemoGrid(() => {
    if (!manualVisible) return profileColumns;
    const visibleSet = new Set(manualVisible);
    return columns.filter(column => visibleSet.has(column.key));
  }, [columns, manualVisible, profileColumns]);

  const normalizedFilters = useMemoGrid(() => (
    Object.fromEntries(
      Object.entries(columnFilters).map(([key, value]) => [
        key,
        normalizeGridFilter(columns.find(column => column.key === key), value),
      ])
    )
  ), [columns, columnFilters]);

  const filteredRows = useMemoGrid(() => {
    const bySearch = (rows || []).filter(row => utils.rowMatchesFlexibleQuery(row, query));
    const byColumns = utils.applyColumnFilters(bySearch, normalizedFilters);
    if (!sortState.key) return byColumns;
    const column = columns.find(col => col.key === sortState.key);
    return [...byColumns].sort((a, b) => compareGridValues(
      a?.[sortState.key],
      b?.[sortState.key],
      sortState.direction,
      column?.numeric
    ));
  }, [utils, rows, query, normalizedFilters, sortState, columns]);

  function updateColumnFilter(key, value) {
    setColumnFilters(prev => {
      const next = { ...prev };
      if (!value) delete next[key];
      else next[key] = value;
      return next;
    });
  }

  function toggleColumn(key) {
    setManualVisible(prev => {
      const current = new Set(prev || profileColumns.map(column => column.key));
      current.has(key) ? current.delete(key) : current.add(key);
      return [...current];
    });
  }

  function toggleSort(key) {
    setSortState(prev => {
      if (prev.key !== key) return { key, direction: "asc" };
      if (prev.direction === "asc") return { key, direction: "desc" };
      return { key: null, direction: "asc" };
    });
  }

  function resetProfile() {
    setProfileName(defaultProfile);
    setManualVisible(null);
    setColumnFilters({});
    setQuery("");
    setSortState({ key: null, direction: "asc" });
    try {
      window.localStorage?.removeItem(profileStorageKey);
    } catch {
      /* localStorage can be unavailable in restricted contexts. */
    }
  }

  const filtersSummary = summarizeGridFilters(query, columnFilters, sortState, columns);

  return (
    <div className="data-grid">
      <div className="data-grid-toolbar">
        <div className="search-wrap data-grid-search">
          <span className="data-grid-search-icon">⌕</span>
          <input className="input search-input"
            value={query}
            onChange={e => setQuery(e.target.value)}
            placeholder={'Busca flexivel: texto ncm:22030000 score>=80 -ignorar "frase exata"'} />
        </div>
        <select className="select data-grid-profile"
          value={profileName}
          onChange={e => setProfileName(e.target.value)}>
          {Object.keys(profiles || { [defaultProfile]: {} }).map(name => (
            <option key={name}>{name}</option>
          ))}
        </select>
        <button className="btn" onClick={() => setColumnPanelOpen(open => !open)}>
          Colunas ({visibleColumns.length}/{columns.length})
        </button>
        <button className="btn" onClick={resetProfile}>
          Resetar perfil
        </button>
        <button className="btn" onClick={() => {
          setQuery("");
          setColumnFilters({});
          setManualVisible(null);
          setSortState({ key: null, direction: "asc" });
        }}>
          Limpar filtros
        </button>
        <span className="mono dim-text data-grid-count">
          {filteredRows.length} de {(rows || []).length}
        </span>
      </div>
      <div className="data-grid-filter-summary mono">
        Filtros ativos: {filtersSummary || "nenhum"}
      </div>

      {columnPanelOpen && (
        <div className="column-picker">
          {columns.filter(column => column.key !== "__select").map(column => (
            <label key={column.key} className="column-picker-item">
              <input type="checkbox" className="checkbox"
                checked={visibleColumns.some(visible => visible.key === column.key)}
                onChange={() => toggleColumn(column.key)} />
              <span>{column.label}</span>
            </label>
          ))}
        </div>
      )}

      <div className="data-table-shell" style={{ maxHeight }}>
        <div className="data-table-scroll">
          <table className="data-table">
            <thead>
              <tr>
                {visibleColumns.map(column => (
                  <th key={column.key} style={{ width: column.width, textAlign: column.align || "left" }}>
                    <button type="button"
                      className="data-grid-sort-button"
                      onClick={() => toggleSort(column.key)}
                      title={`Ordenar por ${column.label}`}>
                      <span>{column.label}</span>
                      <span className="data-grid-sort-mark">
                        {sortState.key === column.key ? (sortState.direction === "asc" ? "ASC" : "DESC") : ""}
                      </span>
                    </button>
                  </th>
                ))}
              </tr>
              <tr className="column-filter-row">
                {visibleColumns.map(column => (
                  <th key={`${column.key}-filter`}>
                    {column.filter === false ? null : (
                      <input className="column-filter-input"
                        value={columnFilters[column.key] || ""}
                        placeholder={column.numeric ? ">= 0" : `Filtrar ${column.label}`}
                        onChange={e => updateColumnFilter(column.key, e.target.value)}
                        onClick={e => e.stopPropagation()} />
                    )}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {filteredRows.map((row, index) => {
                const key = typeof rowKey === "function" ? rowKey(row, index) : row[rowKey];
                const isSelected = selectedIds?.has?.(key);
                const cls = [
                  isSelected ? "is-selected" : "",
                  typeof rowClassName === "function" ? rowClassName(row, index) : "",
                ].filter(Boolean).join(" ");
                return (
                  <tr key={key ?? index} className={cls} onClick={() => onRowClick?.(row, index)}>
                    {visibleColumns.map(column => (
                      <td key={column.key} className={column.className || ""} style={{ textAlign: column.align || "left" }}>
                        {column.render ? column.render(row, index) : formatGridCell(row[column.key])}
                      </td>
                    ))}
                  </tr>
                );
              })}
              {filteredRows.length === 0 && (
                <tr>
                  <td colSpan={Math.max(visibleColumns.length, 1)} className="data-grid-empty">
                    {emptyText}
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
}

function normalizeGridFilter(column, value) {
  const text = String(value || "").trim();
  if (!text) return { value: "" };
  if (column?.numeric) {
    const match = text.match(/^(>=|<=|>|<|=)?\s*(-?\d+(?:[.,]\d+)?)$/);
    if (match) {
      return { operator: match[1] || "=", value: match[2] };
    }
  }
  return { operator: "contains", value: text };
}

function countGridFilters(query, filters) {
  return (String(query || "").trim() ? 1 : 0) +
    Object.values(filters || {}).filter(value => String(value || "").trim()).length;
}

function formatGridCell(value) {
  if (value == null || value === "") return "-";
  if (typeof value === "number") return value.toLocaleString("pt-BR");
  return String(value);
}

function compareGridValues(a, b, direction, numeric) {
  const factor = direction === "desc" ? -1 : 1;
  if (numeric) {
    const av = Number(String(a ?? "").replace(",", "."));
    const bv = Number(String(b ?? "").replace(",", "."));
    if (Number.isFinite(av) && Number.isFinite(bv)) return (av - bv) * factor;
  }
  return String(a ?? "").localeCompare(String(b ?? ""), "pt-BR", {
    numeric: true,
    sensitivity: "base",
  }) * factor;
}

function summarizeGridFilters(query, filters, sortState, columns) {
  const parts = [];
  if (String(query || "").trim()) parts.push(`busca="${String(query).trim()}"`);
  Object.entries(filters || {}).forEach(([key, value]) => {
    if (!String(value || "").trim()) return;
    const label = columns.find(column => column.key === key)?.label || key;
    parts.push(`${label}=${String(value).trim()}`);
  });
  if (sortState?.key) {
    const label = columns.find(column => column.key === sortState.key)?.label || sortState.key;
    parts.push(`ordem=${label} ${sortState.direction}`);
  }
  return parts.slice(0, 4).join(" | ");
}

window.DataGrid = DataGrid;
