/* global window */
(function tableUtilsFactory(globalScope) {
  function normalizeText(value) {
    return String(value ?? "")
      .normalize("NFD")
      .replace(/[\u0300-\u036f]/g, "")
      .replace(/[^\w\s|:>=<!"'-]+/g, " ")
      .replace(/\s+/g, " ")
      .trim()
      .toLowerCase();
  }

  function rowText(row) {
    return Object.values(row || {}).map(normalizeText).join(" ");
  }

  function splitQuery(query) {
    const parts = [];
    const re = /"([^"]+)"|(\S+)/g;
    let match;
    while ((match = re.exec(String(query || ""))) !== null) {
      parts.push({
        raw: match[1] ?? match[2],
        phrase: match[1] != null,
      });
    }
    return parts;
  }

  function parseFlexibleQuery(query) {
    return splitQuery(query).map((part) => {
      let raw = part.raw;
      let negated = false;
      if (raw.startsWith("-") && raw.length > 1) {
        negated = true;
        raw = raw.slice(1);
      }

      const numeric = raw.match(/^([A-Za-z_][\w.-]*)(>=|<=|>|<|=)(-?\d+(?:[.,]\d+)?)$/);
      if (numeric) {
        return {
          type: "numeric",
          field: numeric[1],
          operator: numeric[2],
          value: Number(numeric[3].replace(",", ".")),
          negated,
        };
      }

      const field = raw.match(/^([A-Za-z_][\w.-]*):(.+)$/);
      if (field) {
        return {
          type: "field",
          field: field[1],
          value: field[2],
          negated,
        };
      }

      return {
        type: "text",
        value: raw,
        phrase: part.phrase,
        negated,
      };
    });
  }

  function compareNumeric(actual, operator, expected) {
    const value = Number(String(actual ?? "").replace(",", "."));
    if (!Number.isFinite(value)) return false;
    if (operator === ">=") return value >= expected;
    if (operator === "<=") return value <= expected;
    if (operator === ">") return value > expected;
    if (operator === "<") return value < expected;
    return value === expected;
  }

  function includesAlternative(haystack, needle) {
    const alternatives = String(needle || "")
      .split("|")
      .map(normalizeText)
      .filter(Boolean);
    if (alternatives.length === 0) return true;
    return alternatives.some((term) => haystack.includes(term));
  }

  function tokenMatches(row, token) {
    if (token.type === "numeric") {
      return compareNumeric(row?.[token.field], token.operator, token.value);
    }

    if (token.type === "field") {
      return includesAlternative(normalizeText(row?.[token.field]), token.value);
    }

    return includesAlternative(rowText(row), token.value);
  }

  function rowMatchesFlexibleQuery(row, query) {
    const tokens = parseFlexibleQuery(query);
    if (tokens.length === 0) return true;
    return tokens.every((token) => {
      const matched = tokenMatches(row, token);
      return token.negated ? !matched : matched;
    });
  }

  function compareValue(actual, operator, expected) {
    if (expected == null || String(expected).trim() === "") return true;
    if ([">=", "<=", ">", "<", "="].includes(operator)) {
      return compareNumeric(actual, operator, Number(String(expected).replace(",", ".")));
    }
    const haystack = normalizeText(actual);
    const needle = normalizeText(expected);
    if (operator === "equals") return haystack === needle;
    if (operator === "starts") return haystack.startsWith(needle);
    if (operator === "ends") return haystack.endsWith(needle);
    if (operator === "regex") {
      try {
        return new RegExp(String(expected), "i").test(String(actual ?? ""));
      } catch {
        return false;
      }
    }
    return haystack.includes(needle);
  }

  function applyColumnFilters(rows, filters) {
    const entries = Object.entries(filters || {}).filter(([, cfg]) => {
      if (cfg == null) return false;
      if (typeof cfg === "string") return cfg.trim() !== "";
      return String(cfg.value ?? "").trim() !== "";
    });
    if (entries.length === 0) return rows || [];

    return (rows || []).filter((row) =>
      entries.every(([key, cfg]) => {
        const filter = typeof cfg === "string" ? { value: cfg } : cfg;
        return compareValue(row?.[key], filter.operator || "contains", filter.value);
      })
    );
  }

  function applyColumnProfile(columns, profile) {
    const source = Array.isArray(columns) ? columns : [];
    const visible = Array.isArray(profile?.visible) ? new Set(profile.visible) : null;
    const order = Array.isArray(profile?.order) ? profile.order : [];
    const byKey = new Map(source.map((column) => [column.key, column]));
    const result = [];

    order.forEach((key) => {
      const column = byKey.get(key);
      if (column && (!visible || visible.has(key))) {
        result.push(column);
      }
    });

    source.forEach((column) => {
      if (result.includes(column)) return;
      if (visible && !visible.has(column.key)) return;
      result.push(column);
    });

    return result;
  }

  const api = {
    normalizeText,
    parseFlexibleQuery,
    rowMatchesFlexibleQuery,
    applyColumnFilters,
    applyColumnProfile,
  };

  if (typeof module !== "undefined" && module.exports) {
    module.exports = api;
  }
  if (globalScope) {
    globalScope.AUDIT_TABLE_UTILS = api;
  }
})(typeof window !== "undefined" ? window : globalThis);
