import { renderHook } from "@testing-library/react";
import { describe, it, expect } from "vitest";
import { useAutoHighlight } from "../useAutoHighlight";

describe("useAutoHighlight", () => {
  const rows = [
    { nome: "A", valor: 10, cnpj: null },
    { nome: null, valor: 20, cnpj: "12345678000100" },
    { nome: "C", valor: 15, cnpj: null },
  ];

  it("retorna regras de nulo para cada coluna em modo nulls", () => {
    const { result } = renderHook(() =>
      useAutoHighlight(["nome", "valor", "cnpj"], rows, "nulls"),
    );

    const nullRules = result.current.filter((r) => r.operator === "e_nulo");
    expect(nullRules.map((r) => r.column).sort()).toEqual(["cnpj", "nome", "valor"]);
  });

  it("retorna array vazio quando não há colunas", () => {
    const { result } = renderHook(() =>
      useAutoHighlight([], rows, "nulls"),
    );

    expect(result.current).toHaveLength(0);
  });

  it("inclui regras de outlier no modo all", () => {
    // 10 zeros + one clear outlier: 3σ detection should fire
    const rowsWithOutlier = Array.from({ length: 10 }, () => ({ valor: 0 })).concat([
      { valor: 1000 },
    ]);

    const { result } = renderHook(() =>
      useAutoHighlight(["valor"], rowsWithOutlier, "all"),
    );

    const outlierRules = result.current.filter((r) => r.operator === "maior");
    expect(outlierRules.length).toBeGreaterThan(0);
    expect(outlierRules[0].column).toBe("valor");
  });

  it("não inclui regras de outlier no modo nulls", () => {
    const rowsWithOutlier = [
      { valor: 10 },
      { valor: 1000 },
    ];

    const { result } = renderHook(() =>
      useAutoHighlight(["valor"], rowsWithOutlier, "nulls"),
    );

    const outlierRules = result.current.filter((r) => r.operator === "maior");
    expect(outlierRules).toHaveLength(0);
  });
});
