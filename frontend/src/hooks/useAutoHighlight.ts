import { useMemo } from "react";
import type { HighlightRule } from "../api/types";

export type AutoHighlightMode = "nulls" | "all";

const NULL_COLOR = "rgba(220,38,38,0.18)";
const OUTLIER_COLOR = "rgba(234,179,8,0.18)";

/**
 * Generates automatic highlight rules based on the data currently in view.
 *
 * - `nulls` mode: highlights cells/rows that contain null values in each column.
 * - `all` mode: also highlights simple numeric outliers (values > 3σ from mean).
 *
 * Rules are cheap to compute — they only add/remove CSS color effects in the table.
 */
export function useAutoHighlight(
  columns: string[],
  rows: Record<string, unknown>[],
  mode: AutoHighlightMode,
): HighlightRule[] {
  return useMemo(() => {
    if (columns.length === 0) return [];

    const rules: HighlightRule[] = [];

    // Null-detection rules for every column
    for (const col of columns) {
      rules.push({
        type: "column",
        column: col,
        operator: "e_nulo",
        value: "",
        color: NULL_COLOR,
        label: `${col} nulo`,
      });
    }

    if (mode === "all" && rows.length > 1) {
      // Outlier detection per numeric column
      for (const col of columns) {
        const nums = rows
          .map((r) => r[col])
          .filter((v): v is number => typeof v === "number" && Number.isFinite(v));

        if (nums.length < 4) continue;

        const mean = nums.reduce((acc, v) => acc + v, 0) / nums.length;
        const variance =
          nums.reduce((acc, v) => acc + (v - mean) ** 2, 0) / nums.length;
        const sigma = Math.sqrt(variance);

        if (sigma === 0) continue;

        const threshold = 3 * sigma;
        const hasOutliers = nums.some((v) => Math.abs(v - mean) > threshold);

        if (hasOutliers) {
          // Use "maior" rule as approximate outlier indicator (values > mean + 3σ)
          rules.push({
            type: "column",
            column: col,
            operator: "maior",
            value: String(mean + threshold),
            color: OUTLIER_COLOR,
            label: `${col} outlier (+3σ)`,
          });
        }
      }
    }

    return rules;
  }, [columns, rows, mode]);
}
