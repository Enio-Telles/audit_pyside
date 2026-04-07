const intlInteger = new Intl.NumberFormat("pt-BR");
const intlDecimal = new Intl.NumberFormat("pt-BR", {
  minimumFractionDigits: 2,
  maximumFractionDigits: 2,
});

export function formatCell(value: unknown): string {
  if (value === null || value === undefined) return "";
  if (Array.isArray(value)) return value.join(" | ");
  if (typeof value === "number") {
    if (Number.isInteger(value)) return intlInteger.format(value);
    return intlDecimal.format(value);
  }
  return String(value);
}
