import { fireEvent, render, screen } from "@testing-library/react";
import { vi } from "vitest";
import { ColumnToggle } from "./ColumnToggle";

describe("ColumnToggle", () => {
  const columns = ["nome", "cnpj", "valor", "data"];

  it("exibe contagem de colunas visíveis no botão", () => {
    render(
      <ColumnToggle
        allColumns={columns}
        hiddenColumns={new Set(["data"])}
        onChange={vi.fn()}
        onReset={vi.fn()}
      />,
    );

    expect(screen.getByText(/3\/4/)).toBeDefined();
  });

  it("abre dropdown ao clicar no botão Colunas", () => {
    render(
      <ColumnToggle
        allColumns={columns}
        hiddenColumns={new Set()}
        onChange={vi.fn()}
        onReset={vi.fn()}
      />,
    );

    const btn = screen.getByRole("button", { name: /Colunas/i });
    fireEvent.click(btn);

    // All column names should now be visible in the dropdown
    for (const col of columns) {
      expect(screen.getAllByText(col).length).toBeGreaterThan(0);
    }
  });

  it("chama onChange ao clicar no checkbox de uma coluna", () => {
    const onChange = vi.fn();

    render(
      <ColumnToggle
        allColumns={columns}
        hiddenColumns={new Set()}
        onChange={onChange}
        onReset={vi.fn()}
      />,
    );

    fireEvent.click(screen.getByRole("button", { name: /Colunas/i }));

    // Find checkbox by its position as a sibling of the column name span
    const checkboxes = screen.getAllByRole("checkbox");
    const valorCheckbox = checkboxes[columns.indexOf("valor")];
    fireEvent.click(valorCheckbox);

    expect(onChange).toHaveBeenCalledWith("valor", false);
  });

  it("chama onReset ao clicar em Padrão", () => {
    const onReset = vi.fn();

    render(
      <ColumnToggle
        allColumns={columns}
        hiddenColumns={new Set(["cnpj"])}
        onChange={vi.fn()}
        onReset={onReset}
      />,
    );

    fireEvent.click(screen.getByRole("button", { name: /Colunas/i }));
    fireEvent.click(screen.getByRole("button", { name: /Padrão/i }));

    expect(onReset).toHaveBeenCalledOnce();
  });
});
