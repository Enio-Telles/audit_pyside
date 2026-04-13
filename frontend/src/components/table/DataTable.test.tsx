import { fireEvent, render, screen, within } from "@testing-library/react";
import { vi } from "vitest";
import { DataTable } from "./DataTable";

describe("DataTable", () => {
  it("respeita a ordem e a largura configuradas para as colunas", () => {
    const { container } = render(
      <DataTable
        columns={["id_agrupado", "descr_padrao", "ncm_padrao"]}
        orderedColumns={["descr_padrao", "id_agrupado", "ncm_padrao"]}
        columnWidths={{
          descr_padrao: 260,
          id_agrupado: 180,
          ncm_padrao: 140,
        }}
        rows={[
          {
            id_agrupado: "id_1",
            descr_padrao: "Produto A",
            ncm_padrao: "12345678",
          },
        ]}
      />,
    );

    const tabela = container.querySelector("table");
    expect(tabela).not.toBeNull();

    const cabecalhos = screen.getAllByRole("columnheader");
    expect(cabecalhos.map((cabecalho) => cabecalho.textContent)).toEqual([
      "#",
      "descr_padrao<>",
      "id_agrupado<>",
      "ncm_padrao<>",
    ]);

    const colunas = container.querySelectorAll("col");
    expect(colunas).toHaveLength(4);
    expect(colunas[1]).toHaveStyle({ width: "260px" });
    expect(colunas[2]).toHaveStyle({ width: "180px" });
    expect(colunas[3]).toHaveStyle({ width: "140px" });

    const linha = screen.getAllByRole("row")[1];
    const celulas = within(linha).getAllByRole("cell");
    expect(celulas.map((celula) => celula.textContent)).toEqual([
      "1",
      "Produto A",
      "id_1",
      "12345678",
    ]);
  });

  it("permite reordenar colunas arrastando o cabecalho", () => {
    const aoAlterarOrdem = vi.fn();

    render(
      <DataTable
        columns={["id_agrupado", "descr_padrao", "ncm_padrao"]}
        orderedColumns={["id_agrupado", "descr_padrao", "ncm_padrao"]}
        onOrderedColumnsChange={aoAlterarOrdem}
        rows={[
          {
            id_agrupado: "id_1",
            descr_padrao: "Produto A",
            ncm_padrao: "12345678",
          },
        ]}
      />,
    );

    const cabecalhoOrigem = screen.getByTitle("id_agrupado - clique para ordenar");
    const cabecalhoDestino = screen.getByTitle("ncm_padrao - clique para ordenar");

    fireEvent.dragStart(cabecalhoOrigem);
    fireEvent.dragOver(cabecalhoDestino);
    fireEvent.drop(cabecalhoDestino);

    expect(aoAlterarOrdem).toHaveBeenCalledWith([
      "descr_padrao",
      "ncm_padrao",
      "id_agrupado",
    ]);
  });

  it("permite redimensionar colunas direto no cabecalho", () => {
    const aoAlterarLargura = vi.fn();

    render(
      <DataTable
        columns={["id_agrupado", "descr_padrao"]}
        columnWidths={{ id_agrupado: 180, descr_padrao: 260 }}
        onColumnWidthChange={aoAlterarLargura}
        rows={[
          {
            id_agrupado: "id_1",
            descr_padrao: "Produto A",
          },
        ]}
      />,
    );

    const alca = screen.getByTitle("Redimensionar descr_padrao");
    fireEvent.mouseDown(alca, { clientX: 260 });
    fireEvent.mouseMove(window, { clientX: 320 });
    fireEvent.mouseUp(window);

    expect(aoAlterarLargura).toHaveBeenCalledWith("descr_padrao", 320);
  });
});
