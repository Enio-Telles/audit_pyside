import { useEffect, useMemo, useState } from "react";

const LARGURA_PADRAO_COLUNA = 120;

interface PreferenciasColunasSalvas {
  ordemColunas: string[];
  largurasColunas: Record<string, number>;
}

interface RetornoPreferenciasColunas {
  ordemColunas: string[];
  largurasColunas: Record<string, number>;
  definirOrdemColunas: (ordemColunas: string[]) => void;
  definirLarguraColuna: (coluna: string, largura: number) => void;
  redefinirPreferenciasColunas: () => void;
}

function carregarPreferenciasColunas(
  chavePersistencia: string,
): PreferenciasColunasSalvas {
  if (typeof window === "undefined") {
    return { ordemColunas: [], largurasColunas: {} };
  }

  try {
    const bruto = window.localStorage.getItem(chavePersistencia);
    if (!bruto) return { ordemColunas: [], largurasColunas: {} };

    const preferencias = JSON.parse(bruto) as Partial<PreferenciasColunasSalvas>;
    return {
      ordemColunas: Array.isArray(preferencias.ordemColunas)
        ? preferencias.ordemColunas.filter(
            (coluna): coluna is string => typeof coluna === "string",
          )
        : [],
      largurasColunas:
        preferencias.largurasColunas &&
        typeof preferencias.largurasColunas === "object"
          ? Object.fromEntries(
              Object.entries(preferencias.largurasColunas).filter(
                ([, largura]) =>
                  typeof largura === "number" && Number.isFinite(largura),
              ),
            )
          : {},
    };
  } catch {
    return { ordemColunas: [], largurasColunas: {} };
  }
}

function normalizarOrdemColunas(
  colunasDisponiveis: string[],
  ordemSalva: string[],
): string[] {
  if (!ordemSalva.length) return [...colunasDisponiveis];

  return [
    ...ordemSalva.filter((coluna) => colunasDisponiveis.includes(coluna)),
    ...colunasDisponiveis.filter((coluna) => !ordemSalva.includes(coluna)),
  ];
}

function normalizarLargurasColunas(
  colunasDisponiveis: string[],
  largurasSalvas: Record<string, number>,
): Record<string, number> {
  return Object.fromEntries(
    colunasDisponiveis.map((coluna) => [
      coluna,
      Math.max(80, largurasSalvas[coluna] ?? LARGURA_PADRAO_COLUNA),
    ]),
  );
}

export function usePreferenciasColunas(
  chavePersistencia: string,
  colunasDisponiveis: string[],
  largurasIniciais: Record<string, number> = {},
): RetornoPreferenciasColunas {
  const [preferenciasSalvas, setPreferenciasSalvas] =
    useState<PreferenciasColunasSalvas>(() =>
      carregarPreferenciasColunas(chavePersistencia),
    );

  const ordemColunas = useMemo(
    () =>
      normalizarOrdemColunas(
        colunasDisponiveis,
        preferenciasSalvas.ordemColunas,
      ),
    [colunasDisponiveis, preferenciasSalvas.ordemColunas],
  );

  const largurasColunas = useMemo(
    () =>
      normalizarLargurasColunas(
        colunasDisponiveis,
        { ...largurasIniciais, ...preferenciasSalvas.largurasColunas },
      ),
    [colunasDisponiveis, largurasIniciais, preferenciasSalvas.largurasColunas],
  );

  useEffect(() => {
    if (typeof window === "undefined") return;

    const preferenciasAtualizadas: PreferenciasColunasSalvas = {
      ordemColunas,
      largurasColunas,
    };

    window.localStorage.setItem(
      chavePersistencia,
      JSON.stringify(preferenciasAtualizadas),
    );
  }, [chavePersistencia, largurasColunas, ordemColunas]);

  function definirOrdemColunas(ordemAtualizada: string[]) {
    setPreferenciasSalvas((anterior) => ({
      ...anterior,
      ordemColunas: ordemAtualizada,
    }));
  }

  function definirLarguraColuna(coluna: string, largura: number) {
    setPreferenciasSalvas((anterior) => ({
      ...anterior,
      largurasColunas: {
        ...anterior.largurasColunas,
        [coluna]: Math.max(80, Math.min(600, largura)),
      },
    }));
  }

  function redefinirPreferenciasColunas() {
    setPreferenciasSalvas({
      ordemColunas: [...colunasDisponiveis],
      largurasColunas: normalizarLargurasColunas(
        colunasDisponiveis,
        largurasIniciais,
      ),
    });
  }

  return {
    ordemColunas,
    largurasColunas,
    definirOrdemColunas,
    definirLarguraColuna,
    redefinirPreferenciasColunas,
  };
}
