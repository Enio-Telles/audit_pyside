"""
PaginacaoTabsMixin — paginacao em memoria para tabs de carga completa.

Cada tab (mov_estoque, aba_mensal, aba_anual) mantem:
  - DataFrame filtrado completo (_*_df_filtrado)
  - Pagina atual (_*_page, base 1)

Quando os filtros mudam, aplicar_filtros_*() chama
_armazenar_pagina(key, df) que guarda o DF e reseta para pagina 1.
Os botoes prev/next chamam _prev_page(key) / _next_page(key)
que incrementam/decrementam e chamam o metodo de renderizacao.
"""
from __future__ import annotations

import polars as pl

from interface_grafica.config import DEFAULT_PAGE_SIZE

_TAB_KEYS = ("mov_estoque", "aba_mensal", "aba_anual", "aba_periodos")


class PaginacaoTabsMixin:
    """Gerencia paginacao em memoria para tabs de carga completa."""

    def _init_paginacao_tabs(self) -> None:
        """Deve ser chamado no __init__ da MainWindow apos criacao dos widgets."""
        self._tab_df_filtrado: dict[str, pl.DataFrame] = {
            k: pl.DataFrame() for k in _TAB_KEYS
        }
        self._tab_page: dict[str, int] = {k: 1 for k in _TAB_KEYS}

    # ------------------------------------------------------------------
    # API chamada pelos aplicar_filtros_*()
    # ------------------------------------------------------------------

    def _armazenar_pagina(self, key: str, df_filtrado: pl.DataFrame) -> None:
        """Guarda o DataFrame filtrado e reseta para pagina 1."""
        self._tab_df_filtrado[key] = df_filtrado
        self._tab_page[key] = 1

    def _fatia_pagina(self, key: str) -> pl.DataFrame:
        """Retorna o slice correspondente a pagina atual."""
        df = self._tab_df_filtrado.get(key, pl.DataFrame())
        offset = (self._tab_page[key] - 1) * DEFAULT_PAGE_SIZE
        return df.slice(offset, DEFAULT_PAGE_SIZE)

    def _total_paginas(self, key: str) -> int:
        total = self._tab_df_filtrado.get(key, pl.DataFrame()).height
        return max(1, (total + DEFAULT_PAGE_SIZE - 1) // DEFAULT_PAGE_SIZE)

    def _texto_lbl_pagina(self, key: str) -> str:
        total = self._tab_df_filtrado.get(key, pl.DataFrame()).height
        total_pag = self._total_paginas(key)
        return f"Pagina {self._tab_page[key]}/{total_pag} | {total:,} linhas filtradas"

    # ------------------------------------------------------------------
    # Navegacao generica
    # ------------------------------------------------------------------

    def _prev_page_tab(self, key: str) -> None:
        if self._tab_page.get(key, 1) > 1:
            self._tab_page[key] -= 1
            self._renderizar_pagina_tab(key)

    def _next_page_tab(self, key: str) -> None:
        if self._tab_page.get(key, 1) < self._total_paginas(key):
            self._tab_page[key] += 1
            self._renderizar_pagina_tab(key)

    def _renderizar_pagina_tab(self, key: str) -> None:
        """Redireciona para o renderizador especifico de cada tab."""
        if key == "mov_estoque":
            self._renderizar_pagina_mov_estoque()
        elif key == "aba_mensal":
            self._renderizar_pagina_aba_mensal()
        elif key == "aba_anual":
            self._renderizar_pagina_aba_anual()
        elif key == "aba_periodos":
            self._renderizar_pagina_aba_periodos()

    # ------------------------------------------------------------------
    # Slots conectados aos botoes de cada tab
    # ------------------------------------------------------------------

    def _prev_page_mov_estoque(self) -> None:
        self._prev_page_tab("mov_estoque")

    def _next_page_mov_estoque(self) -> None:
        self._next_page_tab("mov_estoque")

    def _prev_page_aba_mensal(self) -> None:
        self._prev_page_tab("aba_mensal")

    def _next_page_aba_mensal(self) -> None:
        self._next_page_tab("aba_mensal")

    def _prev_page_aba_anual(self) -> None:
        self._prev_page_tab("aba_anual")

    def _next_page_aba_anual(self) -> None:
        self._next_page_tab("aba_anual")

    def _prev_page_aba_periodos(self) -> None:
        self._prev_page_tab("aba_periodos")

    def _next_page_aba_periodos(self) -> None:
        self._next_page_tab("aba_periodos")
