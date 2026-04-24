from __future__ import annotations

from interface_grafica.controllers.shared_state import ViewState


def test_view_state_defaults_and_mutation() -> None:
    state = ViewState()
    assert (state.current_cnpj, state.current_page, state.total_rows) == (None, 1, 0)
    state.current_cnpj = "00000000000191"
    state.current_page = 2
    assert (state.current_cnpj, state.current_page, state.current_file) == ("00000000000191", 2, None)
