from pathlib import Path
import re


def _ler_main_window() -> str:
    caminho = Path("src/interface_grafica/ui/main_window.py")
    return caminho.read_text(encoding="utf-8")


def _contains_sequence_ignore_ws(conteudo: str, *items: str) -> bool:
    # build a pattern that matches the quoted items in order, allowing
    # arbitrary whitespace and newlines between them
    quoted = [re.escape(f'"{it}"') for it in items]
    pattern = r"\s*,\s*".join(quoted)
    return re.search(pattern, conteudo, flags=re.DOTALL) is not None


def _mapping_contains(conteudo: str, key: str, *items: str) -> bool:
    # check that a mapping key (e.g. "exportar": [ ... ]) contains the items
    key_pat = re.escape(f'"{key}"') + r"\s*:\s*\["
    body_match = re.search(key_pat + r"(.*?)\]", conteudo, flags=re.DOTALL)
    if not body_match:
        return False
    body = body_match.group(1)
    return _contains_sequence_ignore_ws(body, *items)


def test_preset_mov_estoque_usa_fonte_em_vez_de_origem():
    conteudo = _ler_main_window()

    assert _contains_sequence_ignore_ws(conteudo, "ordem_operacoes", "Tipo_operacao", "fonte")
    assert re.search(r'"Tipo_operacao"\s*,\s*"origem"', conteudo) is None


def test_preset_mov_estoque_expoe_fonte_nos_perfis_de_auditoria():
    conteudo = _ler_main_window()

    assert _mapping_contains(conteudo, "exportar", "ordem_operacoes", "Tipo_operacao", "fonte")
    assert _mapping_contains(conteudo, "auditoria", "ordem_operacoes", "Tipo_operacao", "fonte")
    assert _mapping_contains(conteudo, "auditoria fiscal", "ordem_operacoes", "Tipo_operacao", "fonte")
