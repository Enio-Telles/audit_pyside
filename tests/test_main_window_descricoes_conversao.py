from pathlib import Path


def _ler_main_window() -> str:
    caminho = Path("src/interface_grafica/ui/main_window.py")
    return caminho.read_text(encoding="utf-8")


def test_preset_mov_estoque_usa_fonte_em_vez_de_origem():
    conteudo = _ler_main_window()

    assert '"ordem_operacoes", "Tipo_operacao", "fonte"' in conteudo
    assert '"Tipo_operacao", "origem"' not in conteudo


def test_preset_mov_estoque_expoe_fonte_nos_perfis_de_auditoria():
    conteudo = _ler_main_window()

    assert '"exportar": ["ordem_operacoes", "Tipo_operacao", "fonte"' in conteudo
    assert '"auditoria": ["ordem_operacoes", "Tipo_operacao", "fonte"' in conteudo
    assert '"auditoria fiscal": ["ordem_operacoes", "Tipo_operacao", "fonte"' in conteudo
