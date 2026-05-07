"""
Pacote fisconforme para integracao do pipeline "Fisconforme nao Atendido".

O painel GUI e carregado sob demanda para permitir importar utilitarios sem
exigir bibliotecas Qt no ambiente de testes nao-GUI.
"""

__all__ = ["FisconformeNaoAtendidoPanel"]


def __getattr__(name):
    if name == "FisconformeNaoAtendidoPanel":
        from .panel import FisconformeNaoAtendidoPanel

        return FisconformeNaoAtendidoPanel
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
