"""Proxy module for backward compatibility. Real module: transformacao.movimentacao_estoque_pkg.movimentacao_estoque"""
from transformacao.movimentacao_estoque_pkg.calculo_saldos import (  # noqa: F401
    calcular_saldo_estoque_anual as _calcular_saldo_estoque_anual,
)
from transformacao.movimentacao_estoque_pkg.movimentacao_estoque import *  # noqa: F401,F403
