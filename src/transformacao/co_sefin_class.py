"""Proxy module for backward compatibility.
This module delegates to transformacao.movimentacao_estoque_pkg.co_sefin_class
and replaces itself in sys.modules so tests that import this proxy get the
real implementation object (allowing monkeypatch to target the correct module).
"""
import importlib
import sys

_pkg = importlib.import_module("transformacao.movimentacao_estoque_pkg.co_sefin_class")
# Replace this module object in sys.modules with the real package module
sys.modules[__name__] = _pkg

