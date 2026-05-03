"""
audit-pyside-perf-mcp: Servidor MCP para auditoria e performance.

Ferramentas seguras para: checks Python, performance, Oracle read-only, PySide, Polars.
"""

import os
import sys
from pathlib import Path

# Adiciona o módulo ao path
MODULE_PATH = Path(__file__).parent / "src"
if str(MODULE_PATH) not in sys.path:
    sys.path.insert(0, str(MODULE_PATH))

from mcp.server.fastmcp import FastMCP

# Importa os módulos de tools
from audit_pyside_perf_mcp.config import Config, validate_config
from audit_pyside_perf_mcp.tools_project import register_project_tools
from audit_pyside_perf_mcp.tools_oracle import register_oracle_tools
from audit_pyside_perf_mcp.tools_pyside import register_pyside_tools
from audit_pyside_perf_mcp.tools_perf import register_perf_tools
from audit_pyside_perf_mcp.tools_polars import register_polars_tools
from audit_pyside_perf_mcp.tools_gates import register_gates_tools

# Cria o servidor MCP
mcp = FastMCP("audit-pyside-perf-mcp")

# Registra todos os módulos de tools
register_project_tools(mcp)
register_oracle_tools(mcp)
register_pyside_tools(mcp)
register_perf_tools(mcp)
register_polars_tools(mcp)
register_gates_tools(mcp)


def main():
    """Inicia o servidor MCP."""
    mcp.run(transport="stdio")


if __name__ == "__main__":
    main()

