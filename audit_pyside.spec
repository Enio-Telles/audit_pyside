# -*- mode: python ; coding: utf-8 -*-
# PyInstaller spec for Fiscal Parquet Analyzer (audit_pyside)
#
# Build:
#   uv run pyinstaller audit_pyside.spec --clean --noconfirm
#
# The application reads Oracle credentials from .env, Parquet data from
# dados/, and SQL files from sql/ — these runtime directories are NOT
# bundled and must exist beside the executable in production.
#
# Path resolution note:
#   src/utilitarios/project_paths.py derives PROJECT_ROOT from __file__
#   (parents[2]).  Inside a one-dir bundle, sys._MEIPASS/src/utilitarios/
#   project_paths.py resolves parents[2] == sys._MEIPASS — which is the
#   extracted bundle directory.  For a portable installation, place dados/,
#   sql/, workspace/, and .env in the same directory as the executable, or
#   set DATA_ROOT / SQL_ROOT via environment variables before launch.
#
# UPX note:
#   UPX is intentionally disabled (upx=False). On some systems, UPX
#   compressed binaries cause false-positive antivirus alerts and can
#   corrupt PySide6/Qt shared libraries, leading to startup crashes.

import os
import sys
from pathlib import Path

from PyInstaller.utils.hooks import collect_submodules, collect_data_files

ROOT = Path(SPECPATH)  # noqa: F821  (injected by PyInstaller)

# Optional version resource file for Windows.
# The workflow sets VERSION_FILE to an absolute path before invoking PyInstaller.
# When building locally without the env var the attribute is omitted.
_version_file = os.environ.get("VERSION_FILE") or None
SRC = ROOT / "src"

# sys.path patch so collect_submodules can find src/* packages
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

block_cipher = None

# ---------------------------------------------------------------------------
# Hidden imports: collect all submodules of runtime packages.
# collect_submodules finds Python modules; files with digit-prefixed names
# (01_*.py, 02_*.py, 03_*.py) are loaded via spec_from_file_location and
# must be declared in datas instead (see below).
# ---------------------------------------------------------------------------
hiddenimports = [
    *collect_submodules("transformacao"),
    *collect_submodules("extracao"),
    *collect_submodules("metodologia_mds"),
    *collect_submodules("interface_grafica"),
    *collect_submodules("utilitarios"),
    # PySide6 plugins not always auto-detected
    "PySide6.QtCore",
    "PySide6.QtGui",
    "PySide6.QtWidgets",
    "PySide6.QtNetwork",
    "PySide6.QtPrintSupport",
    # structlog formatters loaded dynamically
    "structlog.stdlib",
    "structlog.dev",
    # polars lazy modules
    "polars._utils.udfs",
    "polars.interchange.dataframe",
    # oracledb loads thin/thick mode modules lazily
    "oracledb.impl.python",
    "oracledb.impl.thin",
    # numba AOT cache
    "numba.core",
    # python-dotenv parser
    "dotenv",
    # runpy is used in config.py to execute migration scripts
    "runpy",
]

# ---------------------------------------------------------------------------
# Datas: files that cannot be discovered as Python modules (digit-prefixed
# scripts loaded via importlib.util.spec_from_file_location at runtime).
# ---------------------------------------------------------------------------
datas = [
    # QSS theme loaded at runtime by main_window.py via Path(__file__)
    (
        str(SRC / "interface_grafica" / "themes" / "noir.qss"),
        "interface_grafica/themes",
    ),
    # Source packages imported via pathex in development must be extracted as
    # directories in _internal/ so the PyInstaller frozen importer can locate
    # them.  Using datas is more reliable than hiddenimports for packages whose
    # sub-modules are discovered only at runtime (e.g. via importlib).
    (str(SRC / "utilitarios"), "utilitarios"),
    (str(SRC / "extracao"), "extracao"),
    (str(SRC / "metodologia_mds"), "metodologia_mds"),
    # tabelas_base stubs loaded via spec_from_file_location
    (str(SRC / "transformacao" / "tabelas_base" / "01_item_unidades.py"), "transformacao/tabelas_base"),
    (str(SRC / "transformacao" / "tabelas_base" / "02_itens.py"), "transformacao/tabelas_base"),
    # rastreabilidade_produtos stub loaded via spec_from_file_location
    (str(SRC / "transformacao" / "rastreabilidade_produtos" / "03_descricao_produtos.py"), "transformacao/rastreabilidade_produtos"),
    # Reference Parquet used by fiscal pipeline (cfop_bi)
    # Only include if the file exists at build time; skip otherwise.
    *(
        [
            (
                str(ROOT / "dados" / "referencias" / "referencias" / "cfop" / "cfop_bi.parquet"),
                "dados/referencias/referencias/cfop",
            )
        ]
        if (ROOT / "dados" / "referencias" / "referencias" / "cfop" / "cfop_bi.parquet").exists()
        else []
    ),
]

a = Analysis(
    [str(ROOT / "app.py")],
    pathex=[str(SRC)],
    binaries=[],
    datas=datas,
    hiddenimports=hiddenimports,
    hookspath=["pyi_hooks"],
    hooksconfig={},
    runtime_hooks=[str(ROOT / "pyi_hooks" / "rthook_utilitarios.py")],
    excludes=[
        # Test / dev dependencies — never needed at runtime
        "pytest",
        "pytest_cov",
        "pytest_mock",
        "ruff",
        "mypy",
        "pre_commit",
        # Notebook ecosystem pulled in transitively
        "IPython",
        "jupyter",
        "notebook",
        # Heavy scientific stack not used by the app
        "matplotlib",
        "scipy",
        "sklearn",
        "tensorflow",
        "torch",
    ],
    win_no_prefer_redirects=False,
    win_private_assemblies=False,
    cipher=block_cipher,
    noarchive=False,
)

pyz = PYZ(a.pure, a.zipped_data, cipher=block_cipher)  # noqa: F821

exe = EXE(  # noqa: F821
    pyz,
    a.scripts,
    [],
    exclude_binaries=True,
    name="FiscalParquetAnalyzer",
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=False,  # UPX disabled: see comment at top of file
    console=False,  # GUI application — suppress the console window on Windows
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
    version=_version_file,  # Windows VERSIONINFO resource (set via VERSION_FILE env var)
)

coll = COLLECT(  # noqa: F821
    exe,
    a.binaries,
    a.zipfiles,
    a.datas,
    strip=False,
    upx=False,  # UPX disabled: see comment at top of file
    upx_exclude=[],
    name="FiscalParquetAnalyzer",
)
