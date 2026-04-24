# -*- mode: python ; coding: utf-8 -*-
# PyInstaller spec for Fiscal Parquet Analyzer (audit_pyside)
#
# Build:
#   pyinstaller audit_pyside.spec
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

from pathlib import Path

ROOT = Path(SPECPATH)  # noqa: F821  (injected by PyInstaller)
SRC = ROOT / "src"

block_cipher = None

a = Analysis(
    [str(ROOT / "app.py")],
    pathex=[str(SRC)],
    binaries=[],
    datas=[
        # QSS theme loaded at runtime by main_window.py via Path(__file__)
        (
            str(SRC / "interface_grafica" / "themes" / "noir.qss"),
            "interface_grafica/themes",
        ),
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
    ],
    hiddenimports=[
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
    ],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
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
    upx=True,
    console=False,  # GUI application — suppress the console window on Windows
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
)

coll = COLLECT(  # noqa: F821
    exe,
    a.binaries,
    a.zipfiles,
    a.datas,
    strip=False,
    upx=True,
    upx_exclude=[],
    name="FiscalParquetAnalyzer",
)
