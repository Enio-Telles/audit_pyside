from __future__ import annotations

import shutil
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]
SRC_ROOT = PROJECT_ROOT / "src"
SQL_ROOT = PROJECT_ROOT / "sql"
SQL_ARCHIVE_ROOT = SQL_ROOT / "archive"
DATA_ROOT = PROJECT_ROOT / "dados"
CNPJ_ROOT = DATA_ROOT / "CNPJ"
WORKSPACE_ROOT = PROJECT_ROOT / "workspace"
CONSULTAS_ROOT = WORKSPACE_ROOT / "consultas"
APP_STATE_ROOT = WORKSPACE_ROOT / "app_state"
LEGACY_WORKSPACE_ROOT = SRC_ROOT / "workspace"
LEGACY_APP_STATE_ROOT = LEGACY_WORKSPACE_ROOT / "app_state"
ENV_PATH = PROJECT_ROOT / ".env"
PIPELINE_SCRIPT = PROJECT_ROOT / "pipeline_oracle_parquet.py"
CFOP_BI_PATH = DATA_ROOT / "referencias" / "referencias" / "cfop" / "cfop_bi.parquet"
TRACEBACK_PATH = WORKSPACE_ROOT / "traceback.txt"


def ensure_runtime_directories() -> None:
    for path in (
        SQL_ROOT,
        SQL_ARCHIVE_ROOT,
        DATA_ROOT,
        CNPJ_ROOT,
        WORKSPACE_ROOT,
        CONSULTAS_ROOT,
        APP_STATE_ROOT,
    ):
        path.mkdir(parents=True, exist_ok=True)


def migrate_legacy_app_state() -> None:
    if not LEGACY_APP_STATE_ROOT.exists():
        return

    APP_STATE_ROOT.mkdir(parents=True, exist_ok=True)

    for legacy_path in LEGACY_APP_STATE_ROOT.iterdir():
        target_path = APP_STATE_ROOT / legacy_path.name
        if target_path.exists():
            continue
        if legacy_path.is_dir():
            shutil.copytree(legacy_path, target_path)
        else:
            shutil.copy2(legacy_path, target_path)


ensure_runtime_directories()
migrate_legacy_app_state()
