from dotenv import load_dotenv

from utilitarios.project_paths import (
    APP_STATE_ROOT,
    CNPJ_ROOT,
    CONSULTAS_ROOT,
    DATA_ROOT,
    ENV_PATH,
    PROJECT_ROOT,
    SQL_ROOT,
    PIPELINE_SCRIPT,  # noqa: F401 (re-exported for pipeline compatibility)
)

APP_NAME = "Fiscal Parquet Analyzer"

# Load environment variables from .env file in the project root
if ENV_PATH.exists():
    load_dotenv(ENV_PATH, override=False, encoding="latin-1")
REGISTRY_FILE = APP_STATE_ROOT / "cnpjs.json"
AGGREGATION_LOG_FILE = APP_STATE_ROOT / "operacoes_agregacao.jsonl"
SELECTIONS_FILE = APP_STATE_ROOT / "selections.json"
SQL_DIR = SQL_ROOT
DADOS_ROOT = DATA_ROOT
DEFAULT_PAGE_SIZE = 200
MAX_DOCX_ROWS = 500

for path in [CONSULTAS_ROOT, APP_STATE_ROOT, SQL_DIR, CNPJ_ROOT]:
    path.mkdir(parents=True, exist_ok=True)


# Run app_state migration script (idempotent, non-fatal)
try:
    import logging
    import runpy

    logger = logging.getLogger(__name__)
    script_path = PROJECT_ROOT / "scripts" / "migrate_app_state_active_load_workers.py"
    if script_path.exists():
        try:
            runpy.run_path(str(script_path), run_name="__main__")
            logger.info("app_state migration executed: %s", script_path)
        except Exception as _e:
            logger.exception("app_state migration failed: %s", _e)
    else:
        logger.debug("No app_state migration script found at %s", script_path)
except Exception:
    # Never fail initialization because migration step errors
    pass
