from dotenv import load_dotenv

from utilitarios.project_paths import (
    APP_STATE_ROOT,
    CNPJ_ROOT,
    CONSULTAS_ROOT,
    DATA_ROOT,
    ENV_PATH,
    PIPELINE_SCRIPT,
    PROJECT_ROOT,
    SQL_ROOT,
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
