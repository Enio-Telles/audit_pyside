"""
Router Oracle — configuração e teste de conexão com o banco de dados Oracle.

Lê as variáveis do arquivo .env da raiz do projeto (C:\\Sistema_pysisde\\.env).
Expõe:
  GET  /api/oracle/config       — retorna configurações das duas conexões (sem senha)
  POST /api/oracle/testar/{slot} — testa a conexão (slot: 1 ou 2) e retorna status
  POST /api/oracle/salvar       — salva configurações no .env
"""
from __future__ import annotations

import logging
import re
from pathlib import Path
from time import perf_counter

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

logger = logging.getLogger(__name__)

router = APIRouter()

# Raiz do projeto = dois níveis acima de backend/routers/oracle.py
_ENV_PATH = Path(__file__).resolve().parents[2] / ".env"


def _read_env() -> dict[str, str]:
    from dotenv import dotenv_values
    if _ENV_PATH.exists():
        return dict(dotenv_values(_ENV_PATH, encoding="utf-8"))
    return {}


def _write_key(conteudo: str, chave: str, valor: str) -> str:
    if re.search(rf"^{chave}=", conteudo, flags=re.MULTILINE):
        return re.sub(rf"^{chave}=.*$", f"{chave}={valor}", conteudo, flags=re.MULTILINE)
    return conteudo.rstrip() + f"\n{chave}={valor}\n"


# ---------------------------------------------------------------------------
# Schemas
# ---------------------------------------------------------------------------

class OracleConexaoConfig(BaseModel):
    host: str
    port: str
    service: str
    user: str
    password: str  # returned so UI can pre-fill (local app only)
    configured: bool


class OracleConfigResponse(BaseModel):
    conexao_1: OracleConexaoConfig
    conexao_2: OracleConexaoConfig


class TestarConexaoRequest(BaseModel):
    host: str
    port: str = "1521"
    service: str
    user: str
    password: str


class TestarConexaoResponse(BaseModel):
    ok: bool
    message: str
    tempo_ms: int


class SalvarConfigRequest(BaseModel):
    oracle_host: str = ""
    oracle_port: str = "1521"
    oracle_service: str = ""
    db_user: str = ""
    db_password: str = ""
    oracle_host_1: str = ""
    oracle_port_1: str = "1521"
    oracle_service_1: str = ""
    db_user_1: str = ""
    db_password_1: str = ""
    log_level: str = ""
    cache_enabled: str = ""
    cache_ttl: str = ""
    dashboard_theme: str = ""


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@router.get("/config", response_model=OracleConfigResponse)
def obter_config():
    """Retorna as configurações das duas conexões Oracle, omitindo as senhas para segurança."""
    env = _read_env()
    return {
        "conexao_1": {
            "host": env.get("ORACLE_HOST", ""),
            "port": env.get("ORACLE_PORT", "1521"),
            "service": env.get("ORACLE_SERVICE", ""),
            "user": env.get("DB_USER", ""),
            "password": "********" if env.get("DB_PASSWORD") else "",
            "configured": bool(env.get("DB_USER") and env.get("DB_PASSWORD")),
        },
        "conexao_2": {
            "host": env.get("ORACLE_HOST_1", ""),
            "port": env.get("ORACLE_PORT_1", "1521"),
            "service": env.get("ORACLE_SERVICE_1", ""),
            "user": env.get("DB_USER_1", ""),
            "password": "********" if env.get("DB_PASSWORD_1") else "",
            "configured": bool(env.get("DB_USER_1") and env.get("DB_PASSWORD_1")),
        },
    }


@router.post("/testar", response_model=TestarConexaoResponse)
def testar_conexao(req: TestarConexaoRequest):
    """Testa a conexão Oracle com os parâmetros fornecidos."""
    t0 = perf_counter()
    try:
        import oracledb  # noqa: PLC0415

        if not all([req.host, req.service, req.user, req.password]):
            return {"ok": False, "message": "Preencha host, serviço, usuário e senha.", "tempo_ms": 0}

        porta = int(req.port) if req.port.isdigit() else 1521
        dsn = oracledb.makedsn(req.host, porta, service_name=req.service)
        conn = oracledb.connect(
            user=req.user,
            password=req.password,
            dsn=dsn,
            tcp_connect_timeout=8,
        )
        versao = ""
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT BANNER FROM V$VERSION WHERE ROWNUM = 1")
                row = cur.fetchone()
                if row:
                    versao = str(row[0]).splitlines()[0]
        finally:
            conn.close()

        tempo_ms = int((perf_counter() - t0) * 1000)
        msg = f"Conexão OK ({tempo_ms} ms)"
        if versao:
            msg += f" — {versao}"
        return {"ok": True, "message": msg, "tempo_ms": tempo_ms}

    except ImportError:
        return {"ok": False, "message": "oracledb não instalado.", "tempo_ms": 0}
    except Exception as exc:  # noqa: BLE001
        logger.exception("Erro ao testar conexao Oracle")
        tempo_ms = int((perf_counter() - t0) * 1000)
        return {"ok": False, "message": "Falha na conexão com o banco de dados.", "tempo_ms": tempo_ms}


@router.post("/salvar")
def salvar_config(req: SalvarConfigRequest):
    """Persiste as configurações no arquivo .env da raiz do projeto."""
    try:
        conteudo = _ENV_PATH.read_text(encoding="utf-8") if _ENV_PATH.exists() else ""
        campos = {
            "ORACLE_HOST": req.oracle_host,
            "ORACLE_PORT": req.oracle_port,
            "ORACLE_SERVICE": req.oracle_service,
            "DB_USER": req.db_user,
            "ORACLE_HOST_1": req.oracle_host_1,
            "ORACLE_PORT_1": req.oracle_port_1,
            "ORACLE_SERVICE_1": req.oracle_service_1,
            "DB_USER_1": req.db_user_1,
        }
        if req.db_password != "********":
            campos["DB_PASSWORD"] = req.db_password
        if req.db_password_1 != "********":
            campos["DB_PASSWORD_1"] = req.db_password_1
        # optional fields — only write if non-empty
        for k, v in [("LOG_LEVEL", req.log_level), ("CACHE_ENABLED", req.cache_enabled),
                     ("CACHE_TTL", req.cache_ttl), ("DASHBOARD_THEME", req.dashboard_theme)]:
            if v:
                campos[k] = v

        for chave, valor in campos.items():
            conteudo = _write_key(conteudo, chave, valor)

        _ENV_PATH.parent.mkdir(parents=True, exist_ok=True)
        _ENV_PATH.write_text(conteudo.strip() + "\n", encoding="utf-8")
        return {"ok": True}
    except Exception as exc:
        raise HTTPException(500, "Erro interno ao salvar as configurações.") from exc


@router.get("/verificar/{slot}", response_model=TestarConexaoResponse)
def verificar_conexao_salva(slot: int):
    """
    Testa a conexão Oracle usando as credenciais do .env (sem expô-las ao frontend).
    slot=1 → conexão principal, slot=2 → conexão secundária.
    """
    if slot not in (1, 2):
        raise HTTPException(400, "slot deve ser 1 ou 2")
    env = _read_env()
    suffix = "" if slot == 1 else "_1"
    host = env.get(f"ORACLE_HOST{suffix}", "").strip()
    port = env.get(f"ORACLE_PORT{suffix}", "1521").strip()
    service = env.get(f"ORACLE_SERVICE{suffix}", "").strip()
    user = env.get(f"DB_USER{suffix}", "").strip()
    password = env.get(f"DB_PASSWORD{suffix}", "").strip()

    if not all([host, service, user, password]):
        return {"ok": False, "message": "Credenciais incompletas no .env.", "tempo_ms": 0}

    req = TestarConexaoRequest(host=host, port=port, service=service, user=user, password=password)
    return testar_conexao(req)
