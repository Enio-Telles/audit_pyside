"""
Router Fisconforme — análise cadastral e malhas fiscais.

Extrai dados do Oracle DW (mesma fonte que o projeto C:\\fisconforme)
e armazena em cache Parquet por CNPJ em CNPJ_ROOT/{cnpj}/fisconforme/.
Isso permite reaproveitamento entre consultas individuais e em lote.
"""
from __future__ import annotations

import logging
import os
import re
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import polars as pl
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from interface_grafica.config import CNPJ_ROOT

logger = logging.getLogger(__name__)
router = APIRouter()

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
FISCONFORME_ROOT = Path(r"C:\fisconforme")
SQL_CADASTRAL = FISCONFORME_ROOT / "sql" / "dados_cadastrais.sql"
SQL_MALHA = FISCONFORME_ROOT / "sql" / "Fisconforme_malha_cnpj.sql"
FISCONFORME_ENV = FISCONFORME_ROOT / ".env"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _limpar_cnpj(cnpj: str) -> str:
    return re.sub(r"\D", "", cnpj or "")


def _validar_cnpj(cnpj: str) -> bool:
    c = _limpar_cnpj(cnpj)
    if len(c) != 14 or len(set(c)) == 1:
        return False
    pesos1 = [5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2]
    s1 = sum(int(c[i]) * pesos1[i] for i in range(12))
    r1 = s1 % 11
    dv1 = 0 if r1 < 2 else 11 - r1
    pesos2 = [6, 5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2]
    s2 = sum(int(c[i]) * pesos2[i] for i in range(13))
    r2 = s2 % 11
    dv2 = 0 if r2 < 2 else 11 - r2
    return dv1 == int(c[12]) and dv2 == int(c[13])


def _cache_dir(cnpj: str) -> Path:
    d = CNPJ_ROOT / cnpj / "fisconforme"
    d.mkdir(parents=True, exist_ok=True)
    return d


def _ler_sql(path: Path) -> Optional[str]:
    if not path.exists():
        logger.error("SQL não encontrado: %s", path)
        return None
    try:
        return path.read_text(encoding="utf-8").strip().rstrip(";")
    except UnicodeDecodeError:
        return path.read_text(encoding="latin-1").strip().rstrip(";")


def _conectar_oracle():
    """Conecta ao Oracle usando variáveis do .env do fisconforme."""
    try:
        import oracledb
        from dotenv import load_dotenv

        if FISCONFORME_ENV.exists():
            load_dotenv(dotenv_path=FISCONFORME_ENV, encoding="latin-1", override=True)

        host = os.getenv("ORACLE_HOST", "").strip()
        porta = int(os.getenv("ORACLE_PORT", "1521").strip())
        servico = os.getenv("ORACLE_SERVICE", "sefindw").strip()
        usuario = os.getenv("DB_USER", "").strip()
        senha = os.getenv("DB_PASSWORD", "").strip()

        if not all([host, usuario, senha]):
            raise ValueError("Credenciais Oracle incompletas no .env do fisconforme")

        dsn = oracledb.makedsn(host, porta, service_name=servico)
        conn = oracledb.connect(user=usuario, password=senha, dsn=dsn)
        with conn.cursor() as cur:
            cur.execute("ALTER SESSION SET NLS_NUMERIC_CHARACTERS = '.,'")
        return conn
    except ImportError:
        raise RuntimeError("oracledb não instalado. Execute: pip install oracledb")


# ---------------------------------------------------------------------------
# Cache helpers
# ---------------------------------------------------------------------------

def _cache_cadastral_path(cnpj: str) -> Path:
    return _cache_dir(cnpj) / "dados_cadastrais.parquet"


def _cache_malha_path(cnpj: str) -> Path:
    return _cache_dir(cnpj) / "malhas.parquet"


def _ler_cache_cadastral(cnpj: str) -> Optional[Dict[str, Any]]:
    p = _cache_cadastral_path(cnpj)
    if not p.exists():
        return None
    try:
        df = pl.read_parquet(p)
        if df.is_empty():
            return None
        row = df.row(0, named=True)
        return {k: ("" if v is None else str(v) if not isinstance(v, str) else v) for k, v in row.items()}
    except Exception as exc:
        logger.warning("Erro ao ler cache cadastral %s: %s", cnpj, exc)
        return None


def _salvar_cache_cadastral(cnpj: str, dados: Dict[str, Any]) -> None:
    p = _cache_cadastral_path(cnpj)
    row = {k: [str(v) if v is not None else ""] for k, v in dados.items()}
    row["cached_at"] = [datetime.now().isoformat()]
    try:
        pl.DataFrame(row).write_parquet(p)
        logger.info("Cache cadastral salvo: %s", cnpj)
    except Exception as exc:
        logger.error("Erro ao salvar cache cadastral %s: %s", cnpj, exc)


def _ler_cache_malha(cnpj: str) -> Optional[List[Dict[str, Any]]]:
    p = _cache_malha_path(cnpj)
    if not p.exists():
        return None
    try:
        df = pl.read_parquet(p)
        return df.to_dicts()
    except Exception as exc:
        logger.warning("Erro ao ler cache malha %s: %s", cnpj, exc)
        return None


def _salvar_cache_malha(cnpj: str, registros: List[Dict[str, Any]]) -> None:
    p = _cache_malha_path(cnpj)
    if not registros:
        return
    try:
        pl.DataFrame(registros).write_parquet(p)
        logger.info("Cache malha salvo: %s (%d registros)", cnpj, len(registros))
    except Exception as exc:
        logger.error("Erro ao salvar cache malha %s: %s", cnpj, exc)


# ---------------------------------------------------------------------------
# Oracle extraction
# ---------------------------------------------------------------------------

def _extrair_cadastral_oracle(cnpj: str) -> Optional[Dict[str, Any]]:
    sql = _ler_sql(SQL_CADASTRAL)
    if not sql:
        return None
    conn = _conectar_oracle()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, {"cnpj": cnpj})
            cols = [c[0].upper() for c in cur.description]
            row = cur.fetchone()
            if not row:
                return None
            dados: Dict[str, Any] = {}
            for i, val in enumerate(row):
                dados[cols[i]] = "" if val is None else (val.strip() if isinstance(val, str) else str(val))
            return dados
    finally:
        conn.close()


def _periodo_para_oracle(periodo: str, default: str) -> str:
    """Converte MM/AAAA → YYYYMM para bind Oracle."""
    if periodo and "/" in periodo:
        try:
            m, y = periodo.split("/")
            return f"{y.strip()}{m.strip().zfill(2)}"
        except Exception:
            pass
    return default


def _extrair_malhas_oracle(cnpj: str, data_inicio: str, data_fim: str) -> List[Dict[str, Any]]:
    sql = _ler_sql(SQL_MALHA)
    if not sql:
        return []
    d_ini = _periodo_para_oracle(data_inicio, "190001")
    d_fim = _periodo_para_oracle(data_fim, "209912")
    conn = _conectar_oracle()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, {"cnpj": cnpj, "data_inicio": d_ini, "data_fim": d_fim})
            cols = [c[0].upper() for c in cur.description]
            rows = cur.fetchall()
            resultado = []
            for row in rows:
                r: Dict[str, Any] = {}
                for i, val in enumerate(row):
                    r[cols[i]] = "" if val is None else (val.strip() if isinstance(val, str) else str(val))
                resultado.append(r)
            return resultado
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------

class DbConfigRequest(BaseModel):
    oracle_host: str
    oracle_port: int = 1521
    oracle_service: str = "sefindw"
    db_user: str
    db_password: str


class ConsultaCnpjRequest(BaseModel):
    cnpj: str
    data_inicio: str = "01/2021"
    data_fim: str = "12/2025"
    forcar_atualizacao: bool = False


class ConsultaLoteRequest(BaseModel):
    cnpjs: List[str]
    data_inicio: str = "01/2021"
    data_fim: str = "12/2025"
    forcar_atualizacao: bool = False


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@router.post("/configurar-db")
def configurar_db(req: DbConfigRequest):
    """Salva credenciais Oracle no .env do fisconforme."""
    try:
        lines = []
        if FISCONFORME_ENV.exists():
            lines = FISCONFORME_ENV.read_text(encoding="latin-1").splitlines()

        keys_to_set = {
            "ORACLE_HOST": req.oracle_host,
            "ORACLE_PORT": str(req.oracle_port),
            "ORACLE_SERVICE": req.oracle_service,
            "DB_USER": req.db_user,
            "DB_PASSWORD": req.db_password,
        }
        existing_keys = set()
        new_lines = []
        for line in lines:
            key = line.split("=", 1)[0].strip()
            if key in keys_to_set:
                new_lines.append(f"{key}={keys_to_set[key]}")
                existing_keys.add(key)
            else:
                new_lines.append(line)
        for k, v in keys_to_set.items():
            if k not in existing_keys:
                new_lines.append(f"{k}={v}")

        FISCONFORME_ENV.write_text("\n".join(new_lines), encoding="latin-1")
        return {"ok": True}
    except Exception as exc:
        raise HTTPException(500, f"Erro ao salvar config: {exc}")


@router.get("/testar-conexao")
def testar_conexao():
    """Testa a conexão com Oracle usando as credenciais salvas."""
    try:
        conn = _conectar_oracle()
        conn.close()
        return {"ok": True, "message": "Conexão estabelecida com sucesso"}
    except Exception as exc:
        return {"ok": False, "message": str(exc)}


@router.get("/config")
def obter_config():
    """Retorna as configurações Oracle atuais (sem senha)."""
    from dotenv import dotenv_values
    cfg = dotenv_values(FISCONFORME_ENV, encoding="latin-1") if FISCONFORME_ENV.exists() else {}
    return {
        "oracle_host": cfg.get("ORACLE_HOST", ""),
        "oracle_port": cfg.get("ORACLE_PORT", "1521"),
        "oracle_service": cfg.get("ORACLE_SERVICE", "sefindw"),
        "db_user": cfg.get("DB_USER", ""),
        "configured": bool(cfg.get("DB_USER") and cfg.get("DB_PASSWORD")),
    }


@router.post("/consulta-cadastral")
def consulta_cadastral(req: ConsultaCnpjRequest):
    """Consulta dados cadastrais de um CNPJ (com cache)."""
    cnpj = _limpar_cnpj(req.cnpj)
    if not _validar_cnpj(cnpj):
        raise HTTPException(400, f"CNPJ inválido: {req.cnpj}")

    from_cache = False

    # Dados cadastrais
    dados = None if req.forcar_atualizacao else _ler_cache_cadastral(cnpj)
    if dados:
        from_cache = True
    else:
        try:
            dados = _extrair_cadastral_oracle(cnpj)
            if dados:
                _salvar_cache_cadastral(cnpj, dados)
        except Exception as exc:
            raise HTTPException(503, f"Erro ao consultar Oracle: {exc}")

    # Malhas
    malhas_cache = None if req.forcar_atualizacao else _ler_cache_malha(cnpj)
    if malhas_cache is not None:
        malhas = malhas_cache
    else:
        try:
            malhas = _extrair_malhas_oracle(cnpj, req.data_inicio, req.data_fim)
            _salvar_cache_malha(cnpj, malhas)
        except Exception as exc:
            logger.warning("Erro ao extrair malhas para %s: %s", cnpj, exc)
            malhas = []

    return {
        "cnpj": cnpj,
        "dados_cadastrais": dados,
        "malhas": malhas,
        "from_cache": from_cache,
    }


@router.post("/consulta-lote")
def consulta_lote(req: ConsultaLoteRequest):
    """Consulta dados cadastrais e malhas para múltiplos CNPJs (com cache)."""
    resultados = []
    for cnpj_raw in req.cnpjs:
        cnpj = _limpar_cnpj(cnpj_raw)
        if not _validar_cnpj(cnpj):
            resultados.append({"cnpj": cnpj_raw, "error": "CNPJ inválido", "dados_cadastrais": None, "malhas": [], "from_cache": False})
            continue

        from_cache = False
        dados = None if req.forcar_atualizacao else _ler_cache_cadastral(cnpj)
        if dados:
            from_cache = True
        else:
            try:
                dados = _extrair_cadastral_oracle(cnpj)
                if dados:
                    _salvar_cache_cadastral(cnpj, dados)
            except Exception as exc:
                resultados.append({"cnpj": cnpj, "error": str(exc), "dados_cadastrais": None, "malhas": [], "from_cache": False})
                continue

        malhas_cache = None if req.forcar_atualizacao else _ler_cache_malha(cnpj)
        if malhas_cache is not None:
            malhas = malhas_cache
        else:
            try:
                malhas = _extrair_malhas_oracle(cnpj, req.data_inicio, req.data_fim)
                _salvar_cache_malha(cnpj, malhas)
            except Exception as exc:
                logger.warning("Erro ao extrair malhas %s: %s", cnpj, exc)
                malhas = []

        resultados.append({
            "cnpj": cnpj,
            "dados_cadastrais": dados,
            "malhas": malhas,
            "from_cache": from_cache,
            "error": None,
        })

    return {"total": len(resultados), "resultados": resultados}


@router.get("/cache/stats")
def cache_stats():
    """Retorna estatísticas do cache fisconforme por CNPJ."""
    cached = []
    if CNPJ_ROOT.exists():
        for cnpj_dir in CNPJ_ROOT.iterdir():
            fisc_dir = cnpj_dir / "fisconforme"
            if fisc_dir.exists():
                cached.append({
                    "cnpj": cnpj_dir.name,
                    "tem_cadastral": (fisc_dir / "dados_cadastrais.parquet").exists(),
                    "tem_malhas": (fisc_dir / "malhas.parquet").exists(),
                })
    return {"total_cnpjs_cached": len(cached), "cnpjs": cached}


@router.delete("/cache/{cnpj}")
def limpar_cache_cnpj(cnpj: str):
    """Remove o cache fisconforme de um CNPJ específico."""
    cnpj = _limpar_cnpj(cnpj)
    fisc_dir = CNPJ_ROOT / cnpj / "fisconforme"
    removidos = []
    for f in ["dados_cadastrais.parquet", "malhas.parquet"]:
        p = fisc_dir / f
        if p.exists():
            p.unlink()
            removidos.append(f)
    return {"ok": True, "cnpj": cnpj, "removidos": removidos}
