"""
Router Fisconforme — análise cadastral e malhas fiscais.

Extrai dados do Oracle DW (mesma fonte que o projeto C:\\fisconforme)
e armazena em cache Parquet por CNPJ em CNPJ_ROOT/{cnpj}/fisconforme/.
Isso permite reaproveitamento entre consultas individuais e em lote.
"""
from __future__ import annotations

import base64
import json
import logging
import os
import re
import zipfile
from datetime import datetime
from io import BytesIO
from pathlib import Path
from typing import Any, Dict, List, Optional
from uuid import uuid4

import polars as pl
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from starlette.responses import StreamingResponse

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
APP_STATE_ROOT = Path(__file__).resolve().parents[2] / "src" / "workspace" / "app_state"
AUDITOR_CONFIG_PATH = APP_STATE_ROOT / "fisconforme_auditor.json"
DSF_ACERVO_PATH = APP_STATE_ROOT / "fisconforme_dsfs.json"
DSF_FILES_ROOT = APP_STATE_ROOT / "fisconforme_dsfs"


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
# Estado persistido do acervo de DSFs
# ---------------------------------------------------------------------------

def _slugify(value: str) -> str:
    cleaned = re.sub(r"[^0-9A-Za-z._-]+", "_", (value or "").strip())
    return cleaned.strip("._-") or "fisconforme"


def _ler_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        logger.warning("Erro ao ler JSON %s: %s", path, exc)
        return default


def _salvar_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )


def _normalizar_cnpjs(cnpjs: List[str]) -> List[str]:
    vistos = set()
    resultado: List[str] = []
    for item in cnpjs:
        cnpj = _limpar_cnpj(item)
        if not cnpj or cnpj in vistos:
            continue
        vistos.add(cnpj)
        resultado.append(cnpj)
    return resultado


def _carregar_dsfs() -> List[Dict[str, Any]]:
    raw = _ler_json(DSF_ACERVO_PATH, [])
    if not isinstance(raw, list):
        return []
    return [item for item in raw if isinstance(item, dict)]


def _salvar_dsfs(registros: List[Dict[str, Any]]) -> None:
    _salvar_json(
        DSF_ACERVO_PATH,
        sorted(
            registros,
            key=lambda item: str(item.get("updated_at", "")),
            reverse=True,
        ),
    )


def _dsf_pdf_path(dsf_id: str, pdf_file_name: str = "") -> Path:
    ext = Path(pdf_file_name).suffix.lower() or ".pdf"
    return DSF_FILES_ROOT / dsf_id / f"dsf_documento{ext}"


def _montar_resumo_dsf(registro: Dict[str, Any]) -> Dict[str, Any]:
    pdf_file_name = str(registro.get("pdf_file_name", "") or "")
    pdf_path = _dsf_pdf_path(str(registro.get("id", "")), pdf_file_name)
    return {
        "id": str(registro.get("id", "") or ""),
        "dsf": str(registro.get("dsf", "") or ""),
        "referencia": str(registro.get("referencia", "") or ""),
        "auditor": str(registro.get("auditor", "") or ""),
        "cargo_titulo": str(registro.get("cargo_titulo", "") or ""),
        "orgao_origem": str(registro.get("orgao_origem", "") or ""),
        "output_dir": str(registro.get("output_dir", "") or ""),
        "cnpjs": list(registro.get("cnpjs", []) or []),
        "cnpjs_count": len(list(registro.get("cnpjs", []) or [])),
        "data_inicio": str(registro.get("data_inicio", "01/2021") or "01/2021"),
        "data_fim": str(registro.get("data_fim", "12/2025") or "12/2025"),
        "updated_at": str(registro.get("updated_at", "") or ""),
        "created_at": str(registro.get("created_at", "") or ""),
        "pdf_file_name": pdf_file_name,
        "pdf_disponivel": pdf_path.exists(),
    }


def _obter_dsf_por_id(dsf_id: str) -> Dict[str, Any]:
    for item in _carregar_dsfs():
        if str(item.get("id", "")) == dsf_id:
            return item
    raise HTTPException(404, f"DSF não encontrada: {dsf_id}")


def _ler_pdf_base64_do_acervo(dsf_id: str, pdf_file_name: str = "") -> str:
    pdf_path = _dsf_pdf_path(dsf_id, pdf_file_name)
    if not pdf_path.exists():
        return ""
    return base64.b64encode(pdf_path.read_bytes()).decode("ascii")


def _resolver_output_dir(output_dir: str, dsf_id: Optional[str]) -> str:
    if output_dir.strip():
        return output_dir.strip()
    if not dsf_id:
        return ""
    try:
        dsf = _obter_dsf_por_id(dsf_id)
    except HTTPException:
        return ""
    return str(dsf.get("output_dir", "") or "").strip()


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


class DsfAcervoRequest(BaseModel):
    id: Optional[str] = None
    dsf: str
    referencia: str = ""
    cnpjs: List[str] = []
    data_inicio: str = "01/2021"
    data_fim: str = "12/2025"
    forcar_atualizacao: bool = False
    auditor: str = ""
    cargo_titulo: str = ""
    matricula: str = ""
    contato: str = ""
    orgao_origem: str = ""
    output_dir: str = ""
    pdf_file_name: str = ""
    pdf_base64: Optional[str] = None


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


@router.get("/dsfs")
def listar_dsfs():
    """Lista o acervo de DSFs cadastradas para reabertura rápida no frontend."""
    return {"items": [_montar_resumo_dsf(item) for item in _carregar_dsfs()]}


@router.get("/dsfs/{dsf_id}")
def obter_dsf(dsf_id: str):
    """Retorna os dados completos de uma DSF salva no acervo."""
    registro = _obter_dsf_por_id(dsf_id)
    return {
        **_montar_resumo_dsf(registro),
        "matricula": str(registro.get("matricula", "") or ""),
        "contato": str(registro.get("contato", "") or ""),
        "forcar_atualizacao": bool(registro.get("forcar_atualizacao", False)),
    }


@router.post("/dsfs")
def salvar_dsf(req: DsfAcervoRequest):
    """Cria ou atualiza um item do acervo Fisconforme por DSF."""
    registros = _carregar_dsfs()
    agora = datetime.now().isoformat()
    registro_existente: Optional[Dict[str, Any]] = None

    if req.id:
        for item in registros:
            if str(item.get("id", "")) == req.id:
                registro_existente = item
                break

    dsf_id = str((registro_existente or {}).get("id") or req.id or uuid4())
    cnpjs = _normalizar_cnpjs(req.cnpjs)
    payload = {
        "id": dsf_id,
        "dsf": req.dsf.strip(),
        "referencia": req.referencia.strip(),
        "cnpjs": cnpjs,
        "data_inicio": req.data_inicio.strip() or "01/2021",
        "data_fim": req.data_fim.strip() or "12/2025",
        "forcar_atualizacao": bool(req.forcar_atualizacao),
        "auditor": req.auditor.strip(),
        "cargo_titulo": req.cargo_titulo.strip(),
        "matricula": req.matricula.strip(),
        "contato": req.contato.strip(),
        "orgao_origem": req.orgao_origem.strip(),
        "output_dir": req.output_dir.strip(),
        "pdf_file_name": req.pdf_file_name.strip()
        or str((registro_existente or {}).get("pdf_file_name", "") or ""),
        "created_at": str((registro_existente or {}).get("created_at", "") or agora),
        "updated_at": agora,
    }

    if registro_existente:
        registros = [item for item in registros if str(item.get("id", "")) != dsf_id]

    if req.pdf_base64 is not None:
        pdf_path = _dsf_pdf_path(dsf_id, payload["pdf_file_name"])
        pdf_path.parent.mkdir(parents=True, exist_ok=True)
        if req.pdf_base64.strip():
            pdf_path.write_bytes(base64.b64decode(req.pdf_base64))
        elif pdf_path.exists():
            pdf_path.unlink()

    registros.append(payload)
    _salvar_dsfs(registros)

    if payload["auditor"] or payload["cargo_titulo"] or payload["matricula"]:
        _salvar_auditor_config(
            AuditorConfigRequest(
                auditor=payload["auditor"],
                cargo_titulo=payload["cargo_titulo"],
                matricula=payload["matricula"],
                contato=payload["contato"],
                orgao_origem=payload["orgao_origem"],
            )
        )

    return {
        **_montar_resumo_dsf(payload),
        "matricula": payload["matricula"],
        "contato": payload["contato"],
        "forcar_atualizacao": payload["forcar_atualizacao"],
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


# ---------------------------------------------------------------------------
# Modelos e helpers para geração de notificação
# ---------------------------------------------------------------------------

MODELO_NOTIFICACAO = (
    Path(__file__).resolve().parent.parent.parent
    / "modelo"
    / "modelo_notificacao_fisconforme_n_atendido.txt"
)


class GerarNotificacaoRequest(BaseModel):
    cnpj: str
    dsf: str = ""
    dsf_id: Optional[str] = None
    auditor: str
    cargo_titulo: str = ""
    matricula: str = ""
    contato: str = ""
    orgao_origem: str = ""
    output_dir: str = ""
    pdf_base64: Optional[str] = None


class GerarNotificacoesLoteRequest(BaseModel):
    cnpjs: List[str]
    dsf: str = ""
    dsf_id: Optional[str] = None
    auditor: str
    cargo_titulo: str = ""
    matricula: str = ""
    contato: str = ""
    orgao_origem: str = ""
    output_dir: str = ""
    pdf_base64: Optional[str] = None


class AuditorConfigRequest(BaseModel):
    auditor: str = ""
    cargo_titulo: str = ""
    matricula: str = ""
    contato: str = ""
    orgao_origem: str = ""


def _gerar_tabela_html(malhas: List[Dict[str, Any]]) -> str:
    """Gera tabela HTML de pendências para inserção no template."""
    if not malhas:
        return "<p><em>(Sem pendências registradas)</em></p>"

    colunas = [
        ("ID Pend.", "ID_PENDENCIA"),
        ("ID Notif.", "ID_NOTIFICACAO"),
        ("Malha ID", "MALHAS_ID"),
        ("Título", "TITULO_MALHA"),
        ("Período", "PERIODO"),
        ("Status Pend.", "STATUS_PENDENCIA"),
        ("Status Notif.", "STATUS_NOTIFICACAO"),
        ("Ciência", "DATA_CIENCIA_CONSOLIDADA"),
    ]

    estilo_th = (
        "border:1px solid #ccc;padding:4px 8px;background:#f0f0f0;"
        "font-family:Arial,sans-serif;font-size:10px;"
    )
    estilo_td = (
        "border:1px solid #ccc;padding:4px 8px;"
        "font-family:Arial,sans-serif;font-size:10px;"
    )

    linhas = ["<table style='border-collapse:collapse;width:100%;'>", "<thead><tr>"]
    for label, _ in colunas:
        linhas.append(f"<th style='{estilo_th}'>{label}</th>")
    linhas.append("</tr></thead><tbody>")

    for m in malhas:
        linhas.append("<tr>")
        for _, col in colunas:
            val = m.get(col) or m.get(col.lower(), "")
            linhas.append(f"<td style='{estilo_td}'>{val}</td>")
        linhas.append("</tr>")

    linhas.append("</tbody></table>")
    return "\n".join(linhas)


def _converter_pdf_base64_para_html(pdf_base64: str) -> str:
    """Converte PDF em base64 para conjunto de tags <img> em HTML (uma por página)."""
    try:
        import base64
        import io
        import fitz  # PyMuPDF

        target_width = 547
        target_height = 775
        pdf_bytes = base64.b64decode(pdf_base64)
        doc = fitz.open(stream=io.BytesIO(pdf_bytes), filetype="pdf")
        imgs: List[str] = []
        for page in doc:
            page_rect = page.rect
            scale_x = target_width / max(page_rect.width, 1)
            scale_y = target_height / max(page_rect.height, 1)
            mat = fitz.Matrix(scale_x, scale_y)
            pix = page.get_pixmap(matrix=mat)
            png_bytes = pix.tobytes("png")
            b64 = base64.b64encode(png_bytes).decode()
            imgs.append(
                f'<img src="data:image/png;base64,{b64}" '
                f'width="{target_width}" height="{target_height}" '
                f'style="width:{target_width}px;height:{target_height}px;display:block;margin-bottom:10px;" />'
            )
        doc.close()
        return "\n".join(imgs)
    except ImportError:
        logger.warning("PyMuPDF (fitz) não instalado — DSF_IMAGENS será vazio.")
        return ""
    except Exception as exc:
        logger.warning("Erro ao converter PDF: %s", exc)
        return ""


def _carregar_auditor_config() -> Dict[str, str]:
    try:
        raw = _ler_json(AUDITOR_CONFIG_PATH, {})
        if not isinstance(raw, dict):
            raise ValueError("Conteúdo inválido")

        return {
            "auditor": str(raw.get("auditor", "") or ""),
            "cargo_titulo": str(raw.get("cargo_titulo", "") or ""),
            "matricula": str(raw.get("matricula", "") or ""),
            "contato": str(raw.get("contato", "") or ""),
            "orgao_origem": str(raw.get("orgao_origem", "") or ""),
        }
    except Exception as exc:
        logger.warning("Erro ao carregar config do auditor: %s", exc)
        return {
            "auditor": "",
            "cargo_titulo": "",
            "matricula": "",
            "contato": "",
            "orgao_origem": "",
        }


def _salvar_auditor_config(req: AuditorConfigRequest) -> None:
    payload = {
        "auditor": req.auditor,
        "cargo_titulo": req.cargo_titulo,
        "matricula": req.matricula,
        "contato": req.contato,
        "orgao_origem": req.orgao_origem,
        "saved_at": datetime.now().isoformat(),
    }
    _salvar_json(AUDITOR_CONFIG_PATH, payload)


def _salvar_notificacao_em_disco(
    conteudo: str,
    nome_arquivo: str,
    output_dir: str,
) -> str:
    target_dir = Path(output_dir).expanduser()
    target_dir.mkdir(parents=True, exist_ok=True)
    target_path = target_dir / nome_arquivo
    target_path.write_text(conteudo, encoding="utf-8")
    return str(target_path)


def _montar_notificacao(
    cnpj: str,
    dsf: str,
    dsf_id: Optional[str],
    auditor: str,
    cargo_titulo: str,
    matricula: str,
    contato: str,
    orgao_origem: str,
    pdf_base64: Optional[str] = None,
) -> tuple[str, str]:
    if not MODELO_NOTIFICACAO.exists():
        raise HTTPException(500, f"Template não encontrado: {MODELO_NOTIFICACAO}")

    dados = _ler_cache_cadastral(cnpj) or {}
    malhas = _ler_cache_malha(cnpj) or []

    def _get(*keys: str) -> str:
        for k in keys:
            v = dados.get(k) or dados.get(k.upper()) or dados.get(k.lower())
            if v:
                return str(v)
        return ""

    razao_social = _get("NOME", "RAZAO_SOCIAL", "Nome", "razao_social")
    ie = _get("IE", "INSCRICAO_ESTADUAL")

    tabela_html = _gerar_tabela_html(malhas)
    pdf_origem = pdf_base64
    if not pdf_origem and dsf_id:
        registro_dsf = _obter_dsf_por_id(dsf_id)
        pdf_origem = _ler_pdf_base64_do_acervo(
            dsf_id,
            str(registro_dsf.get("pdf_file_name", "") or ""),
        )
    dsf_imagens = _converter_pdf_base64_para_html(pdf_origem) if pdf_origem else ""

    conteudo = MODELO_NOTIFICACAO.read_text(encoding="utf-8")
    substituicoes = {
        "{{RAZAO_SOCIAL}}": razao_social,
        "{{CNPJ}}": cnpj,
        "{{IE}}": ie,
        "{{DSF}}": dsf,
        "{{AUDITOR}}": auditor,
        "{{CARGO_TITULO}}": cargo_titulo,
        "{{MATRICULA}}": matricula,
        "{{CONTATO}}": contato,
        "{{ORGAO_ORIGEM}}": orgao_origem,
        "{{TABELA}}": tabela_html,
        "{{DSF_IMAGENS}}": dsf_imagens,
    }
    for placeholder, valor in substituicoes.items():
        conteudo = conteudo.replace(placeholder, valor)

    return conteudo, f"notificacao_det_{cnpj}.txt"


@router.get("/auditor-config")
def obter_auditor_config():
    """Retorna os dados salvos do auditor para reutilização na UI web."""
    return _carregar_auditor_config()


@router.post("/auditor-config")
def salvar_auditor_config(req: AuditorConfigRequest):
    """Persiste os dados do auditor em arquivo local do projeto."""
    try:
        _salvar_auditor_config(req)
        return {"ok": True}
    except Exception as exc:
        raise HTTPException(500, f"Erro ao salvar dados do auditor: {exc}") from exc


@router.post("/gerar-notificacao")
def gerar_notificacao(req: GerarNotificacaoRequest):
    """Gera o HTML da notificação preenchido com dados cadastrais e do auditor."""
    cnpj = _limpar_cnpj(req.cnpj)
    if not _validar_cnpj(cnpj):
        raise HTTPException(400, f"CNPJ inválido: {req.cnpj}")

    if not MODELO_NOTIFICACAO.exists():
        raise HTTPException(500, f"Template não encontrado: {MODELO_NOTIFICACAO}")

    # Lê dados cadastrais do cache (já salvos em parquet pela consulta)
    dados = _ler_cache_cadastral(cnpj) or {}
    malhas = _ler_cache_malha(cnpj) or []

    # Determina valores de cada placeholder com fallbacks
    def _get(*keys: str) -> str:
        for k in keys:
            v = dados.get(k) or dados.get(k.upper()) or dados.get(k.lower())
            if v:
                return str(v)
        return ""

    razao_social = _get("NOME", "RAZAO_SOCIAL", "Nome", "razao_social")
    ie = _get("IE", "INSCRICAO_ESTADUAL")

    tabela_html = _gerar_tabela_html(malhas)
    dsf_imagens = _converter_pdf_base64_para_html(req.pdf_base64) if req.pdf_base64 else ""

    conteudo = MODELO_NOTIFICACAO.read_text(encoding="utf-8")
    substituicoes = {
        "{{RAZAO_SOCIAL}}": razao_social,
        "{{CNPJ}}": req.cnpj,
        "{{IE}}": ie,
        "{{DSF}}": req.dsf,
        "{{AUDITOR}}": req.auditor,
        "{{CARGO_TITULO}}": req.cargo_titulo,
        "{{MATRICULA}}": req.matricula,
        "{{CONTATO}}": req.contato,
        "{{TABELA}}": tabela_html,
        "{{DSF_IMAGENS}}": dsf_imagens,
    }
    for placeholder, valor in substituicoes.items():
        conteudo = conteudo.replace(placeholder, valor)

    nome_arquivo = f"notificacao_det_{cnpj}.txt"
    return {"conteudo": conteudo, "nome_arquivo": nome_arquivo}


@router.post("/gerar-notificacao-v2")
def gerar_notificacao_v2(req: GerarNotificacaoRequest):
    """Gera a notificação com suporte a órgão de origem."""
    cnpj = _limpar_cnpj(req.cnpj)
    if not _validar_cnpj(cnpj):
        raise HTTPException(400, f"CNPJ inválido: {req.cnpj}")

    conteudo, nome_arquivo = _montar_notificacao(
        cnpj=cnpj,
        dsf=req.dsf,
        dsf_id=req.dsf_id,
        auditor=req.auditor,
        cargo_titulo=req.cargo_titulo,
        matricula=req.matricula,
        contato=req.contato,
        orgao_origem=req.orgao_origem,
        pdf_base64=req.pdf_base64,
    )
    output_dir = _resolver_output_dir(req.output_dir, req.dsf_id)
    salvo_em = ""
    if output_dir:
        salvo_em = _salvar_notificacao_em_disco(conteudo, nome_arquivo, output_dir)
    return {"conteudo": conteudo, "nome_arquivo": nome_arquivo, "salvo_em": salvo_em}


@router.post("/gerar-notificacoes-lote")
def gerar_notificacoes_lote(req: GerarNotificacoesLoteRequest):
    """Gera um arquivo ZIP contendo todas as notificações solicitadas."""
    cnpjs_validos: List[str] = []
    for cnpj_raw in req.cnpjs:
        cnpj = _limpar_cnpj(cnpj_raw)
        if not cnpj:
            continue
        if not _validar_cnpj(cnpj):
            raise HTTPException(400, f"CNPJ inválido no lote: {cnpj_raw}")
        cnpjs_validos.append(cnpj)

    if not cnpjs_validos:
        raise HTTPException(400, "Nenhum CNPJ válido informado para geração em lote.")

    output_dir = _resolver_output_dir(req.output_dir, req.dsf_id)
    zip_buffer = BytesIO()
    with zipfile.ZipFile(zip_buffer, "w", compression=zipfile.ZIP_DEFLATED) as zip_file:
        for cnpj in cnpjs_validos:
            conteudo, nome_arquivo = _montar_notificacao(
                cnpj=cnpj,
                dsf=req.dsf,
                dsf_id=req.dsf_id,
                auditor=req.auditor,
                cargo_titulo=req.cargo_titulo,
                matricula=req.matricula,
                contato=req.contato,
                orgao_origem=req.orgao_origem,
                pdf_base64=req.pdf_base64,
            )
            zip_file.writestr(nome_arquivo, conteudo)
            if output_dir:
                _salvar_notificacao_em_disco(conteudo, nome_arquivo, output_dir)

    zip_buffer.seek(0)
    nome_zip = f"notificacoes_fisconforme_{datetime.now().strftime('%Y%m%d_%H%M%S')}.zip"
    headers = {"Content-Disposition": f'attachment; filename="{nome_zip}"'}
    if output_dir:
        target_dir = Path(output_dir).expanduser()
        target_dir.mkdir(parents=True, exist_ok=True)
        (target_dir / nome_zip).write_bytes(zip_buffer.getvalue())
        headers["X-Saved-To"] = str(target_dir)
        headers["X-Saved-Count"] = str(len(cnpjs_validos))
    return StreamingResponse(zip_buffer, media_type="application/zip", headers=headers)
