from __future__ import annotations

import logging
import re
from pathlib import Path
from typing import Any

import polars as pl
from dotenv import dotenv_values
from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, field_validator

from services.registry_service import RegistryService
from services.parquet_service import ParquetService
from utilitarios.project_paths import CNPJ_ROOT, ENV_PATH
from utilitarios.sql_catalog import resolve_sql_path

router = APIRouter()
registry = RegistryService()
logger = logging.getLogger(__name__)
audit_logger = logging.getLogger("audit.cnpj")

SQL_DADOS_CADASTRAIS = resolve_sql_path("dados_cadastrais.sql")


class CNPJAdd(BaseModel):
    cnpj: str

    @field_validator("cnpj")
    @classmethod
    def validate_cnpj(cls, v: str) -> str:
        cleaned = re.sub(r"\D", "", v or "")
        if len(cleaned) < 11:
            raise ValueError("CNPJ deve ter ao menos 11 dígitos")
        return cleaned


def _sanitize(cnpj: str) -> str:
    return re.sub(r"\D", "", cnpj or "")


def _caminho_parquet_cadastral(svc: ParquetService, cnpj: str) -> Path:
    return svc.cnpj_dir(cnpj) / "arquivos_parquet" / f"dados_cadastrais_{cnpj}.parquet"


def _extrair_razao_social_de_registro(registro: dict[str, Any]) -> str | None:
    """Localiza a razão social sem alterar os nomes de colunas já usados pelo projeto."""
    for coluna in ("razao_social", "RAZAO_SOCIAL", "Nome", "NOME", "nome"):
        valor = registro.get(coluna)
        if valor is not None and str(valor).strip():
            return str(valor).strip()
    return None


def _extrair_nome_fantasia_de_registro(registro: dict[str, Any]) -> str | None:
    """Localiza o nome fantasia nas colunas possíveis do registro cadastral."""
    for coluna in ("Nome Fantasia", "nome_fantasia", "NOME_FANTASIA", "no_fantasia", "NomeFantasia"):
        valor = registro.get(coluna)
        if valor is not None and str(valor).strip():
            return str(valor).strip()
    return None


def _extrair_nome_fantasia_do_parquet(svc: ParquetService, cnpj: str) -> str | None:
    caminho_parquet = _caminho_parquet_cadastral(svc, cnpj)
    if not caminho_parquet.exists():
        return None
    try:
        df = pl.read_parquet(caminho_parquet)
        if df.is_empty():
            return None
        registro = df.row(0, named=True)
        return _extrair_nome_fantasia_de_registro(registro)
    except Exception:
        return None


def _ler_sql_dados_cadastrais() -> str | None:
    if not SQL_DADOS_CADASTRAIS.exists():
        logger.warning("SQL cadastral não encontrada: %s", SQL_DADOS_CADASTRAIS)
        return None

    try:
        return SQL_DADOS_CADASTRAIS.read_text(encoding="utf-8").strip().rstrip(";")
    except UnicodeDecodeError:
        return SQL_DADOS_CADASTRAIS.read_text(encoding="latin-1").strip().rstrip(";")
    except Exception as exc:
        logger.warning("Erro ao ler SQL cadastral %s: %s", SQL_DADOS_CADASTRAIS, exc)
        return None


def _montar_parametros_sql_cadastral(sql: str, cnpj: str) -> dict[str, str]:
    """
    Monta os binds aceitos pela SQL cadastral sem assumir um nome fixo de placeholder.
    """
    placeholders = {
        nome.upper()
        for nome in re.findall(r":([A-Za-z_][A-Za-z0-9_]*)", sql or "")
    }

    parametros: dict[str, str] = {}
    if "CO_CNPJ_CPF" in placeholders:
        parametros["CO_CNPJ_CPF"] = cnpj
    if "CNPJ" in placeholders:
        parametros["CNPJ"] = cnpj
    if "CNPJ_CPF" in placeholders:
        parametros["CNPJ_CPF"] = cnpj
    if "CPF_CNPJ" in placeholders:
        parametros["CPF_CNPJ"] = cnpj

    if not parametros and placeholders:
        # Fallback conservador: se existir somente um bind, reutiliza o CNPJ sem alterar a regra da SQL.
        if len(placeholders) == 1:
            parametros[next(iter(placeholders))] = cnpj

    return parametros


def _consultar_dados_cadastrais_oracle(cnpj: str) -> dict[str, Any] | None:
    """
    Refaz a consulta cadastral somente quando o parquet local não consegue informar a razão social.
    """
    sql = _ler_sql_dados_cadastrais()
    if not sql:
        return None
    parametros_sql = _montar_parametros_sql_cadastral(sql, cnpj)
    if not parametros_sql:
        logger.warning(
            "Não foi possível identificar o bind de CNPJ na SQL cadastral para %s.",
            cnpj,
        )
        return None

    configuracao = dotenv_values(ENV_PATH, encoding="utf-8") if ENV_PATH.exists() else {}
    host = str(configuracao.get("ORACLE_HOST", "") or "").strip()
    porta = str(configuracao.get("ORACLE_PORT", "1521") or "1521").strip()
    servico = str(configuracao.get("ORACLE_SERVICE", "sefindw") or "sefindw").strip()
    usuario = str(configuracao.get("DB_USER", "") or "").strip()
    senha = str(configuracao.get("DB_PASSWORD", "") or "").strip()

    if not all([host, usuario, senha]):
        logger.info("Fallback cadastral ignorado para %s: credenciais Oracle ausentes.", cnpj)
        return None

    try:
        import oracledb
    except ImportError:
        logger.warning("Fallback cadastral indisponível: pacote oracledb não instalado.")
        return None

    try:
        dsn = oracledb.makedsn(host, int(porta or "1521"), service_name=servico)
        conexao = oracledb.connect(user=usuario, password=senha, dsn=dsn)
    except Exception as exc:
        logger.warning("Não foi possível conectar ao Oracle para %s: %s", cnpj, exc)
        return None

    try:
        with conexao.cursor() as cursor:
            # Mantém o mesmo padrão numérico das outras consultas Oracle do sistema.
            cursor.execute("ALTER SESSION SET NLS_NUMERIC_CHARACTERS = '.,'")
            cursor.execute(sql, parametros_sql)
            if not cursor.description:
                return None

            linha = cursor.fetchone()
            if not linha:
                return None

            colunas = [str(coluna[0]) for coluna in cursor.description]
            dados: dict[str, Any] = {}
            for indice, valor in enumerate(linha):
                coluna = colunas[indice]
                if valor is None:
                    dados[coluna] = ""
                elif isinstance(valor, str):
                    dados[coluna] = valor.strip()
                else:
                    dados[coluna] = str(valor)
            return dados
    except Exception as exc:
        logger.warning("Erro ao consultar dados cadastrais no Oracle para %s: %s", cnpj, exc)
        return None
    finally:
        try:
            conexao.close()
        except Exception:
            pass


def _salvar_parquet_cadastral(svc: ParquetService, cnpj: str, dados: dict[str, Any]) -> None:
    if not dados:
        return

    linha_normalizada = {
        coluna: ["" if valor is None else valor if isinstance(valor, str) else str(valor)]
        for coluna, valor in dados.items()
    }
    try:
        svc.save_dataset(_caminho_parquet_cadastral(svc, cnpj), pl.DataFrame(linha_normalizada))
    except Exception as exc:
        logger.warning("Erro ao salvar parquet cadastral para %s: %s", cnpj, exc)


def _extrair_razao_social_do_parquet(svc: ParquetService, cnpj: str) -> str | None:
    """
    Lê a razão social do parquet cadastral do CNPJ, quando disponível.

    A listagem continua funcional mesmo se o parquet estiver ausente ou inválido.
    """
    caminho_parquet = _caminho_parquet_cadastral(svc, cnpj)
    if not caminho_parquet.exists():
        return None

    try:
        df = pl.read_parquet(caminho_parquet)
        if df.is_empty():
            return None

        registro = df.row(0, named=True)
        return _extrair_razao_social_de_registro(registro)
    except Exception:
        return None

    return None


def _obter_razao_social_com_fallback(svc: ParquetService, cnpj: str) -> str | None:
    """
    Mantém o fluxo atual pelo parquet e só recompõe o cache cadastral se o nome estiver ausente.
    """
    razao_social = _extrair_razao_social_do_parquet(svc, cnpj)
    if razao_social:
        return razao_social

    dados_cadastrais = _consultar_dados_cadastrais_oracle(cnpj)
    if not dados_cadastrais:
        return None

    _salvar_parquet_cadastral(svc, cnpj, dados_cadastrais)
    return _extrair_razao_social_de_registro(dados_cadastrais)


@router.get("")
def list_cnpjs():
    records = registry.list_records()
    svc = ParquetService(CNPJ_ROOT)
    discovered = svc.list_cnpjs()
    seen = {r.cnpj for r in records}
    for d in discovered:
        if d not in seen:
            registry.upsert(d)
            seen.add(d)
    return [
        {
            "cnpj": r.cnpj,
            "razao_social": _obter_razao_social_com_fallback(svc, r.cnpj),
            "nome_fantasia": _extrair_nome_fantasia_do_parquet(svc, r.cnpj),
            "added_at": r.added_at,
            "last_run_at": r.last_run_at,
        }
        for r in registry.list_records()
    ]


@router.post("")
def add_cnpj(body: CNPJAdd):
    cnpj = _sanitize(body.cnpj)
    if len(cnpj) < 11:
        raise HTTPException(400, "CNPJ inválido")
    record = registry.upsert(cnpj)
    svc = ParquetService(CNPJ_ROOT)
    return {
        "cnpj": record.cnpj,
        "razao_social": _obter_razao_social_com_fallback(svc, record.cnpj),
        "added_at": record.added_at,
        "last_run_at": record.last_run_at,
    }


@router.delete("/{cnpj}")
def remove_cnpj(cnpj: str):
    cnpj = _sanitize(cnpj)
    # Remove from registry only - does not delete data files
    raw = registry._load_raw()
    new_raw = [r for r in raw if r["cnpj"] != cnpj]
    registry._save_raw(new_raw)
    return {"removed": cnpj}


@router.get("/{cnpj}/files")
def list_files(cnpj: str):
    cnpj = _sanitize(cnpj)
    svc = ParquetService(CNPJ_ROOT)
    files = svc.list_parquet_files(cnpj)
    return [
        {"name": p.name, "path": str(p), "size": p.stat().st_size if p.exists() else 0}
        for p in files
    ]


@router.get("/{cnpj}/schema")
def get_schema(cnpj: str, path: str):
    try:
        p = Path(path).resolve()
        if not p.is_relative_to(CNPJ_ROOT.resolve()):
            raise ValueError()
    except Exception:
        raise HTTPException(400, "Caminho inválido ou acesso negado")
    if not p.exists():
        raise HTTPException(404, "Arquivo não encontrado")
    svc = ParquetService(CNPJ_ROOT)
    cols = svc.get_schema(p)
    return {"columns": cols}


class DeleteFileRequest(BaseModel):
    path: str


@router.delete("/{cnpj}/parquets/single")
def delete_parquet_file(cnpj: str, body: DeleteFileRequest, request: Request):
    """Apaga um arquivo parquet específico do CNPJ."""
    cnpj = _sanitize(cnpj)
    cnpj_root = (CNPJ_ROOT / cnpj).resolve()
    try:
        p = Path(body.path).resolve()
        if not p.is_relative_to(cnpj_root):
            raise ValueError()
    except Exception:
        raise HTTPException(400, "Caminho inválido ou acesso negado")
    if not p.exists():
        raise HTTPException(404, "Arquivo não encontrado")
    p.unlink()
    _ip = request.client.host if request.client else "unknown"
    audit_logger.info("CNPJ %s: arquivo '%s' apagado por %s", cnpj, p.name, _ip)
    return {"deleted": body.path}


@router.delete("/{cnpj}/parquets/all")
def delete_all_parquets(cnpj: str, request: Request):
    """Apaga todos os arquivos parquet listados para o CNPJ."""
    cnpj = _sanitize(cnpj)
    svc = ParquetService(CNPJ_ROOT)
    files = svc.list_parquet_files(cnpj)
    deleted = []
    for p in files:
        try:
            if p.exists():
                p.unlink()
                deleted.append(str(p))
        except Exception as exc:
            logger.warning("Erro ao apagar %s: %s", p, exc)
    _ip = request.client.host if request.client else "unknown"
    audit_logger.info("CNPJ %s: %d arquivo(s) apagados em massa por %s", cnpj, len(deleted), _ip)
    return {"deleted": deleted, "count": len(deleted)}


_PREFIXOS_AGREGACAO = (
    "produtos_agrupados_",
    "produtos_final_",
    "itens_",
    "descricao_produtos_",
    "fontes_produtos_",
    "item_unidades_",
)


@router.delete("/{cnpj}/analises/agregacao")
def delete_agregacao(cnpj: str, request: Request):
    """Apaga os arquivos de agregação de produtos do CNPJ."""
    cnpj = _sanitize(cnpj)
    pasta = CNPJ_ROOT / cnpj / "analises" / "produtos"
    deleted = []
    if pasta.exists():
        for p in pasta.glob("*.parquet"):
            if p.name.startswith(_PREFIXOS_AGREGACAO):
                try:
                    p.unlink()
                    deleted.append(str(p))
                except Exception as exc:
                    logger.warning("Erro ao apagar %s: %s", p, exc)
    _ip = request.client.host if request.client else "unknown"
    audit_logger.info("CNPJ %s: %d arquivo(s) de agregação apagados por %s", cnpj, len(deleted), _ip)
    return {"deleted": deleted, "count": len(deleted)}


@router.delete("/{cnpj}/analises/conversao")
def delete_conversao(cnpj: str, request: Request):
    """Apaga o arquivo de fatores de conversão de unidades do CNPJ."""
    cnpj = _sanitize(cnpj)
    pasta = CNPJ_ROOT / cnpj / "analises" / "produtos"
    deleted = []
    if pasta.exists():
        for p in pasta.glob(f"fatores_conversao_{cnpj}.parquet"):
            try:
                p.unlink()
                deleted.append(str(p))
            except Exception as exc:
                logger.warning("Erro ao apagar %s: %s", p, exc)
    _ip = request.client.host if request.client else "unknown"
    audit_logger.info("CNPJ %s: %d fator(es) de conversão apagados por %s", cnpj, len(deleted), _ip)
    return {"deleted": deleted, "count": len(deleted)}


def _sum_dir_bytes(paths: list[Path]) -> int:
    total = 0
    for p in paths:
        try:
            if p.exists() and p.is_file():
                total += p.stat().st_size
        except Exception:
            pass
    return total


@router.get("/{cnpj}/storage")
def get_storage(cnpj: str):
    """Retorna resumo de espaço em disco usado por seção para o CNPJ."""
    cnpj = _sanitize(cnpj)

    brutos = CNPJ_ROOT / cnpj / "arquivos_parquet"
    analises = CNPJ_ROOT / cnpj / "analises" / "produtos"

    parquet_files: list[Path] = list(brutos.glob("*.parquet")) if brutos.exists() else []
    agregacao_files: list[Path] = []
    conversao_files: list[Path] = []

    if analises.exists():
        for p in analises.glob("*.parquet"):
            if p.name.startswith(_PREFIXOS_AGREGACAO):
                agregacao_files.append(p)
            elif p.name.startswith(f"fatores_conversao_{cnpj}"):
                conversao_files.append(p)

    parquet_bytes = _sum_dir_bytes(parquet_files)
    agregacao_bytes = _sum_dir_bytes(agregacao_files)
    conversao_bytes = _sum_dir_bytes(conversao_files)

    return {
        "cnpj": cnpj,
        "parquet": {"bytes": parquet_bytes, "count": len(parquet_files)},
        "agregacao": {"bytes": agregacao_bytes, "count": len(agregacao_files)},
        "conversao": {"bytes": conversao_bytes, "count": len(conversao_files)},
        "total_bytes": parquet_bytes + agregacao_bytes + conversao_bytes,
    }
