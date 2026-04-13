"""
Serviço de configuração para o módulo Fisconforme integrado.

Expõe as funções de leitura/escrita do .env e de perfis de auditores,
originalmente definidas em gerar_notificacoes.py, como um serviço isolado.
"""
from __future__ import annotations

import logging
import re
from pathlib import Path
from typing import Dict

from .path_resolver import get_env_path

logger = logging.getLogger(__name__)


def carregar_config_db() -> Dict[str, str]:
    """
    Carrega as configurações de banco de dados Oracle do .env.
    Retorna padrões da SEFIN se não preenchido.
    """
    try:
        from dotenv import dotenv_values

        env_path = get_env_path()

        padroes: Dict[str, str] = {
            "ORACLE_HOST": "exa01-scan.sefin.ro.gov.br",
            "ORACLE_PORT": "1521",
            "ORACLE_SERVICE": "sefindw",
            "DB_USER": "",
            "DB_PASSWORD": "",
        }

        if not env_path.exists():
            logger.warning(f"Arquivo .env não encontrado em: {env_path}")
            return padroes

        env_vars = dotenv_values(env_path)

        return {
            "ORACLE_HOST": env_vars.get("ORACLE_HOST", padroes["ORACLE_HOST"]),
            "ORACLE_PORT": env_vars.get("ORACLE_PORT", padroes["ORACLE_PORT"]),
            "ORACLE_SERVICE": env_vars.get("ORACLE_SERVICE", padroes["ORACLE_SERVICE"]),
            "DB_USER": env_vars.get("DB_USER", ""),
            "DB_PASSWORD": env_vars.get("DB_PASSWORD", ""),
        }
    except Exception as e:
        logger.warning(f"Erro ao carregar configurações de DB: {e}")
        return {}


def salvar_config_db(dados: Dict[str, str]) -> bool:
    """
    Salva as configurações de conexão Oracle no .env.
    """
    try:
        env_path = get_env_path()

        conteudo_atual = ""
        if env_path.exists():
            with open(env_path, "r", encoding="utf-8") as f:
                conteudo_atual = f.read()

        chaves = ["ORACLE_HOST", "ORACLE_PORT", "ORACLE_SERVICE", "DB_USER", "DB_PASSWORD"]
        conteudo_final = conteudo_atual
        for chave in chaves:
            valor = str(dados.get(chave, "")).strip()
            if re.search(rf"^{chave}=", conteudo_final, flags=re.MULTILINE):
                conteudo_final = re.sub(
                    rf"^{chave}=.*$",
                    f"{chave}={valor}",
                    conteudo_final,
                    flags=re.MULTILINE,
                )
            else:
                conteudo_final = conteudo_final.rstrip() + f"\n{chave}={valor}\n"

        env_path.parent.mkdir(parents=True, exist_ok=True)
        with open(env_path, "w", encoding="utf-8") as f:
            f.write(conteudo_final.strip() + "\n")

        logger.info(f"Configurações de banco de dados salvas em: {env_path}")
        return True
    except Exception as e:
        logger.error(f"Erro ao salvar configurações de DB: {e}")
        return False


def carregar_dados_salvos() -> Dict[str, Dict[str, str]]:
    """
    Carrega perfis de auditores salvos no .env.

    Procura por variáveis no formato CONFIG_<NOME>_AUDITOR, etc.
    """
    try:
        from dotenv import dotenv_values

        env_path = get_env_path()
        if not env_path.exists():
            return {}

        env_vars = dotenv_values(env_path)
        configs: Dict[str, Dict[str, str]] = {}
        padrao_config = re.compile(r"^CONFIG_(.+)_AUDITOR$")

        for chave, valor in env_vars.items():
            match = padrao_config.match(chave)
            if match:
                nome_config = match.group(1)
                configs[nome_config] = {
                    "AUDITOR": str(valor or ""),
                    "MATRICULA": str(env_vars.get(f"CONFIG_{nome_config}_MATRICULA", "") or ""),
                    "DSF": str(env_vars.get(f"CONFIG_{nome_config}_DSF", "") or ""),
                    "CONTATO": str(env_vars.get(f"CONFIG_{nome_config}_CONTATO", "") or ""),
                    "ORGAO": str(env_vars.get(f"CONFIG_{nome_config}_ORGAO", "") or ""),
                }

        return configs
    except Exception as e:
        logger.warning(f"Erro ao carregar configurações salvas: {e}")
        return {}


def salvar_dados_manuais(nome_config: str, dados: Dict[str, str]) -> None:
    """
    Salva um perfil de auditor no .env.

    Args:
        nome_config: Nome identificador do perfil
        dados: Dicionário com AUDITOR, MATRICULA, DSF, CONTATO, ORGAO
    """
    try:
        env_path = get_env_path()

        nome_config_sanitizado = re.sub(r"[^a-zA-Z0-9_]", "_", nome_config).strip("_")
        nome_config_sanitizado = re.sub(r"_+", "_", nome_config_sanitizado)

        campos = ["AUDITOR", "MATRICULA", "DSF", "CONTATO", "ORGAO"]
        linhas_config = [
            f"CONFIG_{nome_config_sanitizado}_{campo}={dados.get(campo, '')}"
            for campo in campos
        ]

        conteudo_atual = ""
        if env_path.exists():
            with open(env_path, "r", encoding="utf-8") as f:
                conteudo_atual = f.read()

        conteudo_sem_antigo = re.sub(
            rf"^CONFIG_{re.escape(nome_config_sanitizado)}_.*$\n?",
            "",
            conteudo_atual,
            flags=re.MULTILINE,
        )

        secao_existe = "# CONFIGURAÇÕES SALVAS DE AUDITORES" in conteudo_sem_antigo
        if not secao_existe:
            conteudo_final = conteudo_sem_antigo.rstrip() + "\n\n"
            conteudo_final += "# =============================================================================\n"
            conteudo_final += "# CONFIGURAÇÕES SALVAS DE AUDITORES\n"
            conteudo_final += "# =============================================================================\n"
        else:
            conteudo_final = conteudo_sem_antigo

        conteudo_final += "\n".join(linhas_config) + "\n"

        env_path.parent.mkdir(parents=True, exist_ok=True)
        with open(env_path, "w", encoding="utf-8") as f:
            f.write(conteudo_final)

        logger.info(f"Configuração '{nome_config_sanitizado}' salva em: {env_path}")
    except Exception as e:
        logger.error(f"Erro ao salvar configuração: {e}")
