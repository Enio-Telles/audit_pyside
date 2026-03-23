"""Modulo auxiliar para identificar bind variables em comandos SQL."""
from __future__ import annotations

import re


def extrair_parametros_sql(sql: str) -> set[str]:
    """
    Identifica variaveis de ligacao no formato :nome dentro de uma string SQL.

    Regras extras:
    - ignora classes POSIX usadas em regex Oracle (ex.: [:alnum:], [:digit:]);
    - ignora ocorrencias ':' logo apos '[' para evitar falsos positivos.
    """
    # Remove classes POSIX, que podem aparecer em REGEXP_REPLACE e similares.
    sql_sem_posix = re.sub(r"\[\s*:[A-Za-z_]\w*\s*:\s*\]", " ", sql)

    # Captura :nome (nome iniciando por letra/underscore), evitando : dentro de '[...]'.
    binds = re.findall(r"(?<!\[):([A-Za-z_]\w*)", sql_sem_posix)
    return set(binds)


def extract_sql_parameters(sql: str) -> list[dict]:
    """
    Identifica bind variables e retorna metadados inferidos para a UI.
    """
    binds = extrair_parametros_sql(sql)
    parametros = []

    for bind in binds:
        bind_lower = bind.lower()

        if "data" in bind_lower:
            tipo = "date"
        elif "cnpj" in bind_lower:
            tipo = "text"
        elif (
            "valor" in bind_lower
            or "numero" in bind_lower
            or "qtd" in bind_lower
            or "quantidade" in bind_lower
        ):
            tipo = "number"
        else:
            tipo = "text"

        required = "cnpj" in bind_lower

        parametros.append({"name": bind, "type": tipo, "required": required})

    return parametros
