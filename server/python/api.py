import re


def _normalizar_cnpj(cnpj: str) -> str:
    """Normaliza CNPJ removendo caracteres nao numericos."""
    cnpj_limpo = re.sub(r"\D", "", cnpj)
    if len(cnpj_limpo) != 14:
        raise ValueError("CNPJ deve conter 14 digitos")
    return cnpj_limpo
