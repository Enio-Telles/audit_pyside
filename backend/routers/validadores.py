"""
Decorator para validação de CNPJ em endpoints FastAPI.
"""

from functools import wraps
from fastapi import HTTPException
from routers._common import validar_cnpj


def validar_cnpj_endpoint(func):
    """
    Decorator que valida o CNPJ antes de executar o endpoint.
    
    Uso:
        @router.get("/{cnpj}/algo")
        @validar_cnpj_endpoint
        def get_algo(cnpj: str):
            ...
    """
    @wraps(func)
    def wrapper(cnpj: str, *args, **kwargs):
        validar_cnpj(cnpj)
        return func(cnpj, *args, **kwargs)
    return wrapper
