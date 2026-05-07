from __future__ import annotations

class GUIError(Exception):
    """Classe base para erros da interface grafica."""
    pass

class LargeParquetForbiddenError(GUIError):
    """
    Erro disparado quando se tenta carregar um arquivo Parquet muito grande
    inteiro na memoria (full load) sem permissao explicita.
    """
    def __init__(self, path: str, size_mb: float, threshold_mb: int):
        self.path = path
        self.size_mb = size_mb
        self.threshold_mb = threshold_mb
        super().__init__(
            f"Arquivo Parquet muito grande ({size_mb:.1f} MB) para carregamento total. "
            f"O limite de seguranca e de {threshold_mb} MB. "
            f"Utilize filtros, paginação ou o backend DuckDB."
        )
