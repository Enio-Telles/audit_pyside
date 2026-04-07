from pathlib import Path
import sys


sys.path.insert(0, str(Path("src").resolve()))

from utilitarios.sql_catalog import list_sql_entries  # noqa: E402


def test_catalogo_sql_ignora_pasta_referencia_e_mantem_oracle_ativa():
    sql_ids = [entry.sql_id for entry in list_sql_entries()]

    assert "arquivos_parquet/atomizadas/ressarcimento_st/referencia/99_consulta_final_consolidada.sql" not in sql_ids
    assert "arquivos_parquet/atomizadas/ressarcimento_st/oracle/10_st_calc_ate_2022.sql" in sql_ids
