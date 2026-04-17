from pathlib import Path
import sys


sys.path.insert(0, str(Path("src").resolve()))

from extracao.extracao_oracle_eficiente import (
    ConsultaSql,
    descobrir_consultas_sql,
    obter_caminho_saida_parquet,
)


def test_descobrir_consultas_sql_varre_subpastas_e_preserva_ordem(tmp_path: Path):
    pasta_sql = tmp_path / "sql"
    (pasta_sql / "c100").mkdir(parents=True)
    (pasta_sql / "arquivos_parquet" / "atomizadas" / "c170").mkdir(parents=True)

    consulta_raiz = pasta_sql / "reg_0000.sql"
    consulta_c100 = pasta_sql / "c100" / "10_c100_raw.sql"
    consulta_c170 = (
        pasta_sql / "arquivos_parquet" / "atomizadas" / "c170" / "20_c170_raw.sql"
    )

    consulta_raiz.write_text("select 1 from dual where :CNPJ = :CNPJ")
    consulta_c100.write_text("select 1 from dual where :CNPJ = :CNPJ")
    consulta_c170.write_text("select 1 from dual where :CNPJ = :CNPJ")

    consultas = descobrir_consultas_sql(diretorios_sql=[pasta_sql])

    assert [consulta.caminho for consulta in consultas] == [
        consulta_c170,
        consulta_c100,
        consulta_raiz,
    ]


def test_descobrir_consultas_sql_resolve_selecao_relativa(tmp_path: Path):
    pasta_sql = tmp_path / "sql"
    caminho_consulta = (
        pasta_sql / "arquivos_parquet" / "atomizadas" / "c100" / "10_c100_raw.sql"
    )
    caminho_consulta.parent.mkdir(parents=True)
    caminho_consulta.write_text("select 1 from dual where :CNPJ = :CNPJ")

    consultas = descobrir_consultas_sql(
        consultas_selecionadas=[
            Path("arquivos_parquet/atomizadas/c100/10_c100_raw.sql")
        ],
        diretorios_sql=[pasta_sql],
    )

    assert len(consultas) == 1
    assert consultas[0].caminho == caminho_consulta
    assert consultas[0].caminho_relativo == Path(
        "arquivos_parquet/atomizadas/c100/10_c100_raw.sql"
    )


def test_obter_caminho_saida_parquet_mantem_hierarquia_relativa(tmp_path: Path):
    consulta = ConsultaSql(
        caminho=Path(
            r"c:\projeto\sql\arquivos_parquet\atomizadas\c100\10_c100_raw.sql"
        ),
        raiz_sql=Path(r"c:\projeto\sql"),
    )

    caminho_saida = obter_caminho_saida_parquet(
        consulta=consulta,
        cnpj_limpo="12345678000190",
        pasta_saida_base=tmp_path
        / "dados"
        / "CNPJ"
        / "12345678000190"
        / "arquivos_parquet",
    )

    assert caminho_saida == (
        tmp_path
        / "dados"
        / "CNPJ"
        / "12345678000190"
        / "arquivos_parquet"
        / "atomizadas"
        / "c100"
        / "10_c100_raw_12345678000190.parquet"
    )
