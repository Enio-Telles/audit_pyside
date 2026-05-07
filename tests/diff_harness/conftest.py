"""Fixtures Polars deterministicas para os testes do differential harness."""
import os
from pathlib import Path

import pytest
import polars as pl


def pytest_addoption(parser: pytest.Parser) -> None:
    """Adiciona opcoes de CLI para validacao v1/v2 de Parquets."""
    group = parser.getgroup("diff_harness")
    group.addoption(
        "--parquet-v1",
        default=os.environ.get("AUDIT_PYSIDE_PARQUET_V1"),
        help=(
            "Caminho do Parquet v1 legado. "
            "Default: env AUDIT_PYSIDE_PARQUET_V1."
        ),
    )
    group.addoption(
        "--parquet-v2",
        default=os.environ.get("AUDIT_PYSIDE_PARQUET_V2"),
        help=(
            "Caminho do Parquet v2 rewriteado com typing/dictionary. "
            "Default: env AUDIT_PYSIDE_PARQUET_V2."
        ),
    )
    group.addoption(
        "--diff-harness-synthetic",
        action="store_true",
        help="Gera Parquets v1/v2 sinteticos para validar a logica dos testes.",
    )


def pytest_configure(config: pytest.Config) -> None:
    """Registra o marker diff_harness para evitar warnings locais."""
    config.addinivalue_line(
        "markers",
        "diff_harness: testes diferenciais byte-a-byte para invariantes fiscais",
    )


def _make_synthetic_v1(path: Path, n: int = 50_000) -> None:
    """Gera Parquet v1 sintetico com colunas string."""
    df = pl.DataFrame(
        {
            "cfop": [["5102", "5101", "5405", "1102", "6102"][i % 5] for i in range(n)],
            "cst_icms": [["000", "010", "060", "090"][i % 4] for i in range(n)],
            "uf": [["RO", "SP", "MG", "RJ", "PR"][i % 5] for i in range(n)],
            "tipo_operacao": [["0", "1"][i % 2] for i in range(n)],
            "mod": [["55", "65"][i % 2] for i in range(n)],
            "ncm": [str(10_000_000 + (i % 8000)) for i in range(n)],
            "id_agrupado": [f"grp_{i % 1000}" for i in range(n)],
            "id_agregado": [f"agg_{i % 5000}" for i in range(n)],
            "__qtd_decl_final_audit__": [float(i % 1000) for i in range(n)],
            "q_conv": [float(i % 100) for i in range(n)],
            "q_conv_fisica": [float(i % 100) * 1.5 for i in range(n)],
        }
    )
    df.write_parquet(path, use_pyarrow=False, statistics=True)


def _make_synthetic_v2(path: Path, n: int = 50_000) -> None:
    """Gera Parquet v2 sintetico semanticamente igual e tipado."""
    df = pl.DataFrame(
        {
            "cfop": [["5102", "5101", "5405", "1102", "6102"][i % 5] for i in range(n)],
            "cst_icms": [["000", "010", "060", "090"][i % 4] for i in range(n)],
            "uf": [["RO", "SP", "MG", "RJ", "PR"][i % 5] for i in range(n)],
            "tipo_operacao": [["0", "1"][i % 2] for i in range(n)],
            "mod": [["55", "65"][i % 2] for i in range(n)],
            "ncm": [str(10_000_000 + (i % 8000)) for i in range(n)],
            "id_agrupado": [f"grp_{i % 1000}" for i in range(n)],
            "id_agregado": [f"agg_{i % 5000}" for i in range(n)],
            "__qtd_decl_final_audit__": [float(i % 1000) for i in range(n)],
            "q_conv": [float(i % 100) for i in range(n)],
            "q_conv_fisica": [float(i % 100) * 1.5 for i in range(n)],
        }
    ).with_columns(
        [
            pl.col("cfop").cast(pl.Enum(["1102", "5101", "5102", "5405", "6102"])),
            pl.col("cst_icms").cast(pl.Enum(["000", "010", "060", "090"])),
            pl.col("uf").cast(pl.Enum(["MG", "PR", "RJ", "RO", "SP"])),
            pl.col("tipo_operacao").cast(pl.Enum(["0", "1"])),
            pl.col("mod").cast(pl.Enum(["55", "65"])),
            pl.col("ncm").cast(pl.Categorical()),
        ]
    )
    df.write_parquet(
        path,
        compression="zstd",
        compression_level=3,
        statistics=True,
        row_group_size=10_000,
    )


@pytest.fixture(scope="session")
def synthetic_v1_path(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """Path do Parquet v1 sintetico."""
    path = tmp_path_factory.mktemp("diff_harness") / "synthetic_v1.parquet"
    _make_synthetic_v1(path)
    return path


@pytest.fixture(scope="session")
def synthetic_v2_path(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """Path do Parquet v2 sintetico."""
    path = tmp_path_factory.mktemp("diff_harness") / "synthetic_v2.parquet"
    _make_synthetic_v2(path)
    return path


@pytest.fixture()
def parquet_v1(request: pytest.FixtureRequest, synthetic_v1_path: Path) -> Path:
    """Resolve o Parquet v1 por CLI/env ou pelo modo sintetico."""
    cli_path = request.config.getoption("--parquet-v1", default=None)
    synthetic = request.config.getoption("--diff-harness-synthetic", default=False)

    if cli_path:
        path = Path(cli_path)
        if not path.exists():
            pytest.fail(f"Parquet v1 nao existe: {path}")
        return path
    if synthetic:
        return synthetic_v1_path
    pytest.skip(
        "Parquet v1 nao disponivel. Use --parquet-v1=PATH, "
        "AUDIT_PYSIDE_PARQUET_V1=PATH ou --diff-harness-synthetic."
    )


@pytest.fixture()
def parquet_v2(request: pytest.FixtureRequest, synthetic_v2_path: Path) -> Path:
    """Resolve o Parquet v2 por CLI/env ou pelo modo sintetico."""
    cli_path = request.config.getoption("--parquet-v2", default=None)
    synthetic = request.config.getoption("--diff-harness-synthetic", default=False)

    if cli_path:
        path = Path(cli_path)
        if not path.exists():
            pytest.fail(f"Parquet v2 nao existe: {path}")
        return path
    if synthetic:
        return synthetic_v2_path
    pytest.skip(
        "Parquet v2 ainda nao disponivel. Use --parquet-v2=PATH, "
        "AUDIT_PYSIDE_PARQUET_V2=PATH ou --diff-harness-synthetic."
    )


@pytest.fixture()
def df_baseline_nfe() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "chave_acesso": [f"NFE_{i:04d}" for i in range(10)],
            "prod_nitem": [1] * 10,
            "id_agrupado": [f"AGR_{i % 3}" for i in range(10)],
        }
    )


@pytest.fixture()
def df_novo_identico(df_baseline_nfe: pl.DataFrame) -> pl.DataFrame:
    return df_baseline_nfe.clone()


@pytest.fixture()
def df_novo_com_divergencia(df_baseline_nfe: pl.DataFrame) -> pl.DataFrame:
    return df_baseline_nfe.with_columns(
        pl.when(pl.col("chave_acesso") == "NFE_0000")
        .then(pl.lit("AGR_ERRADO"))
        .otherwise(pl.col("id_agrupado"))
        .alias("id_agrupado")
    )


@pytest.fixture()
def df_vazio() -> pl.DataFrame:
    return pl.DataFrame(
        {"chave_acesso": pl.Series([], dtype=pl.Utf8), "prod_nitem": pl.Series([], dtype=pl.Int64)}
    )
