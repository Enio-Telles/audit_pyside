import argparse
from pathlib import Path

try:
    import polars as pl
except Exception as exc:  # pragma: no cover - runtime requirement
    raise RuntimeError("Polars is required to run this script") from exc


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Generate conversion factors (fatores) from produtos_final parquet")
    p.add_argument("--cnpj", help="CNPJ to use when locating produtos_final_<cnpj>.parquet")
    p.add_argument("--prod-file", help="Path to produtos_final parquet file (overrides --cnpj)")
    p.add_argument("--out-file", help="Output parquet path for fatores", default=None)
    args = p.parse_args(argv)

    ROOT = Path(__file__).resolve().parents[1]

    if args.prod_file:
        produtos_path = Path(args.prod_file)
        if not produtos_path.exists():
            print('produtos_final not found:', produtos_path)
            return 1
        cnpj = None
    else:
        if not args.cnpj:
            print('Either --cnpj or --prod-file must be provided')
            return 2
        cnpj = ''.join(ch for ch in args.cnpj if ch.isdigit())
        produtos_path = ROOT / 'dados' / 'CNPJ' / cnpj / 'analises' / 'produtos' / f'produtos_final_{cnpj}.parquet'
        if not produtos_path.exists():
            print('produtos_final not found:', produtos_path)
            return 1

    print('Reading', produtos_path)
    df = pl.read_parquet(produtos_path)

    if 'id_agrupado' not in df.columns:
        print('id_agrupado not found in produtos_final')
        return 3

    # prefer unid_ref_sugerida else fallback to None
    if 'unid_ref_sugerida' in df.columns:
        df_fatores = df.select([
            pl.col('id_agrupado').cast(pl.Utf8).alias('id_agrupado'),
            pl.col('unid_ref_sugerida').cast(pl.Utf8).alias('unid_ref'),
        ])
        df_fatores = df_fatores.with_columns(pl.col('unid_ref').alias('unid'))
    else:
        df_fatores = df.select([pl.col('id_agrupado').cast(pl.Utf8).alias('id_agrupado')])
        df_fatores = df_fatores.with_columns(pl.lit(None).alias('unid'))
        df_fatores = df_fatores.with_columns(pl.lit(None).alias('unid_ref'))

    # set fator 1.0 when missing
    if 'fator' not in df_fatores.columns:
        df_fatores = df_fatores.with_columns(pl.lit(1.0).cast(pl.Float64).alias('fator'))

    # deduplicate
    df_fatores = df_fatores.unique(subset=['id_agrupado', 'unid']).sort('id_agrupado')

    if args.out_file:
        out_path = Path(args.out_file)
    elif cnpj:
        out_path = ROOT / 'dados' / 'CNPJ' / cnpj / 'analises' / f'fatores_conversao_{cnpj}.parquet'
    else:
        out_path = Path('fatores_conversao.parquet')

    out_path.parent.mkdir(parents=True, exist_ok=True)
    print('Writing', out_path)
    df_fatores.write_parquet(out_path)
    print('Done. Wrote', out_path, 'rows=', df_fatores.height)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
