import polars as pl
from pathlib import Path

CNJP = "84654326000394"
ROOT = Path(__file__).resolve().parents[1]
ANALISES = ROOT / 'dados' / 'CNPJ' / CNJP / 'analises'
PRODUTOS = ANALISES / 'produtos' / f'produtos_final_{CNJP}.parquet'
OUT = ANALISES / f'fatores_conversao_{CNJP}.parquet'

if not PRODUTOS.exists():
    print('produtos_final not found:', PRODUTOS)
    raise SystemExit(1)

print('Reading', PRODUTOS)
df = pl.read_parquet(PRODUTOS)

if 'id_agrupado' not in df.columns:
    print('id_agrupado not found in produtos_final')
    raise SystemExit(1)

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

# set faktor 1.0
if 'fator' not in df_fatores.columns:
    df_fatores = df_fatores.with_columns(pl.lit(1.0).cast(pl.Float64).alias('fator'))

# deduplicate
df_fatores = df_fatores.unique(subset=['id_agrupado','unid']).sort('id_agrupado')

print('Writing', OUT)
df_fatores.write_parquet(OUT)
print('Done. Wrote', OUT, 'rows=', df_fatores.height)
