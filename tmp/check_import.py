import sys, traceback, os
# Garantir que o src fique no path para imports locais
sys.path.insert(0, "src")
OUT = "tmp/import_result.txt"
with open(OUT, "w", encoding="utf-8") as out:
    try:
        import polars as pl
        import transformacao.rastreabilidade_produtos.fatores_conversao as fmod
        out.write("OK\n")
        out.write(f"polars:{pl.__version__}\n")
    except Exception:
        out.write("ERROR\n")
        traceback.print_exc(file=out)
        sys.exit(1)
