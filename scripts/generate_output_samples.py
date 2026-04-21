"""Gerar amostras (até N linhas) de Parquet de saída e salvar em Markdown.

Exemplo:
  python scripts/generate_output_samples.py --cnpj 84654326000394

Opções úteis: `--base-dir` para apontar para outra raiz de dados e `--out-dir`.
"""
from __future__ import annotations
import argparse
from pathlib import Path
import datetime
import sys

try:
    import polars as pl
except Exception as e:
    print("Polars não disponível:", e)
    raise


def value_to_str(v):
    if v is None:
        return ""
    if isinstance(v, (list, tuple)):
        return ", ".join(str(x) for x in v)
    if isinstance(v, (datetime.date, datetime.datetime)):
        return v.isoformat()
    s = str(v)
    # escape pipe for markdown
    return s.replace("|", "\\|").replace("\n", " ")


def df_to_markdown(df: pl.DataFrame, max_rows: int = 3) -> str:
    if df.is_empty():
        return "(vazio)\n"
    cols = df.columns
    rows = df.head(max_rows).to_dicts()
    header = "| " + " | ".join(cols) + " |\n"
    sep = "| " + " | ".join(["---"] * len(cols)) + " |\n"
    body = ""
    for r in rows:
        vals = [value_to_str(r.get(c, "")) for c in cols]
        body += "| " + " | ".join(vals) + " |\n"
    return header + sep + body


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Generate markdown samples for generated Parquet outputs")
    p.add_argument("--cnpj", required=True, help="CNPJ to generate samples for")
    p.add_argument("--base-dir", default=str(Path("dados") / "CNPJ"), help="Root CNPJ data directory")
    p.add_argument("--out-dir", default=str(Path("docs") / "referencias" / "samples"), help="Output directory for sample markdowns")
    p.add_argument("--max-rows", type=int, default=3, help="Maximum number of rows to include in samples")
    args = p.parse_args(argv)

    cnpj = ''.join(ch for ch in args.cnpj if ch.isdigit())
    base = Path(args.base_dir) / cnpj / "analises" / "produtos"
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    files = [
        (base / f"mov_estoque_{cnpj}.parquet", f"mov_estoque_{cnpj}"),
        (base / f"aba_periodos_{cnpj}.parquet", f"aba_periodos_{cnpj}"),
        (base / f"aba_mensal_{cnpj}.parquet", f"aba_mensal_{cnpj}"),
        (base / f"aba_anual_{cnpj}.parquet", f"aba_anual_{cnpj}"),
    ]

    created = []
    summary_lines = []
    for path, name in files:
        md_path = out_dir / f"{name}_{cnpj}.md"
        if not path.exists():
            summary_lines.append(f"{path} : NOT FOUND")
            continue
        try:
            df = pl.read_parquet(path)
            nrows = df.height
            ncols = df.width
            md = "# Amostra: {0}\n\n".format(path.name)
            md += f"**Arquivo**: {path}\n\n"
            md += f"**Linhas totais**: {nrows}  \n**Colunas**: {ncols}\n\n"
            md += f"Amostra (até {args.max_rows} linhas):\n\n"
            md += df_to_markdown(df, max_rows=args.max_rows)
            md += "\n"
            md_path.write_text(md, encoding="utf-8")
            created.append(md_path)
            summary_lines.append(f"{md_path} : created, rows={nrows}, cols={ncols}")
        except Exception as e:
            summary_lines.append(f"{path} : ERROR {e}")

    # índice resumido
    index_path = out_dir / f"index_output_samples_{cnpj}.md"
    idx_md = f"# Amostras de saída para CNPJ {cnpj}\n\n"
    idx_md += "Arquivos gerados:\n\n"
    for l in summary_lines:
        idx_md += f"- {l}\n"
    index_path.write_text(idx_md, encoding="utf-8")
    created.append(index_path)

    print("Created files:")
    for p in created:
        print(p)

    return 0


if __name__ == '__main__':
    sys.exit(main())
