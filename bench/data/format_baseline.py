import json
import sys
from pathlib import Path


def format_report():
    json_path = Path("docs/baseline_performance.json")
    if not json_path.exists():
        print(f"Erro: {json_path} não encontrado.")
        sys.exit(1)

    try:
        with open(json_path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception as e:
        print(f"Erro ao ler JSON: {e}")
        sys.exit(1)

    meta = data.get("meta", {})
    results = data.get("results", [])

    output = []
    output.append("# Baseline de Performance (Arquivos Sintéticos)")
    output.append("")

    is_proxy = meta.get("is_proxy", False)
    if is_proxy:
        output.append("> **Nota:** Esta execução foi marcada como **PROXY**. Os resultados podem não representar o baseline final de 2GB.")
        output.append("")

    output.append("Este relatório apresenta as medições de baseline para operações comuns na GUI utilizando arquivos Parquet sintéticos.")
    output.append("")
    output.append("## Ambiente")
    output.append(f"- **Data:** {meta.get('timestamp', 'N/A')}")
    output.append(f"- **SO:** {meta.get('os', 'N/A')}")
    output.append(f"- **CPU:** {meta.get('cpu', 'N/A')}")
    output.append(f"- **RAM:** {meta.get('ram_gb', 'N/A')} GB")
    output.append(f"- **Disco:** {meta.get('disk_type', 'N/A')}")
    output.append(f"- **Python:** {meta.get('python', 'N/A')}")
    output.append(f"- **Polars:** {meta.get('polars_version', 'N/A')}")
    output.append(f"- **DuckDB:** {meta.get('duckdb_version', 'N/A')}")
    output.append(f"- **Rounds:** {meta.get('rounds', 'N/A')}")
    output.append("")

    output.append("## Resultados das Medições")
    output.append("")
    output.append("| Tamanho (MB) | Operação | Backend | Mean (s) | P95 (s) | RSS Delta | CV% |")
    output.append("|---|---|---|---|---|---|---|")

    for r in results:
        p95_s = r.get("p95_s")
        p95_str = f"{p95_s:.3f}s" if p95_s is not None else "N/A"
        mean_s = f"{r['mean_s']:.3f}s"
        rss = f"{r['mean_rss_delta_mb']:+.1f} MB"
        cv = f"{r['cv_pct']}%"
        output.append(
            f"| {r['size_mb']} | {r['operation']} | {r['backend']} | {mean_s} | {p95_str} | {rss} | {cv} |"
        )

    output.append("")
    output.append("## Resumo por Tamanho")
    output.append("")

    sizes = sorted(list(set(r["size_mb"] for r in results)))
    for size in sizes:
        output.append(f"### Arquivo de {size} MB")
        output.append("| KPI | Meta | Valor (P95/Mean) | Resultado |")
        output.append("|---|---|---|---|")

        for r in [res for res in results if res["size_mb"] == size]:
            val = r.get("p95_s", r["mean_s"])
            val_str = f"{val:.3f}s"

            if r["operation"] == "ttfp":
                meta_val = "<= 5.0s"
                status = "[PASS]" if val <= 5.0 else "[FAIL]"
                output.append(f"| TTFP | {meta_val} | {val_str} | {status} |")
            elif r["operation"].startswith("page_"):
                # Simplificamos para nao poluir o resumo se houver muitas paginas
                if r["operation"] == "page_2":
                    meta_val = "<= 2.0s"
                    status = "[PASS]" if val <= 2.0 else "[FAIL]"
                    output.append(f"| Page Change (p2) | {meta_val} | {val_str} | {status} |")
            elif r["operation"] == "filter_contem" or r["operation"] == "filter_apply":
                meta_val = "<= 5.0s"
                status = "[PASS]" if val <= 5.0 else "[FAIL]"
                output.append(f"| Filter Apply | {meta_val} | {val_str} | {status} |")
            elif r["operation"] == "distinct":
                meta_val = "<= 1.0s"
                status = "[PASS]" if val <= 1.0 else "[FAIL]"
                output.append(f"| Distinct Values | {meta_val} | {val_str} | {status} |")
            elif r["operation"] == "export_50k":
                meta_val = "<= 2.0s"
                status = "[PASS]" if val <= 2.0 else "[FAIL]"
                output.append(f"| Export 50k rows | {meta_val} | {val_str} | {status} |")
        output.append("")

    Path("docs/baseline_performance.md").write_text("\n".join(output), encoding="utf-8")
    print("Relatório docs/baseline_performance.md atualizado.")


if __name__ == "__main__":
    format_report()
