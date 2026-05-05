import json
from pathlib import Path


def format_report():
    json_path = Path("docs/baseline_performance.json")
    if not json_path.exists():
        print("Erro: docs/baseline_performance.json não encontrado.")
        return

    with open(json_path, "r") as f:
        data = json.load(f)

    results = data.get("results", [])

    print("# Baseline de Performance (Arquivos Sintéticos)")
    print()
    print(
        "Este relatório apresenta as medições de baseline para operações comuns na GUI utilizando diferentes tamanhos de arquivos Parquet sintéticos."
    )
    print()
    print("## Ambiente")
    print(f"- Data: {data['meta'].get('timestamp', 'N/A')}")
    print(f"- Python: {data['meta'].get('python', 'N/A')}")
    print()
    print("## Resultados das Medições")
    print()
    print("| Tamanho (MB) | Operação | Backend | P95 Time (s) | RSS Delta (MB) |")
    print("|---|---|---|---|---|")

    for r in results:
        print(
            f"| {r['size_mb']} | {r['operation']} | {r['backend']} | {r['p95_s']:.3f}s | {r['mean_rss_delta_mb']:+.1f} MB |"
        )

    print()
    print("## Resumo por Tamanho")
    print()

    sizes = sorted(list(set(r["size_mb"] for r in results)))
    for size in sizes:
        print(f"### Arquivo de {size} MB")
        print("| KPI | Meta | P95 Medido | Resultado |")
        print("|---|---|---|---|")

        for r in [res for res in results if res["size_mb"] == size]:
            meta = ""
            status = ""
            if r["operation"] == "ttfp":
                meta = "<= 5.0s"
                status = "[PASS]" if r["p95_s"] <= 5.0 else "[FAIL]"
                print(f"| TTFP | {meta} | {r['p95_s']:.3f}s | {status} |")
            elif r["operation"] == "page_change":
                meta = "<= 2.0s"
                status = "[PASS]" if r["p95_s"] <= 2.0 else "[FAIL]"
                print(f"| Page Change | {meta} | {r['p95_s']:.3f}s | {status} |")
            elif r["operation"] == "filter_apply":
                meta = "<= 5.0s"
                status = "[PASS]" if r["p95_s"] <= 5.0 else "[FAIL]"
                print(f"| Filter Apply | {meta} | {r['p95_s']:.3f}s | {status} |")
            elif r["operation"] == "export_50k":
                meta = "N/A"
                status = "-"
                print(f"| Export 50k rows | {meta} | {r['p95_s']:.3f}s | {status} |")
        print()


if __name__ == "__main__":
    format_report()
