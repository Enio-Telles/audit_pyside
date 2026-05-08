import json, sys
from pathlib import Path

def format_report():
    json_path = Path("docs/baseline_performance.json")
    if not json_path.exists():
        print(f"Erro: {json_path} não encontrado."); sys.exit(1)
    data = json.loads(json_path.read_text(encoding="utf-8"))
    meta, results = data.get("meta", {}), data.get("results", [])
    output = ["# Baseline de Performance (Arquivos Sintéticos)", ""]
    if meta.get("is_proxy"):
        output.append("> **Nota:** Esta execução foi marcada como **PROXY**.\n")
    output.append("Este relatório apresenta medições de baseline para operações comuns na GUI (fixtures conforme política D4).\n")
    output.append("## Ambiente")
    for k, v in [("Data", "timestamp"), ("SO", "os"), ("CPU", "cpu"), ("RAM", "ram_gb"), ("Python", "python"), ("Polars", "polars_version"), ("DuckDB", "duckdb_version"), ("PyArrow", "pyarrow_version"), ("Rounds", "rounds")]:
        output.append(f"- **{k}:** {meta.get(v, 'N/A')}{' GB' if k=='RAM' else ''}")
    output.append("\n## Resultados das Medições\n")
    output.append("| Tamanho (MB) | Operação | Backend | Mean (s) | P95 (s) | RSS Delta | CV% |")
    output.append("|---|---|---|---|---|---|---|")
    for r in results:
        p95 = r.get("p95_s"); p95_s = f"{p95:.3f}s" if p95 is not None else "N/A"
        output.append(f"| {r['size_mb']} | {r['operation']} | {r['backend']} | {r['mean_s']:.3f}s | {p95_s} | {r['mean_rss_delta_mb']:+.1f} MB | {r['cv_pct']}% |")
    output.append("\n## Resumo por Tamanho\n")
    sizes = sorted(list(set(r["size_mb"] for r in results)))
    for size in sizes:
        output.append(f"### Arquivo de {size} MB")
        output.append("| KPI | Meta | Valor (P95/Mean) | Resultado |")
        output.append("|---|---|---|---|")
        for r in [res for res in results if res["size_mb"] == size]:
            p95_v = r.get("p95_s"); val = p95_v if p95_v is not None else r["mean_s"]
            m_map = {"ttfp": ("TTFP", 5.0), "page_2": ("Page Change (p2)", 2.0), "filter_apply": ("Filter Apply", 5.0), "distinct": ("Distinct Values", 1.0), "export_50k": ("Export 50k rows", 2.0)}
            if r["operation"] in m_map:
                name, limit = m_map[r["operation"]]
                status = "[PASS]" if val <= limit else "[FAIL]"
                output.append(f"| {name} | <= {limit}s | {val:.3f}s | {status} |")
        output.append("")
    Path("docs/baseline_performance.md").write_text("\n".join(output), encoding="utf-8")
    print("Relatório docs/baseline_performance.md atualizado.")

if __name__ == "__main__": format_report()
