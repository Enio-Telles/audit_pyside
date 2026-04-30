"""
Servidor web leve para o frontend React do audit_pyside.

Serve os arquivos estaticos (audit_pyside_frontend.html + audit_react/)
e expoe endpoints REST que ligam ao pipeline Python existente:

    GET  /                             -> audit_pyside_frontend.html
    GET  /audit_react/*                -> arquivos estaticos
    GET  /api/status                   -> versao e saude
    GET  /api/cnpjs                    -> CNPJs com razao/uf/periodo do reg_0000
    GET  /api/parquets?cnpj=XXXXX      -> parquets do CNPJ
    GET  /api/dados?cnpj=XXXXX         -> produtos, conversao, estoque, logs
    POST /api/similaridade/ordenar     -> ordena blocos por similaridade

Uso:
    python app_react.py
    python app_react.py --port 8765 --host 127.0.0.1 --no-browser
"""
from __future__ import annotations

import argparse
import json
import mimetypes
import re
import sys
import threading
import urllib.parse
import webbrowser
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from socketserver import ThreadingMixIn

ROOT = Path(__file__).parent
SRC_DIR = ROOT / "src"
for _p in (SRC_DIR, SRC_DIR / "utilitarios"):
    if str(_p) not in sys.path:
        sys.path.insert(0, str(_p))

DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 8765

# ============================================================
# Handlers de API
# ============================================================

def _api_status() -> dict:
    return {"ok": True, "versao": "0.4.0", "servidor": "app_react"}


def _meta_cnpj(cnpj: str) -> dict:
    """Le razao, uf e periodo do parquet reg_0000 ou fallback no c100."""
    import polars as pl

    cnpj_dir = ROOT / "dados" / "CNPJ" / cnpj / "arquivos_parquet"
    meta: dict = {"razao": None, "uf": None, "periodo": None}

    # Tenta reg_0000 (EFD — tem nome/uf/dt_ini/dt_fin)
    reg0 = list(cnpj_dir.glob(f"reg_0000_{cnpj}.parquet"))
    if reg0:
        try:
            df = pl.read_parquet(reg0[0], columns=["nome", "uf", "dt_fin"])
            if len(df):
                row = df.sort("dt_fin", descending=True).to_dicts()[0]
                meta["razao"] = row.get("nome") or None
                meta["uf"] = row.get("uf") or None
                dt = row.get("dt_fin")
                if dt:
                    q = (dt.month - 1) // 3 + 1
                    meta["periodo"] = f"{dt.year}-Q{q}"
        except Exception:
            pass

    # Fallback: periodo via c100 (tem periodo_efd como string "YYYY/MM")
    if not meta["periodo"]:
        c100 = list(cnpj_dir.glob(f"c100_{cnpj}.parquet"))
        if c100:
            try:
                df2 = pl.read_parquet(c100[0], columns=["periodo_efd"])
                ult = df2["periodo_efd"].max()
                if ult:
                    ano, mes = str(ult).split("/")
                    q = (int(mes) - 1) // 3 + 1
                    meta["periodo"] = f"{ano}-Q{q}"
            except Exception:
                pass

    return meta


def _api_cnpjs() -> list[dict]:
    try:
        from interface_grafica.services.parquet_service import ParquetService
        from interface_grafica.services.registry_service import RegistryService

        svc = ParquetService(root=ROOT / "dados" / "CNPJ")
        reg = RegistryService(registry_file=ROOT / "workspace" / "app_state" / "cnpjs.json")
        cnpjs_fs: set[str] = set(svc.list_cnpjs())
        cnpjs_reg: dict[str, object] = {r.cnpj: r for r in reg.list_records()}
        todos = sorted(cnpjs_fs | set(cnpjs_reg))
        resultado = []
        for cnpj in todos:
            arquivos = svc.list_parquet_files(cnpj)
            rec = cnpjs_reg.get(cnpj)
            meta = _meta_cnpj(cnpj)
            resultado.append({
                "id":       cnpj,
                "razao":    meta["razao"],
                "uf":       meta["uf"],
                "periodo":  meta["periodo"],
                "last_run": rec.last_run_at if rec else None,
                "parquets": len(arquivos),
            })
        return resultado
    except Exception as exc:
        return [{"error": str(exc)}]


def _api_parquets(cnpj: str) -> list[dict]:
    if not cnpj:
        return []
    try:
        from interface_grafica.services.parquet_service import ParquetService

        svc = ParquetService(root=ROOT / "dados" / "CNPJ")
        arquivos = svc.list_parquet_files(cnpj)
        cnpj_dir = svc.cnpj_dir(cnpj)
        resultado = []
        for p in arquivos:
            stat = p.stat()
            rows: int | None = None
            try:
                import polars as pl

                rows = int(pl.scan_parquet(p).select(pl.len()).collect().item())
            except Exception:
                rows = None
            try:
                rel = str(p.relative_to(cnpj_dir)).replace("\\", "/")
            except ValueError:
                rel = p.name
            # grupo: subdirectory logo abaixo do dir do CNPJ, se existir
            partes = p.relative_to(cnpj_dir).parts if cnpj_dir in p.parents or p.is_relative_to(cnpj_dir) else ()
            grupo = partes[0] if len(partes) > 1 else None
            tam = (
                f"{stat.st_size / 1_000_000:.1f} MB"
                if stat.st_size > 1_000_000
                else f"{stat.st_size // 1024} KB"
            )
            resultado.append({
                "name": rel.removesuffix(".parquet"),
                "loc": str(p),
                "rows": rows,
                "size": tam,
                "group": grupo,
            })
        return resultado
    except Exception as exc:
        return [{"error": str(exc)}]


def _api_dados(cnpj: str) -> dict:
    """
    Carrega dados reais de um CNPJ a partir dos parquets de analise.
    Retorna: { produtos, stats, conversao, estoque, logs }
    """
    import polars as pl

    base = ROOT / "dados" / "CNPJ" / cnpj / "analises" / "produtos"
    resultado: dict = {}

    # ---- produtos_final → SIM_ROWS ----
    pf = base / f"produtos_final_{cnpj}.parquet"
    if pf.exists():
        cols_pf = ["id_descricao", "descr_padrao", "ncm_padrao",
                   "cest_padrao", "gtin_padrao", "unid_ref_sugerida"]
        df_pf = pl.read_parquet(pf, columns=cols_pf).head(2000)
        resultado["produtos"] = [
            {
                "id":     r["id_descricao"] or "",
                "bloco":  0,
                "desc":   r["descr_padrao"] or "",
                "ncm":    r["ncm_padrao"]   or "—",
                "cest":   r["cest_padrao"]  or "—",
                "gtin":   r["gtin_padrao"]  or "—",
                "un":     r["unid_ref_sugerida"] or "—",
                "camada": 0,
                "motivo": "—",
                "score":  0,
            }
            for r in df_pf.to_dicts()
        ]
        resultado["stats"] = {
            "n_linhas":    len(resultado["produtos"]),
            "n_blocos":    0,
            "por_camada":  {},
            "executou_ms": 0,
        }

    # ---- fatores_conversao → CONVERSAO_ROWS ----
    fc = base / f"fatores_conversao_{cnpj}.parquet"
    if fc.exists():
        cols_fc = ["id_agrupado", "id_produtos", "descr_padrao",
                   "unid", "unid_ref", "unid_ref_override",
                   "fator", "fator_override", "preco_medio"]
        df_fc = pl.read_parquet(fc, columns=cols_fc)
        resultado["conversao"] = [
            {
                "id_agr":   r["id_agrupado"] or "",
                "id_prod":  r["id_produtos"] or "",
                "desc":     r["descr_padrao"] or "",
                "lista":    r["descr_padrao"] or "",
                "unid":     r["unid"]     or "—",
                "unid_ref": r["unid_ref_override"] or r["unid_ref"] or "—",
                "fator":    r["fator_override"] if r["fator_override"] is not None else r["fator"],
                "preco":    r["preco_medio"],
            }
            for r in df_fc.to_dicts()
        ]

    # ---- mov_estoque → ESTOQUE_ROWS (primeiras 500 linhas) ----
    me = base / f"mov_estoque_{cnpj}.parquet"
    if me.exists():
        cols_me = ["ordem_operacoes", "Tipo_operacao", "fonte",
                   "Descr_item", "nsu", "Chv_nfe", "mod", "Ser", "num_nfe"]
        df_me = pl.read_parquet(me, columns=cols_me).head(500)
        resultado["estoque"] = [
            {
                "ord":  r["ordem_operacoes"],
                "tipo": r["Tipo_operacao"] or "",
                "fonte": r["fonte"] or "",
                "desc": r["Descr_item"] or "",
                "nsu":  r["nsu"]    or "",
                "chv":  r["Chv_nfe"] or "",
                "mod":  r["mod"]    or "",
                "ser":  r["Ser"]    or "",
                "nu":   r["num_nfe"] or "",
            }
            for r in df_me.to_dicts()
        ]

    # ---- logs via perf_events.jsonl ----
    log_file = ROOT / "logs" / "performance" / "perf_events.jsonl"
    if log_file.exists():
        linhas_raw = log_file.read_text(encoding="utf-8").splitlines()
        log_lines = []
        for linha in reversed(linhas_raw[-200:]):
            try:
                e = json.loads(linha)
                ts_full = e.get("timestamp", "")
                ts = ts_full[11:19] if len(ts_full) >= 19 else "—"
                status = e.get("status", "ok")
                lvl = "ERROR" if status == "error" else "INFO"
                evento = e.get("evento", "")
                src = evento.split(".")[0][:13]
                dur = e.get("duracao_s", 0)
                ctx = e.get("contexto", {})
                msg = f"{evento} · {dur:.3f}s"
                if ctx.get("linhas"):
                    msg += f" · {ctx['linhas']} linhas"
                elif ctx.get("total_rows"):
                    msg += f" · {ctx['total_rows']} linhas"
                log_lines.append({"ts": ts, "lvl": lvl, "src": src, "msg": msg})
            except Exception:
                pass
        resultado["logs"] = log_lines[:60]

    return resultado


def _api_similaridade_ordenar(body: bytes) -> dict:
    """
    Corpo JSON esperado:
      {
        "rows":       [{"id", "desc", "ncm", "cest", "gtin", "un", ...}],
        "metodo":     "particionamento" | "composto" | "apenas_descricao",
        "thresholds": {"camada_1": 50, "camada_2": 65, "camada_3": 80, "camada_5": 70},
        "opcoes":     {"camada_desc": bool, "canon": bool}
      }

    Resposta:
      {
        "rows":  [{"id", "desc", "ncm", "cest", "gtin", "un",
                   "bloco", "camada", "motivo", "score", ...}],
        "stats": {"n_linhas", "n_blocos", "por_camada", "executou_ms"}
      }
    """
    import time

    import polars as pl

    payload: dict = json.loads(body)
    rows: list[dict] = payload.get("rows", [])
    metodo: str = payload.get("metodo", "particionamento")
    thresholds: dict = payload.get("thresholds", {})
    opcoes: dict = payload.get("opcoes", {})

    if not rows:
        return {"rows": [], "stats": {"n_linhas": 0, "n_blocos": 0, "por_camada": {}, "executou_ms": 0}}

    # Frontend usa "desc"; backend resolve por aliases como "descr_padrao", "descricao" etc.
    # Renomeia "desc" -> "descr_padrao" para que o resolver encontre a coluna.
    rows_norm = [
        {("descr_padrao" if k == "desc" else k): v for k, v in r.items()}
        for r in rows
    ]
    df = pl.from_dicts(rows_norm, infer_schema_length=len(rows_norm))

    t0 = time.monotonic()

    if metodo == "composto":
        from interface_grafica.services.descricao_similarity_service import (
            ordenar_blocos_similaridade_descricao,
        )
        df_out = ordenar_blocos_similaridade_descricao(
            df,
            janela=4,
            limite_bloco=int(thresholds.get("camada_1", 82)),
            usar_ncm_cest=True,
        )
    elif metodo == "apenas_descricao":
        from interface_grafica.services.inverted_index_descricao import (
            ordenar_blocos_apenas_por_descricao,
        )
        df_out = ordenar_blocos_apenas_por_descricao(
            df,
            threshold=float(thresholds.get("camada_5", 70)) / 100,
        )
    else:
        from interface_grafica.services.particionamento_fiscal import (
            ordenar_blocos_por_particionamento_fiscal,
        )
        th: dict = {}
        for k_front, k_back in (
            ("camada_1", "camada_1"),
            ("camada_2", "camada_2"),
            ("camada_3", "camada_3"),
            ("camada_5", "camada_5"),
        ):
            if k_front in thresholds:
                th[k_back] = int(thresholds[k_front])
        df_out = ordenar_blocos_por_particionamento_fiscal(
            df,
            incluir_camada_so_descricao=bool(opcoes.get("camada_desc", False)),
            thresholds=th or None,
        )

    elapsed_ms = int((time.monotonic() - t0) * 1000)

    # Renomeia colunas de saida do servico para nomes do frontend
    _RENAME = {
        "descr_padrao": "desc",
        "sim_bloco":    "bloco",
        "sim_motivo":   "motivo",
        "sim_camada":   "camada",
        "sim_score":    "score",
    }
    rename_map = {k: v for k, v in _RENAME.items() if k in df_out.columns}
    if rename_map:
        df_out = df_out.rename(rename_map)

    # Remove colunas internas de debug (sim_* remanescentes e __sim_*)
    internas = [c for c in df_out.columns if c.startswith("sim_") or c.startswith("__sim_")]
    if internas:
        df_out = df_out.drop(internas)

    rows_out: list[dict] = df_out.to_dicts()

    # Estatisticas
    por_camada: dict[str, int] = {}
    if "camada" in df_out.columns:
        for c in range(6):
            n = int((df_out["camada"] == c).sum())
            if n:
                por_camada[str(c)] = n

    n_blocos = int(df_out["bloco"].n_unique()) if "bloco" in df_out.columns else 0

    return {
        "rows": rows_out,
        "stats": {
            "n_linhas": len(rows_out),
            "n_blocos": n_blocos,
            "por_camada": por_camada,
            "executou_ms": elapsed_ms,
        },
    }


def _json_body(body: bytes) -> dict:
    if not body:
        return {}
    payload = json.loads(body.decode("utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("Corpo JSON deve ser um objeto.")
    return payload


def _normalizar_cpf_cnpj(value: str) -> str:
    digits = re.sub(r"\D", "", value or "")
    if len(digits) not in {11, 14}:
        raise ValueError("Informe CPF com 11 digitos ou CNPJ com 14 digitos.")
    return digits


def _pipeline_response(result) -> dict:
    return {
        "ok": bool(result.ok),
        "cnpj": result.cnpj,
        "mensagens": result.mensagens[-50:],
        "arquivos_gerados": result.arquivos_gerados,
        "erros": result.erros,
        "tempos": result.tempos,
    }


def _api_extrair(body: bytes) -> dict:
    from interface_grafica.services.pipeline_funcoes_service import ServicoPipelineCompleto
    from interface_grafica.services.registry_service import RegistryService

    payload = _json_body(body)
    cnpj = _normalizar_cpf_cnpj(str(payload.get("cnpj") or ""))
    data_limite = payload.get("data_limite") or None
    service = ServicoPipelineCompleto()
    consultas = payload.get("consultas")
    if consultas is None:
        consultas = service.servico_extracao.listar_consultas()
    if not isinstance(consultas, list):
        raise ValueError("consultas deve ser uma lista.")
    result = service.executar_completo(cnpj, consultas, [], data_limite)
    if result.ok:
        RegistryService().upsert(result.cnpj, ran_now=True)
    return _pipeline_response(result)


def _api_processar(body: bytes) -> dict:
    from interface_grafica.services.pipeline_funcoes_service import (
        ServicoPipelineCompleto,
        TABELAS_DISPONIVEIS,
    )
    from interface_grafica.services.registry_service import RegistryService

    payload = _json_body(body)
    cnpj = _normalizar_cpf_cnpj(str(payload.get("cnpj") or ""))
    service = ServicoPipelineCompleto()
    tabelas = payload.get("tabelas")
    if tabelas is None:
        tabelas = [item["id"] for item in TABELAS_DISPONIVEIS]
    if not isinstance(tabelas, list):
        raise ValueError("tabelas deve ser uma lista.")
    result = service.executar_completo(cnpj, [], tabelas, None)
    if result.ok:
        RegistryService().upsert(result.cnpj, ran_now=True)
    return _pipeline_response(result)


# ============================================================
# Servidor HTTP
# ============================================================

class _ThreadedHTTPServer(ThreadingMixIn, HTTPServer):
    daemon_threads = True


class _Handler(BaseHTTPRequestHandler):

    def log_message(self, fmt: str, *args: object) -> None:
        # silencia log de acesso padrao; erros ainda chegam via log_error
        pass

    def log_error(self, fmt: str, *args: object) -> None:
        import sys as _sys
        print(f"[app_react] erro: {fmt % args}", file=_sys.stderr)

    # ---- utilitarios de resposta ----

    def _json(self, data: object, status: int = 200) -> None:
        body = json.dumps(data, default=str, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self._cors()
        self.end_headers()
        self.wfile.write(body)

    def _static(self, path: Path) -> None:
        mime, _ = mimetypes.guess_type(str(path))
        data = path.read_bytes()
        self.send_response(200)
        self.send_header("Content-Type", mime or "application/octet-stream")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _cors(self) -> None:
        self.send_header("Access-Control-Allow-Origin", "*")

    def _not_found(self) -> None:
        self.send_response(404)
        self.send_header("Content-Length", "0")
        self.end_headers()

    def _read_body(self) -> bytes:
        n = int(self.headers.get("Content-Length", 0))
        return self.rfile.read(n) if n else b""

    # ---- GET ----

    def do_GET(self) -> None:
        parsed = urllib.parse.urlparse(self.path)
        path = parsed.path
        params = urllib.parse.parse_qs(parsed.query)

        if path in ("/", "/audit_pyside_frontend.html"):
            self._static(ROOT / "audit_pyside_frontend.html")

        elif path.startswith("/audit_react/"):
            target = ROOT / path.lstrip("/")
            if target.is_file():
                self._static(target)
            else:
                self._not_found()

        elif path == "/api/status":
            try:
                self._json(_api_status())
            except Exception as exc:
                self._json({"error": str(exc)}, status=500)

        elif path == "/api/cnpjs":
            try:
                self._json(_api_cnpjs())
            except Exception as exc:
                self._json({"error": str(exc)}, status=500)

        elif path == "/api/parquets":
            cnpj = (params.get("cnpj") or [""])[0]
            try:
                self._json(_api_parquets(cnpj))
            except Exception as exc:
                self._json({"error": str(exc)}, status=500)

        elif path == "/api/dados":
            cnpj = (params.get("cnpj") or [""])[0]
            try:
                self._json(_api_dados(cnpj))
            except Exception as exc:
                self._json({"error": str(exc)}, status=500)

        else:
            self._not_found()

    # ---- POST ----

    def do_POST(self) -> None:
        parsed = urllib.parse.urlparse(self.path)
        path = parsed.path
        body = self._read_body()

        if path == "/api/similaridade/ordenar":
            try:
                self._json(_api_similaridade_ordenar(body))
            except Exception as exc:
                self._json({"error": str(exc)}, status=500)
        elif path == "/api/extrair":
            try:
                self._json(_api_extrair(body))
            except Exception as exc:
                self._json({"error": str(exc)}, status=500)
        elif path == "/api/processar":
            try:
                self._json(_api_processar(body))
            except Exception as exc:
                self._json({"error": str(exc)}, status=500)
        else:
            self._not_found()

    # ---- OPTIONS (preflight CORS) ----

    def do_OPTIONS(self) -> None:
        self.send_response(204)
        self._cors()
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.send_header("Content-Length", "0")
        self.end_headers()


# ============================================================
# Entrypoint
# ============================================================

def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Servidor React do audit_pyside")
    p.add_argument("--host", default=DEFAULT_HOST)
    p.add_argument("--port", type=int, default=DEFAULT_PORT)
    p.add_argument("--no-browser", action="store_true", help="Nao abrir o navegador automaticamente")
    return p.parse_args()


def main() -> None:
    args = _parse_args()
    url = f"http://{args.host}:{args.port}"

    httpd = _ThreadedHTTPServer((args.host, args.port), _Handler)

    print(f"[app_react] servindo em {url}")
    print(f"[app_react] Ctrl+C para encerrar")

    if not args.no_browser:
        threading.Timer(0.4, webbrowser.open, args=[url]).start()

    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\n[app_react] encerrado.")
    finally:
        httpd.server_close()


if __name__ == "__main__":
    main()
