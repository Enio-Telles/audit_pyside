"""
app_react.py — Lançador unificado: backend FastAPI + frontend React

Modos de uso:
    python app_react.py             # modo dev  (uvicorn :8000 + Vite :5173)
    python app_react.py --prod      # modo prod  (build React → serve tudo em :8000)
    python app_react.py --port 9000 # porta customizada (modo prod)
    python app_react.py --no-browser# não abre o navegador

Atalho de teclado: Ctrl+C encerra ambos os processos.
"""
from __future__ import annotations

import argparse
import os
import shutil
import signal
import subprocess
import sys
import time
import webbrowser
from pathlib import Path
from threading import Thread

# Garante que stdout use UTF-8 para evitar erros de encoding com logs do uvicorn
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")  # type: ignore[attr-defined]
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")  # type: ignore[attr-defined]


def _find_pnpm() -> str | None:
    """Localiza o pnpm quando ele estiver disponivel no ambiente."""
    return shutil.which("pnpm")

# ---------------------------------------------------------------------------
# Caminhos raiz
# ---------------------------------------------------------------------------
ROOT = Path(__file__).resolve().parent
BACKEND_DIR = ROOT / "backend"
FRONTEND_DIR = ROOT / "frontend"
DIST_DIR = FRONTEND_DIR / "dist"
SRC_DIR = ROOT / "src"

# ---------------------------------------------------------------------------
# Utilitários
# ---------------------------------------------------------------------------

def _print(msg: str, *, color: str = "") -> None:
    codes = {"green": "\033[92m", "yellow": "\033[93m", "red": "\033[91m", "cyan": "\033[96m", "reset": "\033[0m"}
    prefix = codes.get(color, "")
    reset = codes["reset"] if prefix else ""
    try:
        print(f"{prefix}{msg}{reset}", flush=True)
    except UnicodeEncodeError:
        safe = f"{prefix}{msg}{reset}".encode(sys.stdout.encoding or "utf-8", errors="replace").decode(sys.stdout.encoding or "utf-8")
        print(safe, flush=True)


def _find_npm() -> str:
    npm = shutil.which("npm")
    if not npm:
        _print("ERRO: npm não encontrado. Instale o Node.js.", color="red")
        sys.exit(1)
    return npm


def _obter_gerenciador_frontend() -> tuple[str, str]:
    """
    Define o gerenciador de pacotes do frontend.

    Priorizamos pnpm quando o lockfile existe porque os wrappers do Windows em
    node_modules/.bin dependem do layout gerado pelo gerenciador utilizado.
    """
    pnpm = _find_pnpm()
    if (FRONTEND_DIR / "pnpm-lock.yaml").exists() and pnpm:
        return "pnpm", pnpm
    return "npm", _find_npm()


def _montar_comando_frontend(executavel_gerenciador: str, script: str) -> list[str]:
    """Monta o comando do script sem alterar a interface publica do projeto."""
    if Path(executavel_gerenciador).stem.lower() == "pnpm":
        return [executavel_gerenciador, script]
    return [executavel_gerenciador, "run", script]


def _dependencias_frontend_estao_validas() -> bool:
    """
    Confere se o binario real do Vite existe na instalacao atual.

    Isso evita falso positivo quando node_modules foi reaproveitado apos mover
    a pasta do projeto e os wrappers de .bin ficaram apontando para caminhos
    absolutos antigos.
    """
    binario_vite = FRONTEND_DIR / "node_modules" / "vite" / "bin" / "vite.js"
    return binario_vite.exists()


def _garantir_dependencias_frontend() -> tuple[str, str]:
    """Garante dependencias utilizaveis antes de subir ou buildar o frontend."""
    nome_gerenciador, executavel_gerenciador = _obter_gerenciador_frontend()

    if _dependencias_frontend_estao_validas():
        return nome_gerenciador, executavel_gerenciador

    _print(
        f"Dependencias do frontend ausentes ou inconsistentes. Reinstalando com {nome_gerenciador}...",
        color="yellow",
    )
    subprocess.run([executavel_gerenciador, "install"], cwd=FRONTEND_DIR, check=True)

    if not _dependencias_frontend_estao_validas():
        _print(
            "ERRO: a reinstalacao terminou, mas o binario do Vite continua indisponivel.",
            color="red",
        )
        sys.exit(1)

    return nome_gerenciador, executavel_gerenciador


def _find_uvicorn() -> str:
    uv = shutil.which("uvicorn")
    if not uv:
        # tenta no mesmo Python
        uv = str(Path(sys.executable).parent / "uvicorn")
        if not Path(uv).exists():
            uv = str(Path(sys.executable).parent / "Scripts" / "uvicorn.exe")
    if not Path(uv).exists():
        _print("ERRO: uvicorn não encontrado. Execute: pip install uvicorn", color="red")
        sys.exit(1)
    return uv


def _wait_for_port(port: int, timeout: float = 30.0) -> bool:
    """Espera até que a porta esteja aceitando conexões."""
    import socket
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.5):
                return True
        except OSError:
            time.sleep(0.3)
    return False


def _stream_output(proc: subprocess.Popen, prefix: str, color: str) -> None:
    """Lê stdout do processo e imprime com prefixo colorido (thread daemon)."""
    assert proc.stdout
    for line in iter(proc.stdout.readline, b""):
        text = line.decode("utf-8", errors="replace").rstrip()
        if text:
            _print(f"[{prefix}] {text}", color=color)


# ---------------------------------------------------------------------------
# Build do frontend (modo produção)
# ---------------------------------------------------------------------------

def build_frontend() -> None:
    nome_gerenciador, executavel_gerenciador = _garantir_dependencias_frontend()
    _print("\n▶  Construindo frontend React...", color="cyan")

    # Instala dependências se node_modules ausente
    if not (FRONTEND_DIR / "node_modules").exists():
        _print("   Instalando dependências npm...", color="yellow")
        subprocess.run([npm, "install"], cwd=FRONTEND_DIR, check=True)

    result = subprocess.run(
        _montar_comando_frontend(executavel_gerenciador, "build"),
        cwd=FRONTEND_DIR,
    )
    if result.returncode != 0:
        _print("ERRO: build do frontend falhou.", color="red")
        sys.exit(1)
    _print("✔  Frontend construído com sucesso.", color="green")


def patch_backend_serve_static() -> None:
    """
    Adiciona montagem de arquivos estáticos ao main.py do backend
    apenas quando executado em modo produção — via monkey-patch em memória,
    sem alterar o arquivo em disco.
    """
    # Resolve caminho do dist
    dist = DIST_DIR.resolve()
    if not dist.exists():
        _print("ERRO: dist/ não encontrado. Execute com --prod para fazer o build.", color="red")
        sys.exit(1)

    # Adiciona src ao sys.path para os services Python
    if str(SRC_DIR) not in sys.path:
        sys.path.insert(0, str(SRC_DIR))

    # Adiciona backend ao sys.path para uvicorn encontrar 'main'
    if str(BACKEND_DIR) not in sys.path:
        sys.path.insert(0, str(BACKEND_DIR))

    # Importa o app e monta estáticos + fallback SPA
    from fastapi.staticfiles import StaticFiles
    from fastapi.responses import FileResponse
    from main import app  # type: ignore[import]

    app.mount("/assets", StaticFiles(directory=str(dist / "assets")), name="assets")

    @app.get("/{full_path:path}", include_in_schema=False)
    def spa_fallback(full_path: str):  # noqa: ARG001
        index = dist / "index.html"
        return FileResponse(str(index))

    _print("✔  Servindo frontend em /  (modo produção)", color="green")


# ---------------------------------------------------------------------------
# Modo DEV: backend + Vite em processos separados
# ---------------------------------------------------------------------------

def run_dev(backend_port: int, open_browser: bool) -> None:
    npm = _find_npm()
    uvicorn = _find_uvicorn()

    _print("\n═══════════════════════════════════════", color="cyan")
    _print("  Fiscal Parquet Analyzer  [MODO DEV]", color="cyan")
    _print("═══════════════════════════════════════", color="cyan")
    _print(f"  Backend  → http://localhost:{backend_port}", color="cyan")
    _print(f"  Frontend → http://localhost:5173", color="cyan")
    _print(f"  API Docs → http://localhost:{backend_port}/docs", color="cyan")
    _print("  Encerrar: Ctrl+C", color="cyan")
    _print("═══════════════════════════════════════\n", color="cyan")

    env = {**os.environ, "PYTHONUNBUFFERED": "1"}

    # Instala deps npm se ausente
    if not (FRONTEND_DIR / "node_modules").exists():
        _print("Instalando dependências npm...", color="yellow")
        subprocess.run([npm, "install"], cwd=FRONTEND_DIR, check=True)

    # Inicia backend
    proc_backend = subprocess.Popen(
        [uvicorn, "main:app", "--host", "0.0.0.0", "--port", str(backend_port), "--reload"],
        cwd=BACKEND_DIR,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        env={**env, "PYTHONPATH": str(SRC_DIR)},
    )
    Thread(target=_stream_output, args=(proc_backend, "API", "green"), daemon=True).start()

    # Inicia frontend Vite
    proc_frontend = subprocess.Popen(
        [npm, "run", "dev"],
        cwd=FRONTEND_DIR,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        env=env,
    )
    Thread(target=_stream_output, args=(proc_frontend, "UI", "yellow"), daemon=True).start()

    procs = [proc_backend, proc_frontend]

    def _shutdown(signum=None, frame=None) -> None:  # noqa: ARG001
        _print("\n\n⏹  Encerrando processos...", color="red")
        for p in procs:
            try:
                p.terminate()
            except Exception:
                pass
        time.sleep(1)
        for p in procs:
            try:
                if p.poll() is None:
                    p.kill()
            except Exception:
                pass
        sys.exit(0)

    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    # Aguarda o backend subir antes de abrir o navegador
    if open_browser:
        _print("Aguardando backend subir...", color="yellow")
        if _wait_for_port(backend_port):
            _print("Aguardando frontend subir...", color="yellow")
            if _wait_for_port(5173):
                _print("Abrindo navegador...", color="cyan")
                webbrowser.open("http://localhost:5173")
        else:
            _print("Backend demorou demais — abra o navegador manualmente.", color="yellow")

    # Aguarda os processos terminarem (ficará aqui até Ctrl+C)
    try:
        while all(p.poll() is None for p in procs):
            time.sleep(1)
    except KeyboardInterrupt:
        pass
    finally:
        _shutdown()


# ---------------------------------------------------------------------------
# Modo PROD: tudo em um único processo uvicorn na mesma porta
# ---------------------------------------------------------------------------

def run_prod(port: int, open_browser: bool) -> None:
    uvicorn_bin = _find_uvicorn()

    # Garante build atualizado
    if not DIST_DIR.exists() or not any(DIST_DIR.iterdir()):
        build_frontend()
    else:
        _print("dist/ já existe — usando build existente. Use --build para forçar.", color="yellow")

    _print("\n═══════════════════════════════════════", color="cyan")
    _print("  Fiscal Parquet Analyzer  [MODO PROD]", color="cyan")
    _print("═══════════════════════════════════════", color="cyan")
    _print(f"  App      → http://localhost:{port}", color="cyan")
    _print(f"  API Docs → http://localhost:{port}/docs", color="cyan")
    _print("  Encerrar: Ctrl+C", color="cyan")
    _print("═══════════════════════════════════════\n", color="cyan")

    # Inicia uvicorn apontando para app_prod:app (wrapper que monta estáticos)
    # Usamos a variável de ambiente FISCAL_PROD=1 para o main.py detectar e auto-montar
    env = {
        **os.environ,
        "PYTHONUNBUFFERED": "1",
        "PYTHONPATH": str(SRC_DIR),
        "FISCAL_PROD": "1",
        "FISCAL_DIST": str(DIST_DIR),
    }

    proc = subprocess.Popen(
        [uvicorn_bin, "main_prod:app", "--host", "0.0.0.0", "--port", str(port)],
        cwd=BACKEND_DIR,
        env=env,
    )

    def _shutdown(signum=None, frame=None) -> None:  # noqa: ARG001
        _print("\n\n⏹  Encerrando...", color="red")
        try:
            proc.terminate()
        except Exception:
            pass
        sys.exit(0)

    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    if open_browser:
        _print("Aguardando servidor subir...", color="yellow")
        if _wait_for_port(port):
            _print("Abrindo navegador...", color="cyan")
            webbrowser.open(f"http://localhost:{port}")
        else:
            _print("Servidor demorou — abra o navegador manualmente.", color="yellow")

    try:
        proc.wait()
    except KeyboardInterrupt:
        pass
    finally:
        _shutdown()


# ---------------------------------------------------------------------------
# Overrides de bootstrap do frontend
# ---------------------------------------------------------------------------

def build_frontend() -> None:
    """
    Reimplementado para validar dependencias de forma real antes do build.
    """
    nome_gerenciador, executavel_gerenciador = _garantir_dependencias_frontend()
    _print("\nConstruindo frontend React...", color="cyan")

    result = subprocess.run(
        _montar_comando_frontend(executavel_gerenciador, "build"),
        cwd=FRONTEND_DIR,
    )
    if result.returncode != 0:
        _print("ERRO: build do frontend falhou.", color="red")
        sys.exit(1)

    _print(f"Frontend construido com sucesso via {nome_gerenciador}.", color="green")


def run_dev(backend_port: int, open_browser: bool) -> None:
    """
    Reimplementado para reparar node_modules inconsistente antes de iniciar o Vite.
    """
    nome_gerenciador, executavel_gerenciador = _garantir_dependencias_frontend()
    uvicorn = _find_uvicorn()

    _print("\n=======================================", color="cyan")
    _print("  Fiscal Parquet Analyzer  [MODO DEV]", color="cyan")
    _print("=======================================", color="cyan")
    _print(f"  Backend  -> http://localhost:{backend_port}", color="cyan")
    _print("  Frontend -> http://localhost:5173", color="cyan")
    _print(f"  API Docs -> http://localhost:{backend_port}/docs", color="cyan")
    _print("  Encerrar: Ctrl+C", color="cyan")
    _print("=======================================\n", color="cyan")

    env = {**os.environ, "PYTHONUNBUFFERED": "1"}
    _print(f"Frontend validado com {nome_gerenciador}.", color="yellow")

    proc_backend = subprocess.Popen(
        [uvicorn, "main:app", "--host", "0.0.0.0", "--port", str(backend_port), "--reload"],
        cwd=BACKEND_DIR,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        env={**env, "PYTHONPATH": str(SRC_DIR)},
    )
    Thread(target=_stream_output, args=(proc_backend, "API", "green"), daemon=True).start()

    proc_frontend = subprocess.Popen(
        _montar_comando_frontend(executavel_gerenciador, "dev"),
        cwd=FRONTEND_DIR,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        env=env,
    )
    Thread(target=_stream_output, args=(proc_frontend, "UI", "yellow"), daemon=True).start()

    procs = [proc_backend, proc_frontend]

    def _shutdown(signum=None, frame=None) -> None:  # noqa: ARG001
        _print("\n\nEncerrando processos...", color="red")
        for processo in procs:
            try:
                processo.terminate()
            except Exception:
                pass
        time.sleep(1)
        for processo in procs:
            try:
                if processo.poll() is None:
                    processo.kill()
            except Exception:
                pass
        sys.exit(0)

    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    if open_browser:
        _print("Aguardando backend subir...", color="yellow")
        if _wait_for_port(backend_port):
            _print("Aguardando frontend subir...", color="yellow")
            if _wait_for_port(5173):
                _print("Abrindo navegador...", color="cyan")
                webbrowser.open("http://localhost:5173")
        else:
            _print("Backend demorou demais. Abra o navegador manualmente.", color="yellow")

    try:
        while all(processo.poll() is None for processo in procs):
            time.sleep(1)
    except KeyboardInterrupt:
        pass
    finally:
        _shutdown()


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Fiscal Parquet Analyzer — lançador web",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--prod",
        action="store_true",
        help="Modo produção: faz build do React e serve tudo via FastAPI (porta única)",
    )
    parser.add_argument(
        "--build",
        action="store_true",
        help="Forçar rebuild do frontend antes de iniciar (modo prod)",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=8000,
        help="Porta do backend / porta única (padrão: 8000)",
    )
    parser.add_argument(
        "--no-browser",
        action="store_true",
        help="Não abrir o navegador automaticamente",
    )
    args = parser.parse_args()

    open_browser = not args.no_browser

    if args.prod:
        if args.build and DIST_DIR.exists():
            shutil.rmtree(DIST_DIR)
        run_prod(port=args.port, open_browser=open_browser)
    else:
        run_dev(backend_port=args.port, open_browser=open_browser)


if __name__ == "__main__":
    main()
