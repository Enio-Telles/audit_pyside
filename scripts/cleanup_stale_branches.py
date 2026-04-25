"""Script para listar e remover branches remotas obsoletas.

Modo padrão: --dry-run (apenas imprime comandos, não executa).
Modo real:   --execute (requer confirmação explícita).

Proteções hard-coded: 'main', 'master' e 'develop' nunca são tocadas.

Uso:
    uv run python scripts/cleanup_stale_branches.py
    uv run python scripts/cleanup_stale_branches.py --merged-only
    uv run python scripts/cleanup_stale_branches.py --execute
    uv run python scripts/cleanup_stale_branches.py --remote upstream --base main
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from dataclasses import dataclass, field
from datetime import datetime, timezone, UTC


# Branches que nunca devem ser deletadas, independente de qualquer argumento.
# Inclui 'develop' pois é convenção amplamente usada em outros repos que podem
# reutilizar este script — melhor proteger preventivamente do que ter de
# adicioná-la manualmente depois de um acidente.
PROTECTED = frozenset({"main", "master", "develop"})


@dataclass
class BranchInfo:
    """Informações sobre uma branch remota."""

    name: str
    short_name: str
    last_commit_date: datetime | None = None
    merged: bool = False
    age_days: int | None = field(default=None, init=False)

    def __post_init__(self) -> None:
        if self.last_commit_date is not None:
            now = datetime.now(tz=UTC)
            delta = now - self.last_commit_date
            self.age_days = delta.days


def _run(cmd: list[str], check: bool = True) -> str:
    """Executa um subcomando git e retorna stdout."""
    result = subprocess.run(cmd, capture_output=True, text=True, check=check)
    return result.stdout.strip()


def fetch_remote(remote: str) -> None:
    """Atualiza referências remotas e remove branches deletadas no servidor."""
    print(f"[fetch] git fetch {remote} --prune ...", flush=True)
    subprocess.run(["git", "fetch", remote, "--prune"], check=True)


def list_remote_branches(remote: str) -> list[str]:
    """Retorna os nomes completos (ex: origin/foo) de todas as branches remotas."""
    raw = _run(["git", "branch", "-r", "--no-color"])
    branches = []
    for line in raw.splitlines():
        line = line.strip()
        if not line or "->" in line:
            continue
        if line.startswith(f"{remote}/"):
            branches.append(line)
    return branches


def find_merged_branches(remote: str, base: str) -> set[str]:
    """Retorna conjunto de branches remotas já mergeadas em *base*."""
    raw = _run(["git", "branch", "-r", "--merged", f"{remote}/{base}", "--no-color"])
    merged: set[str] = set()
    for line in raw.splitlines():
        line = line.strip()
        if not line or "->" in line:
            continue
        merged.add(line)
    return merged


def get_branch_date(branch_ref: str) -> datetime | None:
    """Retorna a data do último commit de uma branch remota."""
    try:
        raw = _run(
            ["git", "log", "-1", "--format=%cd", "--date=iso-strict", branch_ref],
            check=False,
        )
        if raw:
            return datetime.fromisoformat(raw)
    except Exception:
        pass
    return None


def _check_base_reachable(remote: str, base: str, branch_ref: str) -> bool:
    """Verifica se ``remote/base`` é alcançável como ancestral de *branch_ref*.

    Usa ``git merge-base`` para confirmar que o histórico da base é acessível
    localmente. Retorna False (e não lança exceção) se o comando falhar —
    tipicamente quando o remote não foi fetchado recentemente.
    """
    try:
        _run(["git", "merge-base", f"{remote}/{base}", branch_ref], check=True)
        return True
    except subprocess.CalledProcessError:
        return False


def collect_branches(
    remote: str,
    base: str,
    merged_only: bool,
    min_age_days: int,
) -> list[BranchInfo]:
    """Coleta branches candidatas à remoção com metadados.

    Args:
        remote: Nome do remote (ex: 'origin').
        base: Branch de referência para detectar merges (ex: 'main').
        merged_only: Se True, inclui apenas branches já mergeadas.
        min_age_days: Número mínimo de dias desde o último commit para incluir a branch.

    Returns:
        Lista de BranchInfo ordenada por data do último commit (mais antigas primeiro).
    """
    all_branches = list_remote_branches(remote)
    merged = find_merged_branches(remote, base)

    prefix = f"{remote}/"
    results: list[BranchInfo] = []

    base_ref = f"{remote}/{base}"

    for full_name in all_branches:
        if full_name == base_ref:
            continue

        short_name = full_name.removeprefix(prefix)

        if short_name in PROTECTED:
            continue

        if not _check_base_reachable(remote, base, full_name):
            print(
                f"Aviso: branch {short_name!r} ignorada — base {remote}/{base} não alcançável.",
                file=sys.stderr,
            )
            continue

        is_merged = full_name in merged

        if merged_only and not is_merged:
            continue

        date = get_branch_date(full_name)
        info = BranchInfo(
            name=full_name,
            short_name=short_name,
            last_commit_date=date,
            merged=is_merged,
        )

        if min_age_days > 0 and (info.age_days is None or info.age_days < min_age_days):
            continue

        results.append(info)

    results.sort(key=lambda b: (b.last_commit_date or datetime.min.replace(tzinfo=UTC)))
    return results


def print_dry_run(branches: list[BranchInfo], remote: str) -> None:
    """Imprime comandos de deleção em modo dry-run sem executar nada."""
    if not branches:
        print("Nenhuma branch candidata encontrada.")
        return

    print(f"\n{'-' * 72}")
    print(f"  DRY-RUN: {len(branches)} branch(es) candidata(s) à remoção")
    print(f"{'-' * 72}")
    print(f"  {'Branch':<50} {'Último commit':<20} {'Merged'}")
    print(f"  {'-'*50} {'-'*20} {'-'*6}")

    for b in branches:
        date_str = b.last_commit_date.strftime("%Y-%m-%d") if b.last_commit_date else "desconhecida"
        merged_str = "sim" if b.merged else "não"
        print(f"  {b.short_name:<50} {date_str:<20} {merged_str}")

    print(f"\n{'-' * 72}")
    print("  Comandos para execução manual:")
    print()
    for b in branches:
        print(f"  git push {remote} --delete {b.short_name!r}")
    print()
    print("  Para executar automaticamente (reaplique os mesmos filtros com --execute):")
    print(f"{'-' * 72}\n")


def execute_deletions(branches: list[BranchInfo], remote: str) -> int:
    """Deleta as branches remotas após confirmação.

    Args:
        branches: Branches a deletar.
        remote: Nome do remote.

    Returns:
        Número de branches deletadas com sucesso.
    """
    if not branches:
        print("Nenhuma branch a deletar.")
        return 0

    print(f"\nATENÇÃO: {len(branches)} branch(es) serão DELETADAS do remote '{remote}'.")
    print("Branches a deletar:")
    for b in branches:
        print(f"  - {b.short_name}")

    confirm = input("\nConfirma a deleção? Digite 'sim' para continuar: ").strip().lower()
    if confirm != "sim":
        print("Operação cancelada.")
        return 0

    deleted = 0
    for b in branches:
        try:
            print(f"  Deletando {b.short_name} ... ", end="", flush=True)
            subprocess.run(
                ["git", "push", remote, "--delete", b.short_name],
                check=True,
                capture_output=True,
            )
            print("OK")
            deleted += 1
        except subprocess.CalledProcessError as exc:
            print(f"ERRO: {exc.stderr.decode().strip()}")

    print(f"\n{deleted}/{len(branches)} branch(es) deletadas.")
    return deleted


def build_parser() -> argparse.ArgumentParser:
    """Constrói o parser de argumentos CLI."""
    p = argparse.ArgumentParser(
        description="Lista/remove branches remotas obsoletas (dry-run por padrão).",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    p.add_argument("--remote", default="origin", help="Nome do remote (default: origin)")
    p.add_argument("--base", default="main", help="Branch de referência (default: main)")
    p.add_argument(
        "--merged-only",
        action="store_true",
        help="Incluir apenas branches já mergeadas em --base",
    )
    p.add_argument(
        "--min-age-days",
        type=int,
        default=0,
        metavar="DAYS",
        help="Incluir apenas branches com último commit há pelo menos N dias (default: 0 = todos)",
    )
    p.add_argument(
        "--no-fetch",
        action="store_true",
        help="Não executar git fetch antes de listar",
    )
    p.add_argument(
        "--execute",
        action="store_true",
        help="Executar deleções (requer confirmação interativa). Sem esta flag: dry-run.",
    )
    return p


def main() -> int:
    """Ponto de entrada principal do script."""
    parser = build_parser()
    args = parser.parse_args()

    if not args.no_fetch:
        try:
            fetch_remote(args.remote)
        except subprocess.CalledProcessError:
            if args.execute:
                print(
                    "Erro: git fetch falhou em modo --execute. "
                    "Use --no-fetch para prosseguir com referências locais.",
                    file=sys.stderr,
                )
                return 1
            print("Aviso: git fetch falhou. Continuando com referências locais.", file=sys.stderr)

    branches = collect_branches(
        remote=args.remote,
        base=args.base,
        merged_only=args.merged_only,
        min_age_days=args.min_age_days,
    )

    if args.execute:
        execute_deletions(branches, remote=args.remote)
    else:
        print_dry_run(branches, remote=args.remote)

    return 0


if __name__ == "__main__":
    sys.exit(main())
