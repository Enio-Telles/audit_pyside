import subprocess
import sys
import fnmatch
import os

# Lista de padroes proibidos que NUNCA devem ser commitados
FORBIDDEN_PATTERNS = [
    "PR_BODY.md",
    "full_harness_report.txt",
    "pr_description*",
    "pr_desc*",
    "coverage.xml",
    ".coverage",
    "htmlcov/*",
    "reports/diff/**",
    "scratch/**",
    "playground/**",
    "tmp/**",
    "src/**/tmp_*.py",
    "src/**/scratch_*.py",
    "traceback.txt",
    "all_issues.json",
    "issues.json",
]

def get_tracked_files():
    try:
        result = subprocess.run(
            ["git", "ls-files"],
            capture_output=True,
            text=True,
            check=True
        )
        return result.stdout.splitlines()
    except subprocess.CalledProcessError as e:
        print(f"Erro ao executar git ls-files: {e}")
        sys.exit(1)

def check_forbidden_files():
    tracked_files = get_tracked_files()
    found_forbidden = []

    for file_path in tracked_files:
        # Normalize path for matching (standardize separators)
        normalized_path = file_path.replace(os.sep, '/')

        for pattern in FORBIDDEN_PATTERNS:
            # Match against full path or just the filename
            if (fnmatch.fnmatch(normalized_path, pattern) or
                fnmatch.fnmatch(os.path.basename(normalized_path), pattern)):
                found_forbidden.append(file_path)
                break

    if found_forbidden:
        print("ERRO: Foram encontrados arquivos proibidos no repositorio:")
        for f in sorted(list(set(found_forbidden))):
            print(f"  - {f}")
        print("\nEsses arquivos sao temporarios ou artefatos gerados.")
        print("Remova-os do Git antes de prosseguir:")
        print("  git rm --cached <arquivo>")
        sys.exit(1)
    else:
        print("Sucesso: Nenhum arquivo proibido encontrado.")

if __name__ == "__main__":
    check_forbidden_files()
