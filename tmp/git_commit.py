import subprocess
import os
import traceback

OUT = "tmp/git_result.txt"
with open(OUT, "w", encoding="utf-8") as out:
    try:
        cmds = [
            ["git", "status", "--porcelain"],
            ["git", "rev-parse", "--abbrev-ref", "HEAD"],
            [
                "git",
                "add",
                "src/transformacao/rastreabilidade_produtos/fatores_conversao.py",
            ],
            [
                "git",
                "commit",
                "-m",
                "refactor(rastreabilidade): select first non-empty inner list without explode in fatores_conversao",
            ],
            ["git", "push", "origin", "HEAD"],
        ]
        env = os.environ.copy()
        for c in cmds:
            out.write("CMD: " + " ".join(c) + "\n")
            p = subprocess.run(
                c,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                env=env,
                cwd=os.getcwd(),
                text=True,
            )
            out.write(p.stdout + "\n")
            if p.returncode != 0 and c[0] == "git" and c[1] == "commit":
                out.write("Commit may have failed or no changes to commit\n")
        out.write("DONE\n")
    except Exception:
        traceback.print_exc(file=out)
