import sys, subprocess, os, traceback
# Garantir que o src fique no path para imports locais
sys.path.insert(0, "src")
OUT = "tmp/pytest_result.txt"
with open(OUT, "w", encoding="utf-8") as out:
    try:
        env = os.environ.copy()
        env['PYTHONPATH'] = 'src'
        p = subprocess.run([sys.executable, "-m", "pytest", "-q"], stdout=out, stderr=subprocess.STDOUT, env=env, cwd=os.getcwd())
        out.write(f"\nEXIT:{p.returncode}\n")
    except Exception:
        traceback.print_exc(file=out)
        sys.exit(1)
