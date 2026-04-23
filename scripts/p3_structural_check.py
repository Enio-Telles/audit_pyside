import ast
import json
import pathlib
import sys


target = sys.argv[1] if len(sys.argv) > 1 else "src/interface_grafica"
result = {}
for f in sorted(pathlib.Path(target).rglob("*.py")):
    source = f.read_text(encoding="utf-8-sig")
    tree = ast.parse(source)
    classes = [n.name for n in ast.walk(tree) if isinstance(n, ast.ClassDef)]
    functions = [
        n.name
        for n in ast.walk(tree)
        if isinstance(n, ast.FunctionDef | ast.AsyncFunctionDef)
    ]
    result[str(f)] = {
        "lines": len(source.splitlines()),
        "classes": classes,
        "functions_count": len(functions),
    }
print(json.dumps(result, indent=2))
