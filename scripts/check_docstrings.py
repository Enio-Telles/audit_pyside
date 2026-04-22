#!/usr/bin/env python3
"""Simple docstring checker for the repository.

Usage: python scripts/check_docstrings.py

Exits with code 0 when no missing docstrings are found, otherwise 1.
"""
from __future__ import annotations

import ast
import sys
from pathlib import Path


def find_py_files(root: Path) -> list[Path]:
    exclude = {".git", "venv", "env", "__pycache__", ".venv"}
    files = []
    for p in root.rglob("*.py"):
        if any(part in exclude for part in p.parts):
            continue
        files.append(p)
    return files


def check_file(path: Path) -> list[str]:
    try:
        src = path.read_text(encoding="utf-8")
    except Exception:
        return [f"{path}: unreadable"]
    try:
        tree = ast.parse(src)
    except Exception:
        return [f"{path}: parse error"]
    missing = []
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            if ast.get_docstring(node) is None:
                lineno = getattr(node, "lineno", "?")
                kind = "class" if isinstance(node, ast.ClassDef) else "def"
                name = getattr(node, "name", "<anon>")
                missing.append(f"{path}:{lineno} {kind} {name}")
    return missing


def main() -> int:
    root = Path(".")
    files = find_py_files(root)
    total_missing = []
    for f in files:
        m = check_file(f)
        if m:
            total_missing.extend(m)
    print(f"Checked {len(files)} Python files. Missing docstrings: {len(total_missing)}")
    for line in total_missing[:500]:
        print(line)
    if total_missing:
        print("\nRun this script and incrementally address flagged items.\n")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
