from pathlib import Path

p = Path("src/transformacao/movimentacao_estoque_pkg/movimentacao_estoque.py")
s = p.read_text(encoding="utf-8")
bal = 0
changes = []
for i, line in enumerate(s.splitlines(), 1):
    old = bal
    for ch in line:
        if ch == "(":
            bal += 1
        elif ch == ")":
            bal -= 1
    if bal != old:
        changes.append((i, bal, line.strip()))
for ln, b, txt in changes:
    print(f"line {ln}: bal={b} | {txt}")
print("final balance", bal)
