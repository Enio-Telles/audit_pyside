from pathlib import Path
p=Path('src/transformacao/movimentacao_estoque_pkg/movimentacao_estoque.py')
s=p.read_text(encoding='utf-8')
for i,l in enumerate(s.splitlines(),1):
    if 260 <= i <= 330:
        print(f'{i:04d}: {l}')
