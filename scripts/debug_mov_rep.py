from pathlib import Path
import sys
sys.path.insert(0,str(Path('src').resolve()))
import polars as pl
from transformacao.movimentacao_estoque_pkg.movimentacao_estoque import marcar_mov_rep_por_chave_item

df = pl.DataFrame({
    'Chv_nfe':[None,None],
    'num_doc':['DOC001','DOC001'],
    'Num_item':['1','1'],
    'Qtd':[10.0,10.0]
})
print('INPUT')
print(df)
res = marcar_mov_rep_por_chave_item(df)
print('OUTPUT')
print(res)
print('MOV_REP:', res['mov_rep'].to_list())
