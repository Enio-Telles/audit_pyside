from pathlib import Path
import polars as pl
import tempfile
import sys
sys.path.insert(0, str(Path('src').resolve()))
import importlib
mod = importlib.import_module('transformacao.movimentacao_estoque_pkg.co_sefin_class')

import datetime

with tempfile.TemporaryDirectory() as td:
    tmp = Path(td)
    dados_dir = tmp / 'dados'
    refs_dir = dados_dir / 'referencias'
    sefin_dir = refs_dir / 'CO_SEFIN'
    sefin_dir.mkdir(parents=True, exist_ok=True)
    # monkeypatch module globals
    mod.ROOT_DIR = tmp
    mod.DADOS_DIR = dados_dir
    mod.REFS_DIR = refs_dir
    cnpj = '12345678000199'
    # save produtos_agrupados
    pasta = dados_dir / 'CNPJ' / cnpj / 'analises' / 'produtos'
    pasta.mkdir(parents=True, exist_ok=True)
    pl.DataFrame([{'id_agrupado':'id_agr_1','co_sefin_padrao':'999'}]).write_parquet(pasta / f'produtos_agrupados_{cnpj}.parquet')
    # write sitafe_cest_ncm
    pl.DataFrame({'it_nu_cest':['2222'],'it_nu_ncm':['1111'],'it_co_sefin':['123']}).write_parquet(sefin_dir / 'sitafe_cest_ncm.parquet')
    # write sitafe_produto_sefin_aux
    pl.DataFrame({'it_co_sefin':['999','999','123'],'it_da_inicio':['20240101','20230101','20240101'],'it_da_final':[None,'20231231',None],'it_pc_interna':[17.0,12.0,5.0]}).write_parquet(sefin_dir / 'sitafe_produto_sefin_aux.parquet')
    df_mov = pl.DataFrame({'id_agrupado':['id_agr_1'],'ncm_padrao':['1111'],'cest_padrao':['2222'],'Dt_doc':[datetime.date(2025,1,15)],'Dt_e_s':[None]})
    print('INPUT df_mov:')
    print(df_mov)
    # Reproduce internal steps
    df_mov_lazy = df_mov.lazy()
    df_after = mod.gerar_co_sefin_final(df_mov_lazy)
    print('\nDF after gerar_co_sefin_final:')
    print(df_after.collect())
    df_agr = mod._carregar_co_sefin_padrao(cnpj)
    print('\nProdutos agrupados (co_sefin_padrao):')
    print(df_agr)
    df_joined = df_after.lazy().join(df_agr.lazy(), on='id_agrupado', how='left')
    print('\nDF after join with produtos_agrupados (schema):')
    print(df_joined.collect())
    res = mod.enriquecer_co_sefin_class(df_mov, cnpj)
    print('\nRESULT:')
    print(res)
