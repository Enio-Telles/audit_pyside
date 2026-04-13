"""
enriquecimento_fontes.py

Objetivo: Materializar as regras de rastreabilidade na pratica sem destruir origens.
Etapas:
1. Carrega base (NFe, NFCe, C170, Bloco H)
2. JOIN map_produto_agrupado (traz id_agrupado)
3. JOIN produtos_agrupados (traz descr_padrao, ncm_padrao)
4. JOIN fatores_conversao (calcula equivalencias de unidade_ref)
5. Salva Parquets Enriched (Camada Gold)
"""

import re
import sys
from pathlib import Path
from utilitarios.project_paths import PROJECT_ROOT
import polars as pl
from rich import print as rprint

ROOT_DIR = PROJECT_ROOT
SRC_DIR = ROOT_DIR / "src"
DADOS_DIR = ROOT_DIR / "dados"
CNPJ_ROOT = DADOS_DIR / "CNPJ"


try:
    from utilitarios.salvar_para_parquet import salvar_para_parquet
except ImportError as e:
    rprint(f"[red]Erro ao importar modulos utilitarios:[/red] {e}")
    sys.exit(1)


def gerar_enriquecimento(cnpj: str, pasta_cnpj: Path | None = None) -> bool:
    cnpj = re.sub(r"\D", "", cnpj)
    if pasta_cnpj is None:
        pasta_cnpj = CNPJ_ROOT / cnpj

    pasta_analises = pasta_cnpj / "analises" / "produtos"
    arq_agrup = pasta_analises / f"produtos_agrupados_{cnpj}.parquet"
    arq_map = pasta_analises / f"map_produto_agrupado_{cnpj}.parquet"
    arq_fator = pasta_analises / f"fatores_conversao_{cnpj}.parquet"
    
    if not (arq_agrup.exists() and arq_map.exists() and arq_fator.exists()):
        rprint("[red]Bases de MDM/Conversao nao encontradas.[/red]")
        return False
        
    df_agrup = pl.read_parquet(arq_agrup).select(["id_agrupado", "descr_padrao", "ncm_padrao", "cest_padrao"])
    df_map = pl.read_parquet(arq_map).rename({"chave_produto": "codigo_fonte"})
    df_fator = pl.read_parquet(arq_fator).select(["id_produtos", "unid", "unid_ref", "fator"]).rename({"id_produtos": "id_agrupado"})
    
    sucesso = True
    
    def enriquecer(df_bruto: pl.DataFrame, col_unid_original: str, col_qtd_original: str, col_valor_unitario: str = None) -> pl.DataFrame:
        df_join = (
            df_bruto
            .join(df_map, on="codigo_fonte", how="left")
            .join(df_agrup, on="id_agrupado", how="left")
            .join(df_fator, left_on=["id_agrupado", col_unid_original], right_on=["id_agrupado", "unid"], how="left")
        )
        
        # Default de fator eh 1.0 se nao encontrado
        df_join = df_join.with_columns(
            pl.col("fator").fill_null(1.0)
        )
        
        # Calcular qtd_padronizada e vuncom_padronizado
        df_join = df_join.with_columns([
            (pl.col(col_qtd_original).cast(pl.Float64) * pl.col("fator")).alias("qtd_padronizada"),
            (pl.col(col_valor_unitario).cast(pl.Float64) / pl.col("fator")).alias("vuncom_padronizado") if col_valor_unitario and col_valor_unitario in df_join.columns else pl.lit(None).alias("vuncom_padronizado")
        ])
        return df_join

    def processar_fonte(prefix: str, arq_origem: Path, col_ucom: str, col_qcom: str, col_vuncom: str):
        if not arq_origem.exists():
            return True
        rprint(f"[cyan]Enriquecendo {prefix}...[/cyan]")
        df_orig = pl.read_parquet(arq_origem)
        
        if "codigo_fonte" not in df_orig.columns:
            rprint(f"[yellow]Ignorando {prefix} - Sem coluna codigo_fonte (falta re-extracao SQL)[/yellow]")
            return True
            
        df_enr = enriquecer(df_orig, col_ucom, col_qcom, col_vuncom)
        return salvar_para_parquet(df_enr, pasta_analises, f"{prefix}_enriquecido_{cnpj}.parquet")
        
    arq_dir = pasta_cnpj / "arquivos_parquet"
    
    # NFe
    f_nfe = list(arq_dir.glob("NFe_*.parquet")) or list(pasta_cnpj.glob("NFe_*.parquet"))
    if f_nfe:
        sucesso &= processar_fonte("nfe", f_nfe[0], "prod_ucom", "prod_qcom", "prod_vuncom")
        
    # NFCe
    f_nfce = list(arq_dir.glob("NFCe_*.parquet")) or list(pasta_cnpj.glob("NFCe_*.parquet"))
    if f_nfce:
        sucesso &= processar_fonte("nfce", f_nfce[0], "prod_ucom", "prod_qcom", "prod_vuncom")
        
    # C170
    f_c170 = list(arq_dir.glob("c170*.parquet")) or list(pasta_cnpj.glob("c170*.parquet"))
    if f_c170:
        sucesso &= processar_fonte("c170", f_c170[0], "unid", "qtd", None) 
        
    # Bloco H
    f_blocoh = list(arq_dir.glob("bloco_h*.parquet")) or list(pasta_cnpj.glob("bloco_h*.parquet"))
    if f_blocoh:
        sucesso &= processar_fonte("bloco_h", f_blocoh[0], "unidade_medida", "quantidade", "valor_unitario")
        
    return sucesso

if __name__ == "__main__":
    if len(sys.argv) > 1:
        gerar_enriquecimento(sys.argv[1])
    else:
        c = input("CNPJ: ")
        gerar_enriquecimento(c)


