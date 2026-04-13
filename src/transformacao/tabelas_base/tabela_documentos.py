import polars as pl
from pathlib import Path
from utilitarios.project_paths import PROJECT_ROOT
import sys
import re
from rich import print as rprint

ROOT_DIR = PROJECT_ROOT
SRC_DIR = ROOT_DIR / "src"
DADOS_DIR = ROOT_DIR / "dados"
CNPJ_ROOT = DADOS_DIR / "CNPJ"


try:
    from utilitarios.salvar_para_parquet import salvar_para_parquet
except ImportError as e:
    rprint(f"[red]Erro ao importar mÃ³dulos utilitÃ¡rios em tabela_documentos:[/red] {e}")
    sys.exit(1)

def gerar_tabela_documentos(cnpj: str, pasta_cnpj: Path | None = None) -> bool:
    """
    Consolida cabeÃ§alhos de notas (C100, NFe, NFCe).
    SaÃ­da: tb_documentos_{cnpj}.parquet
    
    RecomendaÃ§Ã£o Auditoria: OtimizaÃ§Ã£o de tipos (Categorical) e estabilidade.
    """
    cnpj = re.sub(r"\D", "", cnpj)
    if pasta_cnpj is None:
        pasta_cnpj = CNPJ_ROOT / cnpj
    
    arq_dir = pasta_cnpj / "arquivos_parquet"
    if not arq_dir.exists():
        rprint(f"[red]Pasta de parquets brutos nÃ£o encontrada: {arq_dir}[/red]")
        return False

    rprint(f"[bold cyan]Consolidando documentos para CNPJ: {cnpj}[/bold cyan]")

    try:
        # 1. Carregar Fontes
        def _ler(prefix):
            paths = list(arq_dir.glob(f"{prefix}*.parquet"))
            if not paths: return pl.DataFrame()
            return pl.read_parquet(paths[0])

        df_c100 = _ler("c100")
        df_nfe = _ler("NFe")
        df_nfce = _ler("NFCe")

        fragmentos = []
        if not df_c100.is_empty(): fragmentos.append(df_c100.with_columns(pl.lit("C100").alias("origem")))
        if not df_nfe.is_empty(): fragmentos.append(df_nfe.with_columns(pl.lit("NFe").alias("origem")))
        if not df_nfce.is_empty(): fragmentos.append(df_nfce.with_columns(pl.lit("NFCe").alias("origem")))
        
        if not fragmentos:
            rprint("[yellow]Aviso: Nenhum documento bruto encontrado para consolidar.[/yellow]")
            return False
            
        df_final = pl.concat(fragmentos, how="diagonal_relaxed")
        
        # 2. OtimizaÃ§Ã£o de Performance (Audit)
        # Colunas de baixa cardinalidade -> Categorical
        cols_categoricas = ["origem", "situacao", "modelo", "serie", "ind_oper", "ind_emit", "cod_sit"]
        for col in cols_categoricas:
            if col in df_final.columns:
                df_final = df_final.with_columns(pl.col(col).cast(pl.Categorical))

        # 3. Salvar
        pasta_saida = pasta_cnpj / "analises" / "produtos"
        ok = salvar_para_parquet(df_final, pasta_saida, f"tb_documentos_{cnpj}.parquet")
        
        return ok

    except Exception as e:
        rprint(f"[red]Erro ao consolidar documentos para {cnpj}:[/red] {e}")
        return False

if __name__ == "__main__":
    if len(sys.argv) > 1:
        gerar_tabela_documentos(sys.argv[1])
    else:
        c = input("CNPJ: ")
        gerar_tabela_documentos(c)


