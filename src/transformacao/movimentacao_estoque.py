import sys
import re
from pathlib import Path
import polars as pl
from rich import print as rprint

ROOT_DIR = Path(r"c:\funcoes - Copia")
SRC_DIR = ROOT_DIR / "src"
UTILITARIOS_DIR = SRC_DIR / "utilitarios"
DADOS_DIR = ROOT_DIR / "dados"
CNPJ_ROOT = DADOS_DIR / "CNPJ"

if str(UTILITARIOS_DIR) not in sys.path:
    sys.path.insert(0, str(UTILITARIOS_DIR))

# Ensure src is in sys.path to import from transformacao.auxiliares
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

try:
    from salvar_para_parquet import salvar_para_parquet
    from co_sefin_class import enriquecer_co_sefin_class
    from text import remove_accents
    from transformacao.auxiliares.logs import log_exception
except ImportError as e:
    rprint(f"[red]Erro ao importar modulos:[/red] {e}")
    sys.exit(1)


def _norm(text: str | None) -> str:
    if text is None:
        return ""
    return re.sub(r"\s+", " ", (remove_accents(text) or "").upper().strip())


def _normalizar_descricao_expr(col: str) -> pl.Expr:
    return (
        pl.col(col)
        .cast(pl.Utf8, strict=False)
        .map_elements(_norm, return_dtype=pl.String)
        .alias("__descricao_normalizada__")
    )


def _detectar_coluna_descricao(df: pl.DataFrame, fonte: str) -> str | None:
    candidatos = {
        "c170": ["descr_item", "descricao", "prod_xprod"],
        "bloco_h": ["descricao_produto", "descr_item", "descricao", "prod_xprod"],
        "nfe": ["prod_xprod", "descricao", "descr_item"],
        "nfce": ["prod_xprod", "descricao", "descr_item"],
    }
    for col in candidatos.get(fonte, []):
        if col in df.columns:
            return col
    return None


def _detectar_coluna_unidade(df: pl.DataFrame, fonte: str) -> str | None:
    candidatos = {
        "c170": ["unid"],
        "bloco_h": ["unidade_medida", "unidade_media", "unid", "unidade"],
        "nfe": ["prod_ucom"],
        "nfce": ["prod_ucom"],
    }
    for col in candidatos.get(fonte, []):
        if col in df.columns:
            return col
    return None

def _parse_expression(expr_str: str, col_alias: str) -> pl.Expr:
    """Traduz as strings do map_estoque em expressoes do Polars."""
    if not expr_str or str(expr_str).strip() == "" or expr_str == "(vazio)":
        return pl.lit(None).alias(col_alias)
    
    expr_str = str(expr_str).strip()

    # Literal string
    if expr_str.startswith('"') and expr_str.endswith('"'):
        return pl.lit(expr_str.strip('"')).alias(col_alias)

    # NCM/CEST cleanup
    if expr_str in ["cod_ncm", "prod_ncm", "cest", "prod_cest"]:
        return pl.col(expr_str).cast(pl.String).str.replace_all(r"\D", "").alias(col_alias)

    # Complex: Cst
    if expr_str == "icms_orig & icms_cst ou icms_csosn":
        return pl.when(pl.col("icms_cst").is_not_null())\
                 .then(pl.concat_str([pl.col("icms_orig"), pl.col("icms_cst")], separator=""))\
                 .otherwise(pl.concat_str([pl.col("icms_orig"), pl.col("icms_csosn")], separator=""))\
                 .alias(col_alias)

    # Complex: Cod_barra 
    if expr_str == "prod_ceantrib ou caso for nulo -> prod_cean":
        return pl.coalesce(["prod_ceantrib", "prod_cean"]).alias(col_alias)

    # Complex: Valores matemáticos (Vl_item em C170 ou Nfe)
    if expr_str == "vl_item-vl_desc":
        return (pl.col("vl_item").cast(pl.Float64) - pl.col("vl_desc").cast(pl.Float64).fill_null(0)).alias(col_alias)
        
    if expr_str == "prod_vprod+prod_vfrete+prod_vseg+prod_voutro-prod_vdesc":
        return (
            pl.col("prod_vprod").cast(pl.Float64).fill_null(0) +
            pl.col("prod_vfrete").cast(pl.Float64).fill_null(0) +
            pl.col("prod_vseg").cast(pl.Float64).fill_null(0) +
            pl.col("prod_voutro").cast(pl.Float64).fill_null(0) -
            pl.col("prod_vdesc").cast(pl.Float64).fill_null(0)
        ).alias(col_alias)
        
    # Extração via Chave
    if expr_str == "correspondência com chave NF":
        if col_alias == "mod":
            return pl.col("chv_nfe").str.slice(20, 2).alias(col_alias)
        elif col_alias == "co_uf_emit":
            return pl.col("chv_nfe").str.slice(0, 2).alias(col_alias)
        elif col_alias == "co_uf_dest":
            return pl.lit(None).alias(col_alias) # Nao é fácil saber o dest so pela chave para c170
        return pl.lit(None).alias(col_alias)
        
    if expr_str == "\"gerado\" ou \"registro\" (se está no bloco_h)":
        return pl.lit("registro").alias(col_alias)

    # Fallback to column name if it exists... 
    # To prevent missing column error in polars, we select dynamically but wait, polars is strict.
    return pl.col(expr_str).alias(col_alias)


def _padronizar_tipo_operacao_expr(col: str = "Tipo_operacao") -> pl.Expr:
    valor = (
        pl.col(col)
        .cast(pl.Utf8, strict=False)
        .fill_null("")
        .str.strip_chars()
        .str.to_uppercase()
    )
    return (
        pl.when(valor == "INVENTARIO")
        .then(pl.lit("inventario"))
        .when(
            (valor == "0")
            | (valor == "0 - ENTRADA")
            | (valor == "ENTRADA")
            | valor.str.contains("ENTRADA", literal=True)
        )
        .then(pl.lit("1 - ENTRADA"))
        .when(
            (valor == "1")
            | (valor == "1 - SAIDA")
            | (valor == "2 - SAIDAS")
            | (valor == "SAIDA")
            | (valor == "SAIDAS")
            | valor.str.contains("SAIDA", literal=True)
        )
        .then(pl.lit("2 - SAIDAS"))
        .otherwise(pl.col(col).cast(pl.Utf8, strict=False))
        .alias(col)
    )


def _gerar_eventos_estoque(df_mov: pl.DataFrame) -> pl.DataFrame:
    if df_mov.is_empty() or "id_agrupado" not in df_mov.columns:
        return df_mov

    dt_doc_dtype = df_mov.schema.get("Dt_doc", pl.Datetime)
    dt_es_dtype = df_mov.schema.get("Dt_e_s", pl.Datetime)

    df_base = df_mov.with_columns(
        [
            pl.coalesce(
                [
                    pl.col("Dt_e_s").cast(pl.Date, strict=False),
                    pl.col("Dt_doc").cast(pl.Date, strict=False),
                ]
            ).alias("__data_ref__"),
            pl.col("Tipo_operacao").cast(pl.Utf8, strict=False).fill_null("").alias("__tipo_op__"),
        ]
    )

    # Estoque vindo do bloco H vira sempre ESTOQUE FINAL existente.
    df_exist_final = (
        df_base
        .filter(pl.col("__tipo_op__") == "inventario")
        .with_columns(
            [
                pl.lit("3 - ESTOQUE FINAL").alias("Tipo_operacao"),
                pl.col("__data_ref__").cast(dt_doc_dtype, strict=False).alias("Dt_doc"),
                pl.col("__data_ref__").cast(dt_es_dtype, strict=False).alias("Dt_e_s"),
            ]
        )
    )

    rprint("[cyan]Ajustando eventos de estoque...[/cyan]")

    produtos_unicos = (
        df_base
        .filter(pl.col("id_agrupado").is_not_null())
        .select(
            [
                "id_agrupado",
                "ncm_padrao",
                "cest_padrao",
                "descr_padrao",
                "Cod_item",
                "Cod_barra",
                "Ncm",
                "Cest",
                "Tipo_item",
                "Descr_item",
                "Cfop",
                "co_sefin_agr",
                "unid_ref",
                "fator",
            ]
        )
        .unique(subset=["id_agrupado"])
    )

    anos_ativos = (
        df_base
        .filter(
            pl.col("id_agrupado").is_not_null()
            & pl.col("__data_ref__").is_not_null()
            & (pl.col("__tipo_op__") != "inventario")
        )
        .with_columns(pl.col("__data_ref__").dt.year().alias("__ano__"))
        .select(["id_agrupado", "__ano__"])
        .unique()
    )

    movimentos_31_12 = (
        df_base
        .filter(
            pl.col("id_agrupado").is_not_null() 
            & pl.col("__data_ref__").is_not_null()
            & (pl.col("__tipo_op__") == "inventario")
        )
        .with_columns(
            [
                pl.col("__data_ref__").dt.year().alias("__ano__"),
                pl.col("__data_ref__").dt.month().alias("__mes__"),
                pl.col("__data_ref__").dt.day().alias("__dia__"),
            ]
        )
        .filter((pl.col("__mes__") == 12) & (pl.col("__dia__") == 31))
        .select(["id_agrupado", "__ano__"])
        .unique()
    )

    pares_sem_31_12 = anos_ativos.join(movimentos_31_12, on=["id_agrupado", "__ano__"], how="anti")

    df_gerado_final = pl.DataFrame()
    if pares_sem_31_12.height > 0:
        df_gerado_final = (
            pares_sem_31_12
            .join(produtos_unicos, on="id_agrupado", how="left")
            .with_columns(
                [
                    pl.concat_str(
                        [
                            pl.col("__ano__").cast(pl.Utf8),
                            pl.lit("-12-31"),
                        ]
                    )
                    .str.strptime(pl.Date, "%Y-%m-%d", strict=False)
                    .alias("__data_final__"),
                    pl.lit("3 - ESTOQUE FINAL gerado").alias("Tipo_operacao"),
                    pl.lit(0).cast(pl.Float64).alias("Qtd"),
                    pl.lit(0).cast(pl.Float64).alias("Vl_item"),
                    pl.lit(None).alias("Unid"),
                    pl.lit("gerado").alias("Ser"),
                ]
            )
            .with_columns(
                [
                    pl.col("__data_final__").cast(dt_doc_dtype, strict=False).alias("Dt_doc"),
                    pl.col("__data_final__").cast(dt_es_dtype, strict=False).alias("Dt_e_s"),
                    pl.col("__data_final__").alias("__data_ref__"),
                    pl.lit("inventario").alias("__tipo_op__"),
                ]
            )
        )

        for c in df_base.columns:
            if c not in df_gerado_final.columns:
                df_gerado_final = df_gerado_final.with_columns(pl.lit(None).alias(c))
        df_gerado_final = df_gerado_final.select(df_base.columns)

    df_finais = pl.concat(
        [
            df_exist_final.select(df_base.columns),
            df_gerado_final.select(df_base.columns) if not df_gerado_final.is_empty() else pl.DataFrame(schema=df_base.schema),
        ],
        how="vertical_relaxed",
    )

    df_iniciais = pl.DataFrame(schema=df_base.schema)
    if not df_finais.is_empty():
        df_iniciais = (
            df_finais
            .with_columns(
                [
                    (
                        pl.col("__data_ref__").cast(pl.Date, strict=False) + pl.duration(days=1)
                    ).alias("__data_inicial__"),
                ]
            )
            .with_columns(
                [
                    pl.when(pl.col("Tipo_operacao") == "3 - ESTOQUE FINAL")
                    .then(pl.lit("0 - ESTOQUE INICIAL"))
                    .otherwise(pl.lit("0 - ESTOQUE INICIAL gerado"))
                    .alias("Tipo_operacao"),
                    pl.col("__data_inicial__").cast(dt_doc_dtype, strict=False).alias("Dt_doc"),
                    pl.col("__data_inicial__").cast(dt_es_dtype, strict=False).alias("Dt_e_s"),
                ]
            )
        )

    iniciais_deriv_01_01 = (
        df_iniciais
        .with_columns(
            [
                pl.col("Dt_e_s").dt.year().alias("__ano__"),
                pl.col("Dt_e_s").dt.month().alias("__mes__"),
                pl.col("Dt_e_s").dt.day().alias("__dia__"),
            ]
        )
        .filter((pl.col("__mes__") == 1) & (pl.col("__dia__") == 1))
        .select(["id_agrupado", "__ano__"])
        .unique()
    )
    
    inv_01_01_base = (
        df_base
        .filter(
            pl.col("id_agrupado").is_not_null() 
            & pl.col("__data_ref__").is_not_null()
            & (pl.col("__tipo_op__") == "inventario")
        )
        .with_columns(
            [
                pl.col("__data_ref__").dt.year().alias("__ano__"),
                pl.col("__data_ref__").dt.month().alias("__mes__"),
                pl.col("__data_ref__").dt.day().alias("__dia__"),
            ]
        )
        .filter((pl.col("__mes__") == 1) & (pl.col("__dia__") == 1))
        .select(["id_agrupado", "__ano__"])
        .unique()
    )
    
    tem_01_01 = pl.concat([iniciais_deriv_01_01, inv_01_01_base]).unique()
    pares_sem_01_01 = anos_ativos.join(tem_01_01, on=["id_agrupado", "__ano__"], how="anti")
    
    if pares_sem_01_01.height > 0:
        df_gerado_inicial = (
            pares_sem_01_01
            .join(produtos_unicos, on="id_agrupado", how="left")
            .with_columns(
                [
                    pl.concat_str([pl.col("__ano__").cast(pl.Utf8), pl.lit("-01-01")])
                    .str.strptime(pl.Date, "%Y-%m-%d", strict=False)
                    .alias("__data_inicial__"),
                    pl.lit("0 - ESTOQUE INICIAL gerado").alias("Tipo_operacao"),
                    pl.lit(0).cast(pl.Float64).alias("Qtd"),
                    pl.lit(0).cast(pl.Float64).alias("Vl_item"),
                    pl.lit(None).alias("Unid"),
                    pl.lit("gerado").alias("Ser"),
                ]
            )
            .with_columns(
                [
                    pl.col("__data_inicial__").cast(dt_doc_dtype, strict=False).alias("Dt_doc"),
                    pl.col("__data_inicial__").cast(dt_es_dtype, strict=False).alias("Dt_e_s"),
                    pl.col("__data_inicial__").alias("__data_ref__"),
                    pl.lit("inventario").alias("__tipo_op__"),
                ]
            )
        )
        for c in df_base.columns:
            if c not in df_gerado_inicial.columns:
                df_gerado_inicial = df_gerado_inicial.with_columns(pl.lit(None).alias(c))
        df_iniciais = pl.concat([df_iniciais.select(df_base.columns), df_gerado_inicial.select(df_base.columns)], how="vertical_relaxed")

    df_sem_inventario = df_base.filter(pl.col("__tipo_op__") != "inventario")
    df_result = pl.concat(
        [
            df_sem_inventario.select(df_base.columns),
            df_finais.select(df_base.columns) if not df_finais.is_empty() else pl.DataFrame(schema=df_base.schema),
            df_iniciais.select(df_base.columns) if not df_iniciais.is_empty() else pl.DataFrame(schema=df_base.schema),
        ],
        how="vertical_relaxed",
    )

    return df_result.drop(["__data_ref__", "__tipo_op__"], strict=False)


def _valor_bool(value) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    texto = str(value).strip().upper()
    return texto in {"1", "TRUE", "T", "S", "SIM", "Y", "YES"}


def _calcular_saldo_estoque_anual(df: pl.DataFrame) -> pl.DataFrame:
    if df.is_empty():
        return df

    registros = df.to_dicts()
    saldo_qtd = 0.0
    saldo_valor = 0.0
    custo_medio = 0.0

    saldos = []
    entradas_desacob = []
    custos = []

    for row in registros:
        tipo = str(row.get("Tipo_operacao") or "")
        q_conv = float(row.get("q_conv") or 0.0)
        q_sinal = float(row.get("__q_conv_sinal__") or 0.0)
        preco_item = float(row.get("preco_item") or 0.0)
        qtd_decl_final = float(row.get("__qtd_decl_final_audit__") or 0.0)

        entr_desac = 0.0

        if tipo.startswith("0 - ESTOQUE INICIAL") or tipo == "1 - ENTRADA":
            if q_sinal > 0:
                saldo_qtd += q_sinal
                saldo_valor += preco_item
                custo_medio = (saldo_valor / saldo_qtd) if saldo_qtd > 0 else 0.0

        elif tipo == "2 - SAIDAS":
            if q_conv > 0:
                saldo_qtd += q_sinal
                if saldo_qtd < 0:
                    entr_desac = abs(saldo_qtd)
                    saldo_qtd = 0.0
                    saldo_valor = 0.0
                    custo_medio = 0.0
                else:
                    saldo_valor -= q_conv * custo_medio
                    if saldo_qtd <= 0:
                        saldo_qtd = 0.0
                        saldo_valor = 0.0
                        custo_medio = 0.0
                    else:
                        saldo_valor = max(saldo_valor, 0.0)
                        custo_medio = saldo_valor / saldo_qtd

        elif tipo.startswith("3 - ESTOQUE FINAL"):
            if qtd_decl_final > saldo_qtd:
                entr_desac = qtd_decl_final - saldo_qtd

        saldos.append(round(saldo_qtd, 6))
        entradas_desacob.append(round(entr_desac, 6))
        custos.append(round(custo_medio, 6))

    return df.with_columns(
        [
            pl.Series("saldo_estoque_anual", saldos),
            pl.Series("entr_desac_anual", entradas_desacob),
            pl.Series("custo_medio_anual", custos),
        ]
    )


def _carregar_flags_cfop() -> pl.DataFrame:
    arq_cfop = DADOS_DIR / "referencias" / "referencias" / "cfop" / "cfop.parquet"
    arq_cfop_bi = DADOS_DIR / "referencias" / "referencias" / "cfop" / "cfop_bi.parquet"

    df_cfop = pl.DataFrame(schema={"Cfop": pl.Utf8})
    if arq_cfop.exists():
        df_cfop_raw = pl.read_parquet(arq_cfop)
        exprs = [
            pl.col("co_cfop").cast(pl.Utf8, strict=False).str.replace_all(r"\D", "").alias("Cfop"),
        ]
        for col in ["excluir_estoque", "dev_simples"]:
            if col in df_cfop_raw.columns:
                exprs.append(pl.col(col).alias(col))
            else:
                exprs.append(pl.lit(None).alias(col))
        df_cfop = (
            df_cfop_raw
            .select(exprs)
            .filter(pl.col("Cfop").is_not_null() & (pl.col("Cfop") != ""))
            .unique(subset=["Cfop"], keep="first")
        )

    df_cfop_bi = pl.DataFrame(schema={"Cfop": pl.Utf8})
    if arq_cfop_bi.exists():
        df_cfop_bi_raw = pl.read_parquet(arq_cfop_bi)
        exprs = [
            pl.col("co_cfop").cast(pl.Utf8, strict=False).str.replace_all(r"\D", "").alias("Cfop"),
        ]
        for col in ["dev_venda", "dev_compra", "dev_ent_simples"]:
            if col in df_cfop_bi_raw.columns:
                exprs.append(pl.col(col).alias(col))
            else:
                exprs.append(pl.lit(None).alias(col))
        df_cfop_bi = (
            df_cfop_bi_raw
            .select(exprs)
            .filter(pl.col("Cfop").is_not_null() & (pl.col("Cfop") != ""))
            .unique(subset=["Cfop"], keep="first")
        )

    if df_cfop.is_empty() and df_cfop_bi.is_empty():
        return pl.DataFrame(
            schema={
                "Cfop": pl.Utf8,
                "excluir_estoque": pl.Boolean,
                "dev_simples": pl.Boolean,
                "dev_venda": pl.Utf8,
                "dev_compra": pl.Utf8,
                "dev_ent_simples": pl.Utf8,
            }
        )

    if df_cfop.is_empty():
        return df_cfop_bi
    if df_cfop_bi.is_empty():
        return df_cfop
    return df_cfop.join(df_cfop_bi, on="Cfop", how="full", coalesce=True)


def gerar_movimentacao_estoque(cnpj: str, pasta_cnpj: Path | None = None) -> bool:
    cnpj = re.sub(r"\D", "", cnpj)
    if pasta_cnpj is None:
        pasta_cnpj = CNPJ_ROOT / cnpj

    pasta_analises = pasta_cnpj / "analises" / "produtos"
    pasta_brutos = pasta_cnpj / "arquivos_parquet"
    if not pasta_analises.exists():
        rprint(f"[red]Pasta de analises nao encontrada para o CNPJ: {cnpj}[/red]")
        return False

    arq_prod_final = pasta_analises / f"produtos_final_{cnpj}.parquet"
    arq_fatores = pasta_analises / f"fatores_conversao_{cnpj}.parquet"
    if not arq_prod_final.exists():
        rprint(f"[red]Arquivo necessario nao encontrado:[/red] {arq_prod_final}")
        return False
    if not arq_fatores.exists():
        rprint(f"[red]Arquivo necessario nao encontrado:[/red] {arq_fatores}")
        return False

    # Carregar o map_estoque.json (ou mapeamento hardcoded)
    map_json = ROOT_DIR / "map_estoque.json"
    import json
    if not map_json.exists():
        rprint("[red]Arquivo map_estoque.json nao encontrado![/red]")
        return False
    with open(map_json, 'r', encoding='utf-8') as f:
        mapeamento = json.load(f)

    rprint(f"\n[bold cyan]Gerando movimentacao_estoque para CNPJ: {cnpj}[/bold cyan]")

    df_prod_final = (
        pl.read_parquet(arq_prod_final)
        .select(
            [
                "id_agrupado",
                "descricao_normalizada",
                "descr_padrao",
                "ncm_padrao",
                "cest_padrao",
                "descricao_final",
                "co_sefin_final",
                "unid_ref_sugerida",
            ]
        )
        .unique(subset=["descricao_normalizada"])
    )

    df_fatores = (
        pl.read_parquet(arq_fatores)
        .select(["id_agrupado", "unid", "unid_ref", "fator"])
        .rename({"unid": "__unid_fator__"})
        .unique(subset=["id_agrupado", "__unid_fator__"])
    )
    df_flags_cfop = _carregar_flags_cfop()

    df_ref_por_chave = pl.DataFrame(
        schema={
            "chv_nfe": pl.Utf8,
            "nsu_ref": pl.Int64,
            "finnfe_ref": pl.Utf8,
            "infprot_cstat_ref": pl.Utf8,
            "co_uf_emit_ref": pl.Utf8,
            "co_uf_dest_ref": pl.Utf8,
        }
    )
    for prefixo_nsu in ("nfe_agr", "nfce_agr", "nfe", "nfce"):
        arq_nsu = pasta_brutos / f"{prefixo_nsu}_{cnpj}.parquet"
        if not arq_nsu.exists():
            continue
        try:
            df_nsu_bruto = pl.read_parquet(arq_nsu)
            df_tmp_nsu = (
                df_nsu_bruto.select(
                    [
                        pl.col("chave_acesso").cast(pl.Utf8, strict=False).alias("chv_nfe"),
                        (pl.col("nsu").cast(pl.Int64, strict=False) if "nsu" in df_nsu_bruto.columns else pl.lit(None, pl.Int64)).alias("nsu_ref"),
                        (pl.col("co_finnfe").cast(pl.Utf8, strict=False) if "co_finnfe" in df_nsu_bruto.columns else pl.lit(None, pl.Utf8)).alias("finnfe_ref"),
                        (pl.col("infprot_cstat").cast(pl.Utf8, strict=False) if "infprot_cstat" in df_nsu_bruto.columns else pl.lit(None, pl.Utf8)).alias("infprot_cstat_ref"),
                        (pl.col("co_uf_emit").cast(pl.Utf8, strict=False) if "co_uf_emit" in df_nsu_bruto.columns else pl.lit(None, pl.Utf8)).alias("co_uf_emit_ref"),
                        (pl.col("co_uf_dest").cast(pl.Utf8, strict=False) if "co_uf_dest" in df_nsu_bruto.columns else pl.lit(None, pl.Utf8)).alias("co_uf_dest_ref"),
                    ]
                )
                .filter(pl.col("chv_nfe").is_not_null())
                .unique(subset=["chv_nfe"], keep="first")
            )
            if df_ref_por_chave.is_empty():
                df_ref_por_chave = df_tmp_nsu
            else:
                df_ref_por_chave = pl.concat([df_ref_por_chave, df_tmp_nsu], how="vertical_relaxed").unique(subset=["chv_nfe"], keep="first")
        except Exception:
            continue

    df_parts = []

    def _resolver_arquivo_origem(prefix_sys: str) -> Path | None:
        candidatos = [
            pasta_brutos / f"{prefix_sys}_agr_{cnpj}.parquet",
            pasta_analises / f"{prefix_sys}_agr_{cnpj}.parquet",
            pasta_brutos / f"{prefix_sys}_{cnpj}.parquet",
            pasta_cnpj / f"{prefix_sys}_{cnpj}.parquet",
            pasta_analises / f"{prefix_sys}_enriquecido_{cnpj}.parquet",
            pasta_brutos / f"{prefix_sys}_produtos_{cnpj}.parquet",
            pasta_analises / f"{prefix_sys}_produtos_{cnpj}.parquet",
        ]
        for arquivo in candidatos:
            if arquivo.exists():
                return arquivo
        return None

    def _process_source(prefix_sys: str, key_map: str):
        arquivo = _resolver_arquivo_origem(prefix_sys)
        if arquivo is None:
            return
            
        df_raw = pl.read_parquet(arquivo)
        col_desc = _detectar_coluna_descricao(df_raw, prefix_sys)
        col_unid = _detectar_coluna_unidade(df_raw, prefix_sys)

        if col_desc and "id_agrupado" not in df_raw.columns:
            df_raw = (
                df_raw
                .with_columns(_normalizar_descricao_expr(col_desc))
                .join(
                    df_prod_final.rename({"descricao_normalizada": "__descricao_normalizada__"}),
                    on="__descricao_normalizada__",
                    how="left",
                )
                .drop("__descricao_normalizada__")
            )

        if col_unid and ("fator" not in df_raw.columns or "unid_ref" not in df_raw.columns):
            df_raw = (
                df_raw
                .with_columns(pl.col(col_unid).cast(pl.String, strict=False).str.strip_chars().alias("__unid_fator__"))
                .join(df_fatores, on=["id_agrupado", "__unid_fator__"], how="left")
                .drop("__unid_fator__")
            )

        if "descricao_final" in df_raw.columns and "descr_padrao" not in df_raw.columns:
            df_raw = df_raw.with_columns(pl.col("descricao_final").alias("descr_padrao"))
        if "co_sefin_final" in df_raw.columns and "co_sefin_agr" not in df_raw.columns:
            df_raw = df_raw.with_columns(pl.col("co_sefin_final").alias("co_sefin_agr"))
        if prefix_sys == "c170" and "chv_nfe" in df_raw.columns and not df_ref_por_chave.is_empty():
            nsu_atual = pl.col("nsu").cast(pl.Int64, strict=False) if "nsu" in df_raw.columns else pl.lit(None, pl.Int64)
            finnfe_atual = pl.col("finnfe").cast(pl.Utf8, strict=False) if "finnfe" in df_raw.columns else pl.lit(None, pl.Utf8)
            infprot_cstat_atual = pl.col("infprot_cstat").cast(pl.Utf8, strict=False) if "infprot_cstat" in df_raw.columns else pl.lit(None, pl.Utf8)
            co_uf_emit_atual = pl.col("co_uf_emit").cast(pl.Utf8, strict=False) if "co_uf_emit" in df_raw.columns else pl.lit(None, pl.Utf8)
            co_uf_dest_atual = pl.col("co_uf_dest").cast(pl.Utf8, strict=False) if "co_uf_dest" in df_raw.columns else pl.lit(None, pl.Utf8)
            df_raw = (
                df_raw
                .with_columns(pl.col("chv_nfe").cast(pl.Utf8, strict=False).alias("chv_nfe"))
                .join(df_ref_por_chave, on="chv_nfe", how="left")
                .with_columns(
                    [
                        pl.coalesce([pl.col("nsu_ref"), nsu_atual]).alias("nsu"),
                        pl.coalesce([pl.col("finnfe_ref"), finnfe_atual]).alias("finnfe"),
                        pl.coalesce([pl.col("infprot_cstat_ref"), infprot_cstat_atual]).alias("infprot_cstat"),
                        pl.coalesce(
                            [
                                pl.col("co_uf_emit_ref"),
                                co_uf_emit_atual,
                                pl.col("chv_nfe").cast(pl.Utf8, strict=False).str.slice(0, 2),
                            ]
                        ).alias("co_uf_emit"),
                        pl.coalesce([pl.col("co_uf_dest_ref"), co_uf_dest_atual]).alias("co_uf_dest"),
                    ]
                )
                .drop(["nsu_ref", "finnfe_ref", "infprot_cstat_ref", "co_uf_emit_ref", "co_uf_dest_ref"], strict=False)
            )

        # Select apenas os que vao mapear pra evitar excesso, mas as expressoes referenciam colunas q devem existir!
        exprs = []
        for m in mapeamento:
            target = m["Campo/tabela"]
            orig = m[key_map]
            
            # Pra previnir erros de coluna que nao existe no raw:
            try:
                # build expression
                e = _parse_expression(orig, target)
                
                # valida se as dependencias de coluna da expressao existem no df
                # se "col("x") is missing, we put null"
                # um try no select isolado descobre
                # Para producao, uma funcao check columns é melhor, faremos um fallback basico
                
                exprs.append(e)
            except Exception as ex:
                exprs.append(pl.lit(None).alias(target))
                
        # Polars: try selecting. Se alguma coluna referenciada falhar, ele crasheia.
        # Entao adicionamos as colunas q não existem como nulas antes.
        cols_required = set()
        # Uma heuristic rapida para descobrir dependencias: vars() de orig
        for m in mapeamento:
            v = str(m[key_map])
            if v and v not in ["(vazio)", "correspondência com chave NF", "icms_orig & icms_cst ou icms_csosn", "prod_ceantrib ou caso for nulo -> prod_cean", "vl_item-vl_desc", "prod_vprod+prod_vfrete+prod_vseg+prod_voutro-prod_vdesc", "\"gerado\" ou \"registro\" (se está no bloco_h)"]:
                if not v.startswith('"'):
                    cols_required.add(v)
        
        # for extra rules 
        extra = ["vl_item", "vl_desc", "prod_vprod", "prod_vfrete", "prod_vseg", "prod_voutro", "prod_vdesc", "icms_cst", "icms_orig", "icms_csosn", "prod_cean", "prod_ceantrib", "chv_nfe"]
        cols_required.update(extra)
        
        for c in cols_required:
            if c not in df_raw.columns:
                df_raw = df_raw.with_columns(pl.lit(None).alias(c))

        df_selecionado = df_raw.select(exprs)
        
        # Alguns enriquecidos ja tem ncm_padrao, cest_padrao, descr_padrao pelo id_agrupado!
        # Se existem, preservamos, senao add null (C170 e Bloco H e NFEs enriquecidos ja possuem!)
        # O map na imagem e task falavam pra puxar campos, ja que estao no enriquecido
        cols_extras_manter = ["id_agrupado", "ncm_padrao", "cest_padrao", "descr_padrao", "co_sefin_agr", "unid_ref", "fator"]
        for c in cols_extras_manter:
            if c in df_raw.columns:
                df_selecionado = df_selecionado.with_columns(df_raw[c])
            else:
                df_selecionado = df_selecionado.with_columns(pl.lit(None).alias(c))

        if prefix_sys == "c170":
            for c in ["finnfe", "infprot_cstat", "co_uf_emit", "co_uf_dest"]:
                if c in df_raw.columns:
                    df_selecionado = df_selecionado.with_columns(df_raw[c])
                else:
                    df_selecionado = df_selecionado.with_columns(pl.lit(None).alias(c))

        df_parts.append(df_selecionado)

    # Sources mapeadas
    _process_source("c170", "C170")
    _process_source("bloco_h", "Bloco_h")
    _process_source("nfe", "Nfe")
    _process_source("nfce", "Nfce")
    
    if not df_parts:
        rprint("[yellow]Nenhuma tabela de origem (enriquecida) foi encontrada.[/yellow]")
        return False
        
    df_mov = pl.concat(df_parts, how="diagonal_relaxed")
    if not df_ref_por_chave.is_empty() and "Chv_nfe" in df_mov.columns:
        nsu_atual = pl.col("nsu").cast(pl.Int64, strict=False) if "nsu" in df_mov.columns else pl.lit(None, pl.Int64)
        finnfe_atual = pl.col("finnfe").cast(pl.Utf8, strict=False) if "finnfe" in df_mov.columns else pl.lit(None, pl.Utf8)
        infprot_cstat_atual = pl.col("infprot_cstat").cast(pl.Utf8, strict=False) if "infprot_cstat" in df_mov.columns else pl.lit(None, pl.Utf8)
        co_uf_emit_atual = pl.col("co_uf_emit").cast(pl.Utf8, strict=False) if "co_uf_emit" in df_mov.columns else pl.lit(None, pl.Utf8)
        co_uf_dest_atual = pl.col("co_uf_dest").cast(pl.Utf8, strict=False) if "co_uf_dest" in df_mov.columns else pl.lit(None, pl.Utf8)
        df_mov = (
            df_mov
            .with_columns(pl.col("Chv_nfe").cast(pl.Utf8, strict=False).alias("Chv_nfe"))
            .join(
                df_ref_por_chave.rename({"chv_nfe": "Chv_nfe"}),
                on="Chv_nfe",
                how="left",
            )
            .with_columns(
                [
                    pl.coalesce([pl.col("nsu_ref"), nsu_atual]).alias("nsu"),
                    pl.coalesce([pl.col("finnfe_ref"), finnfe_atual]).alias("finnfe"),
                    pl.coalesce([pl.col("infprot_cstat_ref"), infprot_cstat_atual]).alias("infprot_cstat"),
                    pl.coalesce(
                        [
                            pl.col("co_uf_emit_ref"),
                            co_uf_emit_atual,
                            pl.col("Chv_nfe").cast(pl.Utf8, strict=False).str.slice(0, 2),
                        ]
                    ).alias("co_uf_emit"),
                    pl.coalesce([pl.col("co_uf_dest_ref"), co_uf_dest_atual]).alias("co_uf_dest"),
                ]
            )
            .drop(["nsu_ref", "finnfe_ref", "infprot_cstat_ref", "co_uf_emit_ref", "co_uf_dest_ref"], strict=False)
        )
    df_mov = df_mov.with_columns(_padronizar_tipo_operacao_expr("Tipo_operacao"))
    
    # 2. Ajustar eventos de estoque final/inicial
    df_mov = _gerar_eventos_estoque(df_mov)

    # 3. Enriquecer com campos da co_sefin_class
    rprint("[cyan]Enriquecendo campos co_sefin...[/cyan]")
    df_final = enriquecer_co_sefin_class(df_mov, cnpj)
    if not df_flags_cfop.is_empty() and "Cfop" in df_final.columns:
        df_final = (
            df_final
            .with_columns(pl.col("Cfop").cast(pl.Utf8, strict=False).str.replace_all(r"\D", "").alias("Cfop"))
            .join(df_flags_cfop, on="Cfop", how="left")
        )
    else:
        for col in ["excluir_estoque", "dev_simples", "dev_venda", "dev_compra", "dev_ent_simples"]:
            if col not in df_final.columns:
                df_final = df_final.with_columns(pl.lit(None).alias(col))
    
    # Ordenacao semantica:
    # - por id_agrupado
    # - por data do evento (preferindo Dt_e_s e depois Dt_doc)
    # - ESTOQUE INICIAL no comeco do dia
    # - ENTRADAS/SAIDAS no fluxo normal do dia
    # - ESTOQUE FINAL no fim do dia
    # - nsu como desempate natural das notas
    df_final = (
        df_final
        .with_columns(
            [
                pl.coalesce(
                    [
                        pl.col("Dt_e_s").cast(pl.Date, strict=False),
                        pl.col("Dt_doc").cast(pl.Date, strict=False),
                    ]
                ).alias("__data_ord__"),
                pl.col("nsu").cast(pl.Int64, strict=False).alias("__nsu_ord__"),
                pl.when(pl.col("Tipo_operacao").cast(pl.Utf8, strict=False).str.starts_with("0 - ESTOQUE INICIAL"))
                .then(pl.lit(0))
                .when(pl.col("Tipo_operacao") == "1 - ENTRADA")
                .then(pl.lit(1))
                .when(pl.col("Tipo_operacao") == "2 - SAIDAS")
                .then(pl.lit(2))
                .when(pl.col("Tipo_operacao").cast(pl.Utf8, strict=False).str.starts_with("3 - ESTOQUE FINAL"))
                .then(pl.lit(3))
                .otherwise(pl.lit(9))
                .alias("__ord_tipo__"),
            ]
        )
        .sort(
            ["id_agrupado", "__data_ord__", "__ord_tipo__", "__nsu_ord__", "Dt_doc", "Dt_e_s"],
            descending=[False, False, False, False, False, False],
            nulls_last=True,
        )
        .with_row_index("ordem_operacoes", offset=1)
        .drop(["__data_ord__", "__nsu_ord__", "__ord_tipo__"], strict=False)
    )

    # Reordenar colunas para exibição: Descr_item e Descr_compl logo após Tipo_operacao
    cols = list(df_final.columns)
    if "Tipo_operacao" in cols and "Descr_item" in cols and "Descr_compl" in cols:
        cols.remove("Descr_item")
        cols.remove("Descr_compl")
        idx = cols.index("Tipo_operacao")
        cols.insert(idx + 1, "Descr_item")
        cols.insert(idx + 2, "Descr_compl")
        df_final = df_final.select(cols)

    data_ref_expr = pl.coalesce(
        [
            pl.col("Dt_e_s").cast(pl.Date, strict=False),
            pl.col("Dt_doc").cast(pl.Date, strict=False),
        ]
    )
    qtd_bruta_expr = pl.col("Qtd").cast(pl.Float64, strict=False).fill_null(0.0).abs()
    fator_expr = pl.col("fator").cast(pl.Float64, strict=False).fill_null(1.0).abs()
    q_conv_base_expr = (qtd_bruta_expr * fator_expr)
    infprot_valido_expr = (
        pl.col("infprot_cstat")
        .cast(pl.Utf8, strict=False)
        .fill_null("")
        .str.strip_chars()
        .is_in(["", "100", "150"])
    )
    q_conv_valido_expr = pl.when(infprot_valido_expr).then(q_conv_base_expr).otherwise(pl.lit(0.0))

    df_final = (
        df_final
        .with_columns(
            [
                data_ref_expr.alias("__data_ref_calc__"),
                data_ref_expr.dt.year().alias("__ano_saldo__"),
                q_conv_base_expr.alias("__q_conv_base__"),
            ]
        )
        .with_columns(
            [
                pl.when(
                    pl.col("Tipo_operacao").cast(pl.Utf8, strict=False).str.starts_with("3 - ESTOQUE FINAL")
                    & (pl.col("__data_ref_calc__").dt.month() == 12)
                    & (pl.col("__data_ref_calc__").dt.day() == 31)
                )
                .then(q_conv_valido_expr)
                .otherwise(pl.lit(0.0))
                .alias("__qtd_decl_final_audit__"),
                pl.when(
                    pl.col("Tipo_operacao").cast(pl.Utf8, strict=False).str.starts_with("0 - ESTOQUE INICIAL")
                    & (pl.col("__data_ref_calc__").dt.month() == 1)
                    & (pl.col("__data_ref_calc__").dt.day() == 1)
                )
                .then(q_conv_valido_expr)
                .when(pl.col("Tipo_operacao") == "1 - ENTRADA")
                .then(q_conv_valido_expr)
                .when(pl.col("Tipo_operacao") == "2 - SAIDAS")
                .then(q_conv_valido_expr)
                .otherwise(pl.lit(0.0))
                .alias("q_conv"),
                pl.when(
                    pl.col("Tipo_operacao").cast(pl.Utf8, strict=False).str.starts_with("0 - ESTOQUE INICIAL")
                    & (pl.col("__data_ref_calc__").dt.month() == 1)
                    & (pl.col("__data_ref_calc__").dt.day() == 1)
                )
                .then(q_conv_valido_expr)
                .when(pl.col("Tipo_operacao") == "1 - ENTRADA")
                .then(q_conv_valido_expr)
                .when(pl.col("Tipo_operacao") == "2 - SAIDAS")
                .then(-q_conv_valido_expr)
                .otherwise(pl.lit(0.0))
                .alias("__q_conv_sinal__"),
            ]
        )
        .with_columns(
            pl.when(pl.col("q_conv") > 0)
            .then(pl.col("preco_item").cast(pl.Float64, strict=False).fill_null(0.0) / pl.col("q_conv"))
            .otherwise(pl.lit(0.0))
            .alias("preco_unit")
        )
        .group_by("id_agrupado", "__ano_saldo__", maintain_order=True)
        .map_groups(_calcular_saldo_estoque_anual)
        .drop(["__data_ref_calc__", "__q_conv_base__"], strict=False)
    )

    for coluna, referencia in [("q_conv", "Qtd"), ("preco_unit", "preco_item")]:
        cols = list(df_final.columns)
        if coluna in cols and referencia in cols:
            cols.remove(coluna)
            idx_ref = cols.index(referencia)
            cols.insert(idx_ref + 1, coluna)
            df_final = df_final.select(cols)

    # Salvar
    saida = pasta_analises / f"mov_estoque_{cnpj}.parquet"
    ok = salvar_para_parquet(df_final, pasta_analises, saida.name)
    if ok:
        rprint(f"[green]Sucesso! {df_final.height} registros salvos.[/green]")
        
    return ok

if __name__ == "__main__":
    try:
        if len(sys.argv) > 1:
            gerar_movimentacao_estoque(sys.argv[1])
        else:
            c = input("CNPJ: ")
            gerar_movimentacao_estoque(c)
    except Exception as e:
        log_exception(e)
        raise
