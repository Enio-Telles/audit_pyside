import sys
import re
from pathlib import Path
from utilitarios.project_paths import PROJECT_ROOT, TRACEBACK_PATH
import polars as pl
from rich import print as rprint

ROOT_DIR = PROJECT_ROOT
SRC_DIR = ROOT_DIR / "src"
DADOS_DIR = ROOT_DIR / "dados"
CNPJ_ROOT = DADOS_DIR / "CNPJ"


try:
    from utilitarios.salvar_para_parquet import salvar_para_parquet
    from utilitarios.validacao_schema import (
        SchemaValidacaoError,
        validar_parquet_essencial,
    )
    from transformacao.co_sefin_class import enriquecer_co_sefin_class
    from utilitarios.text import remove_accents
    from transformacao.movimentacao_estoque_pkg.calculo_saldos import (
        _padronizar_tipo_operacao_expr,
        _boolish_expr,
        gerar_eventos_estoque as _gerar_eventos_estoque,
        calcular_saldo_estoque_anual as _calcular_saldo_estoque_anual,
    )
    from transformacao.movimentacao_estoque_pkg.mapeamento_fontes import (
        normalizar_descricao_expr as _normalizar_descricao_expr,
        detectar_coluna_descricao as _detectar_coluna_descricao,
        detectar_coluna_unidade as _detectar_coluna_unidade,
        parse_expression as _parse_expression,
        carregar_flags_cfop as _carregar_flags_cfop,
    )
except ImportError as e:
    rprint(f"[red]Erro ao importar modulos:[/red] {e}")
    sys.exit(1)


# --- FunÃ§Ãµes utilitÃ¡rias extraÃ­das para sub-mÃ³dulos ---
# calculo_saldos.py: _padronizar_tipo_operacao_expr, _boolish_expr,
#                    gerar_eventos_estoque, calcular_saldo_estoque_anual
# mapeamento_fontes.py: normalizar_descricao_expr, detectar_coluna_descricao,
#                       detectar_coluna_unidade, parse_expression, carregar_flags_cfop
# ---


def marcar_mov_rep_por_chave_item(df: pl.DataFrame) -> pl.DataFrame:
    """Marca mov_rep quando houver mais de uma linha com a mesma Chv_nfe + Num_item."""
    if df.is_empty() or "Chv_nfe" not in df.columns or "Num_item" not in df.columns:
        return df

    chave_expr = pl.col("Chv_nfe").cast(pl.Utf8, strict=False).fill_null("").str.strip_chars()
    item_expr = pl.col("Num_item").cast(pl.Utf8, strict=False).fill_null("").str.strip_chars()
    repetido_expr = (
        (chave_expr != "")
        & (item_expr != "")
        & (pl.len().over(["Chv_nfe", "Num_item"]) > 1)
    )

    if "mov_rep" in df.columns:
        return df.with_columns(
            (repetido_expr | _boolish_expr("mov_rep").fill_null(False)).alias("mov_rep")
        )

    return df.with_columns(repetido_expr.alias("mov_rep"))


def filtrar_movimentacoes_por_fonte(df: pl.DataFrame) -> pl.DataFrame:
    """Aplica a regra fiscal de direcao por origem na mov_estoque."""
    if df.is_empty() or "fonte" not in df.columns or "Tipo_operacao" not in df.columns:
        return df

    fonte_expr = pl.col("fonte").cast(pl.Utf8, strict=False).str.to_lowercase()
    tipo_expr = pl.col("Tipo_operacao").cast(pl.Utf8, strict=False)
    filtro_expr = (
        pl.when(fonte_expr == "c170")
        .then(tipo_expr == "1 - ENTRADA")
        .when(fonte_expr.is_in(["nfe", "nfce"]))
        .then(tipo_expr == "2 - SAIDAS")
        .otherwise(pl.lit(True))
    )
    return df.filter(filtro_expr)


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

    try:
        validar_parquet_essencial(
            arq_prod_final,
            [
                "id_agrupado",
                "descricao_normalizada",
                "descr_padrao",
                "ncm_padrao",
                "cest_padrao",
                "descricao_final",
                "co_sefin_final",
                "unid_ref_sugerida",
            ],
            contexto="movimentacao_estoque/produtos_final",
        )
        validar_parquet_essencial(
            arq_fatores,
            ["id_agrupado", "unid", "unid_ref", "fator"],
            contexto="movimentacao_estoque/fatores_conversao",
        )
    except SchemaValidacaoError as exc:
        rprint(f"[red]{exc}[/red]")
        return False

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
                # Para producao, uma funcao check columns Ã© melhor, faremos um fallback basico
                
                exprs.append(e)
            except Exception as ex:
                exprs.append(pl.lit(None).alias(target))
                
        # Polars: try selecting. Se alguma coluna referenciada falhar, ele crasheia.
        # Entao adicionamos as colunas q nÃ£o existem como nulas antes.
        cols_required = set()
        # Uma heuristic rapida para descobrir dependencias: vars() de orig
        for m in mapeamento:
            v = str(m[key_map])
            if v and v not in ["(vazio)", "correspondÃªncia com chave NF", "icms_orig & icms_cst ou icms_csosn", "prod_ceantrib ou caso for nulo -> prod_cean", "vl_item-vl_desc", "prod_vprod+prod_vfrete+prod_vseg+prod_voutro-prod_vdesc", "\"gerado\" ou \"registro\" (se está no bloco_h)"]:
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

        df_selecionado = df_selecionado.with_columns(pl.lit(prefix_sys).alias("fonte"))

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
    df_mov = (
        df_mov
        .with_columns(_padronizar_tipo_operacao_expr("Tipo_operacao"))
        .pipe(filtrar_movimentacoes_por_fonte)
    )
    
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
    for col in ["mov_rep", "excluir_estoque", "dev_simples", "dev_venda", "dev_compra", "dev_ent_simples"]:
        if col not in df_final.columns:
            df_final = df_final.with_columns(pl.lit(None).alias(col))
    df_final = marcar_mov_rep_por_chave_item(df_final)

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

    # Reordenar colunas para exibiÃ§Ã£o: Descr_item e Descr_compl logo apÃ³s Tipo_operacao
    cols = list(df_final.columns)
    if "Tipo_operacao" in cols:
        for coluna in ["fonte", "Descr_item", "Descr_compl"]:
            if coluna in cols:
                cols.remove(coluna)
        idx = cols.index("Tipo_operacao")
        posicao = idx + 1
        for coluna in ["fonte", "Descr_item", "Descr_compl"]:
            if coluna in df_final.columns:
                cols.insert(posicao, coluna)
                posicao += 1
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
    linha_neutra_expr = (
        _boolish_expr("excluir_estoque").fill_null(False)
        | _boolish_expr("mov_rep").fill_null(False)
    )
    infprot_valido_expr = (
        pl.col("infprot_cstat")
        .cast(pl.Utf8, strict=False)
        .fill_null("")
        .str.strip_chars()
        .is_in(["", "100", "150"])
    )
    q_conv_valido_expr = (
        pl.when(infprot_valido_expr & ~linha_neutra_expr)
        .then(q_conv_base_expr)
        .otherwise(pl.lit(0.0))
    )

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
                # __qtd_decl_final_audit__: captura a quantidade declarada em TODO estoque final do ano
                # A restricao de 31/12 foi removida para permitir auditoria anual correta
                pl.when(
                    pl.col("Tipo_operacao").cast(pl.Utf8, strict=False).str.starts_with("3 - ESTOQUE FINAL")
                )
                .then(q_conv_valido_expr)
                .otherwise(pl.lit(0.0))
                .alias("__qtd_decl_final_audit__"),
                # q_conv: estoque inicial, entradas, saidas e final capturados independente da data
                # para permitir auditoria anual completa
                # Nota: estoque final tem q_conv populado, mas nao afeta o saldo sequencial
                pl.when(
                    pl.col("Tipo_operacao").cast(pl.Utf8, strict=False).str.starts_with("0 - ESTOQUE INICIAL")
                )
                .then(q_conv_valido_expr)
                .when(pl.col("Tipo_operacao") == "1 - ENTRADA")
                .then(q_conv_valido_expr)
                .when(pl.col("Tipo_operacao") == "2 - SAIDAS")
                .then(q_conv_valido_expr)
                .when(
                    pl.col("Tipo_operacao").cast(pl.Utf8, strict=False).str.starts_with("3 - ESTOQUE FINAL")
                )
                .then(q_conv_valido_expr)
                .otherwise(pl.lit(0.0))
                .alias("q_conv"),
                # __q_conv_sinal__: usado no calculo sequencial de saldo
                # estoque final nao entra aqui para nao afetar o saldo acumulado
                pl.when(
                    pl.col("Tipo_operacao").cast(pl.Utf8, strict=False).str.starts_with("0 - ESTOQUE INICIAL")
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
        import traceback
        with open(TRACEBACK_PATH, "w", encoding="utf-8") as f:
            traceback.print_exc(file=f)
        raise


