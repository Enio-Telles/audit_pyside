import json
import sys
import re
from pathlib import Path
from time import perf_counter

import polars as pl
from utilitarios.perf_monitor import registrar_evento_performance

ROOT_DIR = Path(r"c:\funcoes - Copia")
SRC_DIR = ROOT_DIR / "src"
DADOS_DIR = ROOT_DIR / "dados"
CNPJ_ROOT = DADOS_DIR / "CNPJ"


try:
    from transformacao.produtos_agrupados import calcular_atributos_padrao
except ImportError:
    calcular_atributos_padrao = None
    

try:
    from transformacao.produtos_final_v2 import produtos_agrupados as inicializar_produtos_agrupados
except ImportError:
    inicializar_produtos_agrupados = None

try:
    from transformacao.fontes_produtos import gerar_fontes_produtos
except ImportError:
    gerar_fontes_produtos = None

try:
    from transformacao.fatores_conversao import calcular_fatores_conversao
except ImportError:
    calcular_fatores_conversao = None

try:
    from transformacao.precos_medios_produtos_final import calcular_precos_medios_produtos_final
except ImportError:
    calcular_precos_medios_produtos_final = None

try:
    from transformacao.id_agrupados import gerar_id_agrupados
except ImportError:
    gerar_id_agrupados = None

try:
    from transformacao.c170_xml import gerar_c170_xml
except ImportError:
    gerar_c170_xml = None

try:
    from transformacao.c176_xml import gerar_c176_xml
except ImportError:
    gerar_c176_xml = None

try:
    from transformacao.movimentacao_estoque import gerar_movimentacao_estoque
except ImportError:
    gerar_movimentacao_estoque = None

try:
    from transformacao.calculos_mensais import gerar_calculos_mensais
except ImportError:
    gerar_calculos_mensais = None

try:
    from transformacao.calculos_anuais import gerar_calculos_anuais
except ImportError:
    gerar_calculos_anuais = None


class ServicoAgregacao:
    """Gerencia a tabela produtos_agrupados e as derivacoes da camada _agr."""

    def __init__(self) -> None:
        self.ultimo_tempo_etapas: dict[str, float] = {}

    def _registrar_tempo(self, nome: str, duracao: float, progresso=None, contexto: dict | None = None) -> None:
        self.ultimo_tempo_etapas[nome] = duracao
        registrar_evento_performance(f"aggregation_service.{nome}", duracao, contexto or {})
        if progresso:
            progresso(f"OK {nome} em {duracao:.2f}s")

    def _executar_etapa_tempo(self, nome: str, funcao, progresso=None, contexto: dict | None = None):
        if progresso:
            progresso(f"Iniciando {nome}...")
        inicio = perf_counter()
        resultado = funcao()
        self._registrar_tempo(nome, perf_counter() - inicio, progresso, contexto=contexto)
        return resultado

    def resumo_tempos(self) -> str:
        if not self.ultimo_tempo_etapas:
            return ""
        return " | ".join(f"{nome}: {duracao:.2f}s" for nome, duracao in self.ultimo_tempo_etapas.items())

    @staticmethod
    def _sanitizar_cnpj(cnpj: str) -> str:
        return re.sub(r"\D", "", cnpj or "")

    def _pasta_produtos(self, cnpj: str) -> Path:
        return CNPJ_ROOT / self._sanitizar_cnpj(cnpj) / "analises" / "produtos"

    def artefatos_estoque_defasados(self, cnpj: str) -> list[str]:
        cnpj = self._sanitizar_cnpj(cnpj)
        pasta_prod = self._pasta_produtos(cnpj)
        mov_path = pasta_prod / f"mov_estoque_{cnpj}.parquet"
        if not mov_path.exists():
            return []

        mov_mtime = mov_path.stat().st_mtime_ns
        derivados = {
            "calculos_mensais": pasta_prod / f"aba_mensal_{cnpj}.parquet",
            "calculos_anuais": pasta_prod / f"aba_anual_{cnpj}.parquet",
        }
        return [
            etapa
            for etapa, caminho in derivados.items()
            if not caminho.exists() or caminho.stat().st_mtime_ns < mov_mtime
        ]

    @staticmethod
    def _normalizar_descricao_para_match(texto: str | None):
        from utilitarios.text import remove_accents
        return " ".join((remove_accents(texto) or "").upper().strip().split()) if texto is not None else ""

    @staticmethod
    def _primeira_descricao_por_chaves(df_prod: pl.DataFrame, chaves: list[str]) -> str | None:
        if not chaves:
            return None
        df_desc = (
            df_prod
            .filter(pl.col("chave_item").is_in(chaves))
            .select(
                pl.col("descricao")
                .cast(pl.Utf8, strict=False)
                .fill_null("")
                .str.strip_chars()
                .alias("descricao")
            )
            .filter(pl.col("descricao") != "")
        )
        return df_desc.item(0, 0) if df_desc.height else None

    @staticmethod
    def _promover_tipos_sefin(df: pl.DataFrame) -> pl.DataFrame:
        """Garante tipos textuais para colunas de sefin/padroes, evitando schema List[null]/Null."""
        casts = []
        if "lista_co_sefin" in df.columns:
            casts.append(
                pl.col("lista_co_sefin")
                .cast(pl.List(pl.Utf8), strict=False)
                .alias("lista_co_sefin")
            )
        if "co_sefin_padrao" in df.columns:
            casts.append(
                pl.col("co_sefin_padrao")
                .cast(pl.Utf8, strict=False)
                .alias("co_sefin_padrao")
            )
        for col in ["ncm_padrao", "cest_padrao", "gtin_padrao"]:
            if col in df.columns:
                casts.append(pl.col(col).cast(pl.Utf8, strict=False).alias(col))
        for col in ["lista_ncm", "lista_cest", "lista_gtin", "lista_descricoes"]:
            if col in df.columns:
                casts.append(
                    pl.col(col)
                    .cast(pl.List(pl.Utf8), strict=False)
                    .alias(col)
                )
        return df.with_columns(casts) if casts else df

    @staticmethod
    def _normalizar_lista_textos(valor) -> list[str]:
        """Converte listas aninhadas do Polars, listas Python ou escalares em lista de strings validas."""
        if valor is None:
            return []
        if isinstance(valor, pl.Series):
            itens = valor.to_list()
        elif isinstance(valor, (list, tuple, set)):
            itens = list(valor)
        else:
            itens = [valor]
        return [str(item).strip() for item in itens if item is not None and str(item).strip()]

    @classmethod
    def _coletar_lista_coluna(cls, df: pl.DataFrame, coluna: str) -> list[str]:
        if df.is_empty() or coluna not in df.columns:
            return []

        series = df.get_column(coluna)
        if series.dtype.is_nested():
            series = series.explode()

        series_clean = (
            series
            .cast(pl.Utf8, strict=False)
            .str.strip_chars()
            .drop_nulls()
        )

        return (
            series_clean
            .filter(series_clean != "")
            .unique()
            .sort()
            .to_list()
        )

    @staticmethod
    def _garantir_colunas_lista_agregacao(df: pl.DataFrame) -> pl.DataFrame:
        exprs = []
        for coluna in ["lista_ncm", "lista_cest", "lista_gtin", "lista_descricoes"]:
            if coluna not in df.columns:
                exprs.append(pl.lit([]).cast(pl.List(pl.Utf8), strict=False).alias(coluna))
        return df.with_columns(exprs) if exprs else df

    @staticmethod
    def _padronizar_chaves_prod(df: pl.DataFrame) -> pl.DataFrame:
        """
        Define chave_item como chave canonica interna.
        Mantem chave_produto como coluna legado.
        """
        if "chave_item" not in df.columns and "id_descricao" in df.columns:
            df = df.with_columns(pl.col("id_descricao").alias("chave_item"))
        if "chave_item" not in df.columns and "chave_produto" in df.columns:
            df = df.with_columns(pl.col("chave_produto").alias("chave_item"))
        if "chave_produto" not in df.columns and "id_descricao" in df.columns:
            df = df.with_columns(pl.col("id_descricao").alias("chave_produto"))
        if "chave_produto" not in df.columns and "chave_item" in df.columns:
            df = df.with_columns(pl.col("chave_item").alias("chave_produto"))
        return df

    def caminho_tabela_agregadas(self, cnpj: str) -> Path:
        return CNPJ_ROOT / cnpj / "analises" / "produtos" / f"produtos_agrupados_{cnpj}.parquet"

    def caminho_tabela_editavel(self, cnpj: str) -> Path:
        """
        A tabela superior da aba de agregacao deve usar a mesma base da
        tabela inferior: produtos_agrupados, sempre com id_agrupado.
        """
        return self.caminho_tabela_agregadas(cnpj)

    def caminho_tabela_base(self, cnpj: str) -> Path:
        return CNPJ_ROOT / cnpj / "analises" / "produtos" / f"descricao_produtos_{cnpj}.parquet"

    def caminho_itens_unidades(self, cnpj: str) -> Path:
        return CNPJ_ROOT / cnpj / "analises" / "produtos" / f"item_unidades_{cnpj}.parquet"

    def caminho_log_agregacoes(self, cnpj: str) -> Path:
        return CNPJ_ROOT / cnpj / "analises" / "produtos" / f"log_agregacoes_{cnpj}.json"

    def caminho_tabela_final(self, cnpj: str) -> Path:
        return CNPJ_ROOT / cnpj / "analises" / "produtos" / f"produtos_final_{cnpj}.parquet"

    def caminho_tabela_id_agrupados(self, cnpj: str) -> Path:
        return CNPJ_ROOT / cnpj / "analises" / "produtos" / f"id_agrupados_{cnpj}.parquet"

    @staticmethod
    def _ler_parquet_colunas(path: Path, colunas: list[str]) -> pl.DataFrame:
        schema_cols = list(pl.read_parquet_schema(path).names())
        selecionadas = [col for col in colunas if col in schema_cols]
        if not selecionadas:
            return pl.DataFrame()
        return pl.scan_parquet(path).select(selecionadas).collect()

    @staticmethod
    def _obter_schema_parquet(path: Path) -> set[str]:
        if not path.exists():
            return set()
        return set(pl.read_parquet_schema(path).names())

    @staticmethod
    def _colunas_metricas_agregacao() -> list[str]:
        return [
            "total_compras",
            "qtd_compras_total",
            "preco_medio_compra",
            "total_vendas",
            "qtd_vendas_total",
            "preco_medio_venda",
            "total_entradas",
            "total_saidas",
            "total_movimentacao",
        ]

    @staticmethod
    def _colunas_descricoes_agregacao() -> set[str]:
        return {
            "lista_ncm",
            "lista_cest",
            "lista_gtin",
            "lista_descricoes",
        }

    def _regenerar_tabela_agregadas_legada(self, cnpj: str) -> bool:
        """
        Recria a camada de agregacao quando o parquet legado nao possui
        colunas canonicas de descricoes.
        """
        path_agrup = self.caminho_tabela_agregadas(cnpj)
        schema_cols = self._obter_schema_parquet(path_agrup)
        if "lista_descricoes" in schema_cols:
            return True

        path_base = self.caminho_tabela_base(cnpj)
        path_final = self.caminho_tabela_final(cnpj)
        if not path_agrup.exists() or not path_base.exists() or not path_final.exists():
            return False

        if inicializar_produtos_agrupados is None:
            raise ImportError("Nao foi possivel importar produtos_final_v2.py.")

        ok_regeneracao = bool(inicializar_produtos_agrupados(cnpj))
        if not ok_regeneracao:
            return False

        path_id_agrupados = self.caminho_tabela_id_agrupados(cnpj)
        if gerar_id_agrupados is not None and path_final.exists():
            ok_regeneracao = bool(gerar_id_agrupados(cnpj)) and ok_regeneracao

        if not ok_regeneracao:
            return False

        schema_regenerado = self._obter_schema_parquet(path_agrup)
        if "lista_descricoes" not in schema_regenerado:
            return False

        if path_id_agrupados.exists() and "lista_descricoes" not in self._obter_schema_parquet(path_id_agrupados):
            return False

        return True

    def garantir_metricas_tabela_agregadas(self, cnpj: str) -> bool:
        path = self.garantir_tabela_agregadas(cnpj, criar_se_ausente=True)
        if not path.exists():
            return False

        if not self._regenerar_tabela_agregadas_legada(cnpj):
            return False

        schema_cols = self._obter_schema_parquet(path)
        colunas_esperadas = set(self._colunas_metricas_agregacao()) | self._colunas_descricoes_agregacao()
        if colunas_esperadas.issubset(schema_cols):
            return True
        ok_padroes = bool(
            self.recalcular_todos_padroes(
                cnpj,
                reprocessar_referencias=False,
                reset_timings=False,
            )
        )
        if not ok_padroes:
            return False
        return bool(
            self.recalcular_valores_totais(
                cnpj,
                reprocessar_referencias=False,
                reset_timings=False,
            )
        )

    def garantir_tabela_agregadas(
        self,
        cnpj: str,
        criar_se_ausente: bool = False,
    ) -> Path:
        path = self.caminho_tabela_agregadas(cnpj)
        if path.exists():
            return path

        if not criar_se_ausente:
            return path

        if inicializar_produtos_agrupados is None:
            raise ImportError("Nao foi possivel importar produtos_agrupados.py.")

        ok = inicializar_produtos_agrupados(cnpj)
        if not ok or not path.exists():
            raise FileNotFoundError(
                "Tabela produtos_agrupados nao encontrada. Gere descricao_produtos/produtos_final antes de abrir a agregacao."
            )

        self.recalcular_valores_totais(cnpj, reprocessar_referencias=False, reset_timings=False)
        self.recalcular_produtos_final(cnpj)
        return path

    def carregar_tabela_agregadas(
        self,
        cnpj: str,
        criar_se_ausente: bool = False,
    ) -> pl.DataFrame:
        path = self.garantir_tabela_agregadas(cnpj, criar_se_ausente=criar_se_ausente)
        if not path.exists():
            return pl.DataFrame()

        df_agrup = pl.read_parquet(path)
        arq_pont = CNPJ_ROOT / cnpj / "analises" / "produtos" / f"map_produto_agrupado_{cnpj}.parquet"
        if arq_pont.exists() and "lista_chave_produto" not in df_agrup.columns:
            df_pont = pl.read_parquet(arq_pont)
            df_list = df_pont.group_by("id_agrupado").agg(pl.col("chave_produto").alias("lista_chave_produto"))
            df_agrup = df_agrup.join(df_list, on="id_agrupado", how="left")
        return df_agrup

    def agregar_linhas(self, cnpj: str, ids_agrupados_selecionados: list[str]) -> dict:
        """
        Une multiplos grupos (linhas de produtos_agrupados) em um so.
        """
        if calcular_atributos_padrao is None:
            raise ImportError("Nao foi possivel importar produtos_agrupados.py.")

        df = self._garantir_colunas_lista_agregacao(
            self._promover_tipos_sefin(self.carregar_tabela_agregadas(cnpj, criar_se_ausente=True))
        )
        df_base = self._ler_parquet_colunas(
            self.caminho_itens_unidades(cnpj),
            ["descricao", "ncm", "cest", "gtin", "co_sefin_item", "fontes", "fonte"],
        ).with_columns(
            pl.col("descricao")
            .map_elements(self._normalizar_descricao_para_match, return_dtype=pl.String)
            .alias("descricao_normalizada_temp")
        )

        df_para_unir = df.filter(pl.col("id_agrupado").is_in(ids_agrupados_selecionados))
        df_restante = df.filter(~pl.col("id_agrupado").is_in(ids_agrupados_selecionados))

        if df_para_unir.height < 2:
            raise ValueError("Selecione pelo menos 2 grupos para unir.")

        todas_chaves_proc = []
        for lista in df_para_unir["lista_chave_produto"]:
            todas_chaves_proc.extend(lista)
        todas_chaves_proc = list(set(todas_chaves_proc))

        df_prod_link = self._padronizar_chaves_prod(
            self._ler_parquet_colunas(
                self.caminho_tabela_base(cnpj),
                [
                    "id_descricao", "chave_item", "chave_produto", "descricao_normalizada", "descricao",
                    "lista_desc_compl", "lista_ncm", "lista_cest", "lista_gtin", "fontes",
                ],
            )
        )
        df_prod_sel = df_prod_link.filter(pl.col("chave_item").is_in(todas_chaves_proc))
        lista_desc_norm = df_prod_sel["descricao_normalizada"].to_list()

        df_base_filtered = df_base.filter(
            pl.col("descricao_normalizada_temp").is_in(lista_desc_norm)
        ).drop("descricao_normalizada_temp")

        padrao = calcular_atributos_padrao(df_base_filtered)
        descr_fallback = self._primeira_descricao_por_chaves(df_prod_link, todas_chaves_proc)

        lista_sefin = []
        for lista in df_para_unir["lista_co_sefin"]:
            lista_sefin.extend(self._normalizar_lista_textos(lista))
        lista_sefin = sorted(list(set(lista_sefin)))
        lista_fontes = []
        if "fontes" in df_para_unir.columns:
            for lista in df_para_unir["fontes"]:
                lista_fontes.extend(self._normalizar_lista_textos(lista))
        if not lista_fontes and "fontes" in df_base_filtered.columns:
            lista_fontes = (
                df_base_filtered
                .explode("fontes")
                .drop_nulls("fontes")
                .select(pl.col("fontes").cast(pl.Utf8, strict=False).str.strip_chars().alias("fonte"))
                .filter(pl.col("fonte") != "")
                .unique()
                .sort("fonte")
                .get_column("fonte")
                .to_list()
            )
        if not lista_fontes and "fonte" in df_base_filtered.columns:
            lista_fontes = (
                df_base_filtered
                .select(
                    pl.when(pl.col("fonte").is_not_null())
                    .then(pl.col("fonte").cast(pl.Utf8, strict=False).str.strip_chars())
                    .otherwise(pl.lit(""))
                    .alias("fonte")
                )
                .filter(pl.col("fonte") != "")
                .unique()
                .sort("fonte")
                .get_column("fonte")
                .to_list()
            )
        if not lista_fontes and "fontes" in df_prod_sel.columns:
            lista_fontes = (
                df_prod_sel
                .explode("fontes")
                .drop_nulls("fontes")
                .select(pl.col("fontes").cast(pl.Utf8, strict=False).str.strip_chars().alias("fonte"))
                .filter(pl.col("fonte") != "")
                .unique()
                .sort("fonte")
                .get_column("fonte")
                .to_list()
            )
        lista_fontes = sorted(list(set(lista_fontes)))
        lista_ncm = self._coletar_lista_coluna(df_prod_sel, "lista_ncm") or self._coletar_lista_coluna(df_base_filtered, "ncm")
        lista_cest = self._coletar_lista_coluna(df_prod_sel, "lista_cest") or self._coletar_lista_coluna(df_base_filtered, "cest")
        lista_gtin = self._coletar_lista_coluna(df_prod_sel, "lista_gtin") or self._coletar_lista_coluna(df_base_filtered, "gtin")
        lista_descricoes = sorted(
            set(
                self._coletar_lista_coluna(df_prod_sel, "descricao")
                + self._coletar_lista_coluna(df_prod_sel, "lista_desc_compl")
            )
        ) or self._coletar_lista_coluna(df_base_filtered, "descricao")

        nova_linha = {
            "id_agrupado": ids_agrupados_selecionados[0],
            "lista_chave_produto": todas_chaves_proc,
            "descr_padrao": padrao.get("descr_padrao") or descr_fallback,
            "ncm_padrao": padrao.get("ncm_padrao"),
            "cest_padrao": padrao.get("cest_padrao"),
            "gtin_padrao": padrao.get("gtin_padrao"),
            "lista_ncm": lista_ncm,
            "lista_cest": lista_cest,
            "lista_gtin": lista_gtin,
            "lista_descricoes": lista_descricoes,
            "lista_co_sefin": lista_sefin,
            "co_sefin_padrao": padrao.get("co_sefin_padrao"),
            "co_sefin_agr": ", ".join(sorted([str(s) for s in lista_sefin])),
            "lista_unidades": sorted(
                list(
                    set(
                        u
                        for sub in df_para_unir["lista_unidades"]
                        for u in self._normalizar_lista_textos(sub)
                    )
                )
            ),
            "co_sefin_divergentes": len(lista_sefin) > 1,
            "fontes": lista_fontes,
        }

        df_nova = pl.DataFrame([nova_linha], schema=df.schema)
        df_resultado = pl.concat([df_nova, df_restante])
        df_resultado = (
            df_resultado.with_row_index("idx")
            .with_columns(
                (pl.lit("id_agrupado_") + (pl.col("idx") + 1).cast(pl.String)).alias("id_agrupado")
            )
            .drop("idx")
        )

        df_resultado_sem_chaves = df_resultado.drop("lista_chave_produto", strict=False)
        df_resultado_sem_chaves.write_parquet(self.caminho_tabela_agregadas(cnpj))

        if "lista_chave_produto" in df_resultado.columns:
            df_map = df_resultado.select(["id_agrupado", "lista_chave_produto"]).explode("lista_chave_produto").rename({"lista_chave_produto": "chave_produto"}).drop_nulls("chave_produto")
            arq_pont = CNPJ_ROOT / cnpj / "analises" / "produtos" / f"map_produto_agrupado_{cnpj}.parquet"
            df_map.write_parquet(arq_pont)

        if not self.recalcular_valores_totais(cnpj, reprocessar_referencias=False, reset_timings=False):
            raise RuntimeError("Falha ao recalcular totais e precos medios da agregacao.")

        # Mantem tabelas derivadas sincronizadas apos revisao manual de agrupamentos.
        self.recalcular_produtos_final(cnpj)
        self.recalcular_referencias_produtos(cnpj)

        self._registrar_log(
            cnpj,
            {
                "tipo": "agregacao",
                "ids_unidos": ids_agrupados_selecionados,
                "nova_descr": nova_linha["descr_padrao"],
            },
        )

        return {"success": True}

    def recalcular_produtos_final(self, cnpj: str) -> bool:
        path_base = self.caminho_tabela_base(cnpj)
        path_agrup = self.caminho_tabela_agregadas(cnpj)
        path_pont = CNPJ_ROOT / cnpj / "analises" / "produtos" / f"map_produto_agrupado_{cnpj}.parquet"
        path_final = self.caminho_tabela_final(cnpj)

        if not path_base.exists() or not path_agrup.exists():
            return False

        df_base = self._padronizar_chaves_prod(pl.scan_parquet(path_base).collect())
        df_agrup = self._promover_tipos_sefin(pl.scan_parquet(path_agrup).collect())

        if path_pont.exists():
            df_pont = (
                pl.scan_parquet(path_pont).collect()
                .with_columns(pl.col("chave_produto").cast(pl.Utf8, strict=False))
            )
        elif "lista_chave_produto" in df_agrup.columns:
            df_pont = (
                df_agrup
                .select(["id_agrupado", "lista_chave_produto"])
                .explode("lista_chave_produto")
                .rename({"lista_chave_produto": "chave_produto"})
                .drop_nulls("chave_produto")
            )
        else:
            return False

        if "lista_chave_produto" not in df_agrup.columns:
            df_list = df_pont.group_by("id_agrupado").agg(pl.col("chave_produto").alias("lista_chave_produto"))
            df_agrup = df_agrup.join(df_list, on="id_agrupado", how="left")

        if "co_sefin_agr" not in df_agrup.columns:
            df_agrup = df_agrup.with_columns(
                pl.col("lista_co_sefin")
                .cast(pl.List(pl.Utf8), strict=False)
                .list.join(", ")
                .alias("co_sefin_agr")
            )

        df_map = (
            df_agrup
            .select(
                [
                    "id_agrupado",
                    "lista_chave_produto",
                    "descr_padrao",
                    "ncm_padrao",
                    "cest_padrao",
                    "gtin_padrao",
                    pl.col("lista_co_sefin").alias("lista_co_sefin_agr"),
                    "co_sefin_padrao",
                    "co_sefin_agr",
                    pl.col("lista_unidades").alias("lista_unidades_agr"),
                    "co_sefin_divergentes",
                ]
            )
            .explode("lista_chave_produto")
            .rename({"lista_chave_produto": "chave_item"})
        )

        df_final = (
            df_base
            .join(df_map, on="chave_item", how="left")
            .with_columns(
                [
                    pl.coalesce([pl.col("descr_padrao"), pl.col("descricao")]).alias("descricao_final"),
                    pl.coalesce([pl.col("ncm_padrao"), pl.col("lista_ncm").list.first()]).alias("ncm_final"),
                    pl.coalesce([pl.col("cest_padrao"), pl.col("lista_cest").list.first()]).alias("cest_final"),
                    pl.coalesce([pl.col("gtin_padrao"), pl.col("lista_gtin").list.first()]).alias("gtin_final"),
                    pl.coalesce(
                        [
                            pl.col("co_sefin_padrao"),
                            pl.col("lista_co_sefin_agr").list.first(),
                            pl.col("lista_co_sefin").list.first(),
                        ]
                    ).alias("co_sefin_final"),
                    pl.coalesce([pl.col("lista_unidades_agr").list.first(), pl.col("lista_unid").list.first()]).alias("unid_ref_sugerida"),
                ]
            )
            .sort(["id_agrupado", "chave_item"], nulls_last=True)
        )

        df_final.write_parquet(path_final)
        if gerar_id_agrupados is not None:
            return bool(gerar_id_agrupados(cnpj))
        return True

    def refazer_tabelas_agr(self, cnpj: str) -> bool:
        """Regenera c170_agr/bloco_h_agr/nfe_agr/nfce_agr com base nas agregacoes atuais."""
        if gerar_fontes_produtos is None:
            raise ImportError("Nao foi possivel importar fontes_produtos.py.")
        return bool(gerar_fontes_produtos(cnpj))

    def recalcular_referencias_agr(self, cnpj: str, progresso=None, reset_timings: bool = True) -> bool:
        """
        Recalcula tabelas dependentes das agregacoes:
        - produtos_final
        - fontes *_agr
        - fatores_conversao
        - c170_xml
        - c176_xml
        - mov_estoque
        - aba_mensal
        - aba_anual
        """
        if reset_timings:
            self.ultimo_tempo_etapas = {}
        inicio_total = perf_counter()
        contexto_base = {"cnpj": cnpj, "fluxo": "recalcular_referencias_agr"}
        ok_final = self._executar_etapa_tempo("produtos_final", lambda: self.recalcular_produtos_final(cnpj), progresso, contexto=contexto_base)
        ok_fontes = self._executar_etapa_tempo("fontes_agr", lambda: self.refazer_tabelas_agr(cnpj), progresso, contexto=contexto_base)
        if calcular_fatores_conversao is None:
            raise ImportError("Nao foi possivel importar fatores_conversao.py.")
        if gerar_c170_xml is None:
            raise ImportError("Nao foi possivel importar c170_xml.py.")
        if gerar_c176_xml is None:
            raise ImportError("Nao foi possivel importar c176_xml.py.")
        if gerar_movimentacao_estoque is None:
            raise ImportError("Nao foi possivel importar movimentacao_estoque.py.")
        if gerar_calculos_mensais is None:
            raise ImportError("Nao foi possivel importar calculos_mensais.py.")
        if gerar_calculos_anuais is None:
            raise ImportError("Nao foi possivel importar calculos_anuais.py.")

        ok_fatores = self._executar_etapa_tempo("fatores_conversao", lambda: bool(calcular_fatores_conversao(cnpj)), progresso, contexto=contexto_base) if (ok_final and ok_fontes) else False
        ok_c170 = self._executar_etapa_tempo("c170_xml", lambda: bool(gerar_c170_xml(cnpj)), progresso, contexto=contexto_base) if ok_fatores else False
        ok_c176 = self._executar_etapa_tempo("c176_xml", lambda: bool(gerar_c176_xml(cnpj)), progresso, contexto=contexto_base) if ok_c170 else False
        ok_mov = self._executar_etapa_tempo("mov_estoque", lambda: bool(gerar_movimentacao_estoque(cnpj)), progresso, contexto=contexto_base) if ok_c176 else False
        ok_mensal = self._executar_etapa_tempo("calculos_mensais", lambda: bool(gerar_calculos_mensais(cnpj)), progresso, contexto=contexto_base) if ok_mov else False
        ok_anual = self._executar_etapa_tempo("calculos_anuais", lambda: bool(gerar_calculos_anuais(cnpj)), progresso, contexto=contexto_base) if ok_mensal else False
        ok_total = bool(ok_final and ok_fontes and ok_fatores and ok_c170 and ok_c176 and ok_mov and ok_mensal and ok_anual)
        self._registrar_tempo(
            "recalcular_referencias_agr_total",
            perf_counter() - inicio_total,
            progresso,
            contexto={**contexto_base, "sucesso": ok_total},
        )
        return ok_total

    def refazer_tabelas_produtos(self, cnpj: str) -> bool:
        """Alias legado para refazer_tabelas_agr."""
        return self.refazer_tabelas_agr(cnpj)

    def recalcular_referencias_produtos(self, cnpj: str, progresso=None, reset_timings: bool = True) -> bool:
        """Alias legado para recalcular_referencias_agr."""
        return self.recalcular_referencias_agr(cnpj, progresso=progresso, reset_timings=reset_timings)

    def recalcular_mov_estoque(self, cnpj: str, progresso=None, reset_timings: bool = True) -> bool:
        """
        Recalcula artefatos diretamente afetados por ajustes manuais em fatores de conversao.
        """
        if gerar_c176_xml is None:
            raise ImportError("Nao foi possivel importar c176_xml.py.")
        if gerar_movimentacao_estoque is None:
            raise ImportError("Nao foi possivel importar movimentacao_estoque.py.")
        if gerar_calculos_mensais is None:
            raise ImportError("Nao foi possivel importar calculos_mensais.py.")
        if gerar_calculos_anuais is None:
            raise ImportError("Nao foi possivel importar calculos_anuais.py.")

        if reset_timings:
            self.ultimo_tempo_etapas = {}
        inicio_total = perf_counter()
        contexto_base = {"cnpj": cnpj, "fluxo": "recalcular_mov_estoque"}
        ok_c176 = self._executar_etapa_tempo("c176_xml", lambda: bool(gerar_c176_xml(cnpj)), progresso, contexto=contexto_base)
        ok_mov = self._executar_etapa_tempo("mov_estoque", lambda: bool(gerar_movimentacao_estoque(cnpj)), progresso, contexto=contexto_base) if ok_c176 else False
        ok_mensal = self._executar_etapa_tempo("calculos_mensais", lambda: bool(gerar_calculos_mensais(cnpj)), progresso, contexto=contexto_base) if ok_mov else False
        ok_anual = self._executar_etapa_tempo("calculos_anuais", lambda: bool(gerar_calculos_anuais(cnpj)), progresso, contexto=contexto_base) if ok_mensal else False
        ok_total = bool(ok_c176 and ok_mov and ok_mensal and ok_anual)
        self._registrar_tempo(
            "recalcular_mov_estoque_total",
            perf_counter() - inicio_total,
            progresso,
            contexto={**contexto_base, "sucesso": ok_total},
        )
        return ok_total

    def recalcular_resumos_estoque(self, cnpj: str, progresso=None, reset_timings: bool = True) -> bool:
        """
        Recalcula apenas os resumos mensal/anual quando estiverem ausentes ou defasados da mov_estoque.
        """
        if gerar_calculos_mensais is None:
            raise ImportError("Nao foi possivel importar calculos_mensais.py.")
        if gerar_calculos_anuais is None:
            raise ImportError("Nao foi possivel importar calculos_anuais.py.")

        if reset_timings:
            self.ultimo_tempo_etapas = {}
        inicio_total = perf_counter()
        contexto_base = {"cnpj": cnpj, "fluxo": "recalcular_resumos_estoque"}
        artefatos_defasados = self.artefatos_estoque_defasados(cnpj)

        if not artefatos_defasados:
            self._registrar_tempo(
                "recalcular_resumos_estoque_total",
                perf_counter() - inicio_total,
                progresso,
                contexto={**contexto_base, "sucesso": True, "etapas": []},
            )
            if progresso:
                progresso("Mensal e anual ja estao sincronizadas com mov_estoque.")
            return True

        ok_mensal = True
        ok_anual = True
        if "calculos_mensais" in artefatos_defasados:
            ok_mensal = self._executar_etapa_tempo(
                "calculos_mensais",
                lambda: bool(gerar_calculos_mensais(cnpj)),
                progresso,
                contexto=contexto_base,
            )
        if "calculos_anuais" in artefatos_defasados:
            ok_anual = self._executar_etapa_tempo(
                "calculos_anuais",
                lambda: bool(gerar_calculos_anuais(cnpj)),
                progresso,
                contexto=contexto_base,
            )

        ok_total = bool(ok_mensal and ok_anual)
        self._registrar_tempo(
            "recalcular_resumos_estoque_total",
            perf_counter() - inicio_total,
            progresso,
            contexto={**contexto_base, "sucesso": ok_total, "etapas": artefatos_defasados},
        )
        return ok_total

    def _registrar_log(self, cnpj: str, entrada: dict):
        log_path = self.caminho_log_agregacoes(cnpj)
        historico = []
        if log_path.exists():
            try:
                with open(log_path, "r", encoding="utf-8") as f:
                    historico = json.load(f)
            except Exception:
                pass

        from datetime import datetime

        entrada["timestamp"] = datetime.now().isoformat()
        historico.append(entrada)
        with open(log_path, "w", encoding="utf-8") as f:
            json.dump(historico, f, indent=2, ensure_ascii=False)

    def ler_linhas_log(self, cnpj: str = "") -> list:
        if not cnpj:
            return []
        log_path = self.caminho_log_agregacoes(cnpj)
        if not log_path.exists():
            return []
        try:
            with open(log_path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return []

    def carregar_tabela_editavel(self, cnpj: str) -> Path:
        path_agr = self.garantir_tabela_agregadas(cnpj, criar_se_ausente=True)
        if not path_agr.exists():
            raise FileNotFoundError(
                "Tabela produtos_agrupados nao encontrada. Gere ou recalcule a camada de agregacao antes de abrir a aba."
            )
        self.garantir_metricas_tabela_agregadas(cnpj)
        return path_agr

    def recalcular_todos_padroes(
        self,
        cnpj: str,
        progresso=None,
        reprocessar_referencias: bool = True,
        reset_timings: bool = True,
    ) -> bool:
        """
        Recalcula descr/ncm/cest/gtin/co_sefin padrao de todos os grupos
        com base nos itens originais em item_unidades.
        """
        if reset_timings:
            self.ultimo_tempo_etapas = {}
        inicio_total = perf_counter()
        if calcular_atributos_padrao is None:
            raise ImportError("Nao foi possivel importar produtos_agrupados.py.")

        path_agrup = self.caminho_tabela_agregadas(cnpj)
        path_prod = self.caminho_tabela_base(cnpj)
        path_base = self.caminho_itens_unidades(cnpj)

        if not path_agrup.exists() or not path_prod.exists() or not path_base.exists():
            return False

        df_agrup = self._garantir_colunas_lista_agregacao(
            self._promover_tipos_sefin(self.carregar_tabela_agregadas(cnpj))
        )
        df_prod = self._padronizar_chaves_prod(
            self._ler_parquet_colunas(
                path_prod,
                [
                    "id_descricao", "chave_item", "chave_produto", "descricao_normalizada", "descricao",
                    "lista_desc_compl", "lista_ncm", "lista_cest", "lista_gtin",
                    "lista_co_sefin", "lista_unid", "fontes",
                ],
            )
        )
        df_base = self._ler_parquet_colunas(path_base, ["descricao", "fonte", "fontes", "ncm", "cest", "gtin", "co_sefin_item"]).with_columns(
            pl.col("descricao")
            .map_elements(self._normalizar_descricao_para_match, return_dtype=pl.String)
            .alias("descricao_normalizada_temp")
        )

        registros = []
        for row in df_agrup.iter_rows(named=True):
            chaves = row.get("lista_chave_produto", []) or []
            desc_norm = (
                df_prod.filter(pl.col("chave_item").is_in(chaves))["descricao_normalizada"].to_list()
                if chaves
                else []
            )

            df_base_filtered = df_base.filter(
                pl.col("descricao_normalizada_temp").is_in(desc_norm)
            ).drop("descricao_normalizada_temp")
            padrao = calcular_atributos_padrao(df_base_filtered)
            descr_fallback = self._primeira_descricao_por_chaves(df_prod, chaves)

            df_prod_sel = df_prod.filter(pl.col("chave_item").is_in(chaves))

            lista_co_sefin = (
                df_prod_sel
                .explode("lista_co_sefin")
                .drop_nulls("lista_co_sefin")
                .select(pl.col("lista_co_sefin").cast(pl.String).alias("co"))
                .unique()
                .sort("co")
                .get_column("co")
                .to_list()
            )

            lista_unidades = (
                df_prod_sel
                .explode("lista_unid")
                .drop_nulls("lista_unid")
                .select(pl.col("lista_unid").cast(pl.String).alias("u"))
                .unique()
                .sort("u")
                .get_column("u")
                .to_list()
            )
            lista_fontes: list[str] = []
            if "fontes" in df_base_filtered.columns:
                lista_fontes = (
                    df_base_filtered
                    .explode("fontes")
                    .drop_nulls("fontes")
                    .select(pl.col("fontes").cast(pl.Utf8, strict=False).str.strip_chars().alias("fonte"))
                    .filter(pl.col("fonte") != "")
                    .unique()
                    .sort("fonte")
                    .get_column("fonte")
                    .to_list()
                )
            if not lista_fontes and "fonte" in df_base_filtered.columns:
                lista_fontes = (
                    df_base_filtered
                    .select(
                        pl.when(pl.col("fonte").is_not_null())
                        .then(pl.col("fonte").cast(pl.Utf8, strict=False).str.strip_chars())
                        .otherwise(pl.lit(""))
                        .alias("fonte")
                    )
                    .filter(pl.col("fonte") != "")
                    .unique()
                    .sort("fonte")
                    .get_column("fonte")
                    .to_list()
                )
            if not lista_fontes and "fontes" in df_prod_sel.columns:
                lista_fontes = (
                    df_prod_sel
                    .explode("fontes")
                    .drop_nulls("fontes")
                    .select(pl.col("fontes").cast(pl.Utf8, strict=False).str.strip_chars().alias("fonte"))
                    .filter(pl.col("fonte") != "")
                    .unique()
                    .sort("fonte")
                    .get_column("fonte")
                    .to_list()
                )

            row["descr_padrao"] = padrao.get("descr_padrao") or row.get("descr_padrao") or descr_fallback
            row["ncm_padrao"] = padrao.get("ncm_padrao")
            row["cest_padrao"] = padrao.get("cest_padrao")
            row["gtin_padrao"] = padrao.get("gtin_padrao")
            row["lista_ncm"] = self._coletar_lista_coluna(df_prod_sel, "lista_ncm") or self._coletar_lista_coluna(df_base_filtered, "ncm")
            row["lista_cest"] = self._coletar_lista_coluna(df_prod_sel, "lista_cest") or self._coletar_lista_coluna(df_base_filtered, "cest")
            row["lista_gtin"] = self._coletar_lista_coluna(df_prod_sel, "lista_gtin") or self._coletar_lista_coluna(df_base_filtered, "gtin")
            row["lista_descricoes"] = sorted(
                set(
                    self._coletar_lista_coluna(df_prod_sel, "descricao")
                    + self._coletar_lista_coluna(df_prod_sel, "lista_desc_compl")
                )
            ) or self._coletar_lista_coluna(df_base_filtered, "descricao")
            row["co_sefin_padrao"] = padrao.get("co_sefin_padrao")
            row["co_sefin_agr"] = ", ".join(sorted([str(s) for s in lista_co_sefin]))
            row["lista_co_sefin"] = lista_co_sefin
            row["lista_unidades"] = lista_unidades
            row["co_sefin_divergentes"] = len(lista_co_sefin) > 1
            row["fontes"] = lista_fontes
            registros.append(row)

        df_novo = pl.DataFrame(registros, schema=df_agrup.schema)
        df_novo.drop("lista_chave_produto", strict=False).write_parquet(path_agrup)
        contexto_base = {"cnpj": cnpj, "fluxo": "recalcular_todos_padroes"}
        if reprocessar_referencias:
            self._executar_etapa_tempo("produtos_final", lambda: self.recalcular_produtos_final(cnpj), progresso, contexto=contexto_base)
            self._executar_etapa_tempo(
                "referencias_produtos",
                lambda: self.recalcular_referencias_produtos(cnpj, progresso=progresso, reset_timings=False),
                progresso,
                contexto=contexto_base,
            )
        self._registrar_tempo("recalcular_todos_padroes_total", perf_counter() - inicio_total, progresso, contexto=contexto_base)
        return True

    def recalcular_valores_totais(
        self,
        cnpj: str,
        progresso=None,
        reprocessar_referencias: bool = True,
        reset_timings: bool = True,
    ) -> bool:
        """
        Recalcula totais e precos medios de compras/vendas por grupo e persiste em produtos_agrupados.
        """
        if reset_timings:
            self.ultimo_tempo_etapas = {}
        inicio_total = perf_counter()
        path_agrup = self.caminho_tabela_agregadas(cnpj)
        path_prod = self.caminho_tabela_base(cnpj)
        path_base = self.caminho_itens_unidades(cnpj)
        if not path_agrup.exists() or not path_prod.exists() or not path_base.exists():
            return False

        df_agrup = self.carregar_tabela_agregadas(cnpj)
        df_prod = self._padronizar_chaves_prod(
            self._ler_parquet_colunas(path_prod, ["id_descricao", "chave_item", "chave_produto", "descricao_normalizada"])
        )
        df_base = self._ler_parquet_colunas(
            path_base,
            ["descricao", "compras", "qtd_compras", "vendas", "qtd_vendas"],
        )
        colunas_metricas = self._colunas_metricas_agregacao()

        if (
            df_agrup.is_empty()
            or "lista_chave_produto" not in df_agrup.columns
            or df_prod.is_empty()
            or "chave_item" not in df_prod.columns
            or "descricao_normalizada" not in df_prod.columns
            or df_base.is_empty()
            or "descricao" not in df_base.columns
        ):
            if "id_agrupado" in df_agrup.columns:
                df_metricas = (
                    df_agrup
                    .select("id_agrupado")
                    .with_columns(
                        [
                            pl.lit(0.0).alias("total_compras"),
                            pl.lit(0.0).alias("qtd_compras_total"),
                            pl.lit(None, dtype=pl.Float64).alias("preco_medio_compra"),
                            pl.lit(0.0).alias("total_vendas"),
                            pl.lit(0.0).alias("qtd_vendas_total"),
                            pl.lit(None, dtype=pl.Float64).alias("preco_medio_venda"),
                            pl.lit(0.0).alias("total_entradas"),
                            pl.lit(0.0).alias("total_saidas"),
                            pl.lit(0.0).alias("total_movimentacao"),
                        ]
                    )
                )
            else:
                df_metricas = pl.DataFrame(schema={"id_agrupado": pl.Utf8, **{col: pl.Float64 for col in colunas_metricas}})
        else:
            df_desc_por_grupo = (
                df_agrup
                .select(["id_agrupado", "lista_chave_produto"])
                .explode("lista_chave_produto")
                .drop_nulls("lista_chave_produto")
                .rename({"lista_chave_produto": "chave_item"})
                .with_columns(pl.col("chave_item").cast(pl.Utf8, strict=False))
                .join(
                    df_prod.select(
                        [
                            pl.col("chave_item").cast(pl.Utf8, strict=False),
                            pl.col("descricao_normalizada").cast(pl.Utf8, strict=False),
                        ]
                    ),
                    on="chave_item",
                    how="left",
                )
                .drop_nulls("descricao_normalizada")
                .unique(subset=["id_agrupado", "descricao_normalizada"])
            )

            df_base_agregado = (
                df_base
                .with_columns(
                    [
                        pl.col("descricao")
                        .map_elements(self._normalizar_descricao_para_match, return_dtype=pl.String)
                        .alias("descricao_normalizada"),
                        pl.col("compras").cast(pl.Float64, strict=False).fill_null(0.0).alias("compras"),
                        pl.col("qtd_compras").cast(pl.Float64, strict=False).fill_null(0.0).alias("qtd_compras"),
                        pl.col("vendas").cast(pl.Float64, strict=False).fill_null(0.0).alias("vendas"),
                        pl.col("qtd_vendas").cast(pl.Float64, strict=False).fill_null(0.0).alias("qtd_vendas"),
                    ]
                )
                .group_by("descricao_normalizada")
                .agg(
                    [
                        pl.col("compras").sum().alias("total_compras"),
                        pl.col("qtd_compras").sum().alias("qtd_compras_total"),
                        pl.col("vendas").sum().alias("total_vendas"),
                        pl.col("qtd_vendas").sum().alias("qtd_vendas_total"),
                    ]
                )
            )

            df_metricas = (
                df_desc_por_grupo
                .join(df_base_agregado, on="descricao_normalizada", how="left")
                .with_columns(
                    [
                        pl.col("total_compras").fill_null(0.0),
                        pl.col("qtd_compras_total").fill_null(0.0),
                        pl.col("total_vendas").fill_null(0.0),
                        pl.col("qtd_vendas_total").fill_null(0.0),
                    ]
                )
                .group_by("id_agrupado")
                .agg(
                    [
                        pl.col("total_compras").sum().alias("total_compras"),
                        pl.col("qtd_compras_total").sum().alias("qtd_compras_total"),
                        pl.col("total_vendas").sum().alias("total_vendas"),
                        pl.col("qtd_vendas_total").sum().alias("qtd_vendas_total"),
                    ]
                )
                .with_columns(
                    [
                        pl.when(pl.col("qtd_compras_total") > 0)
                        .then(pl.col("total_compras") / pl.col("qtd_compras_total"))
                        .otherwise(None)
                        .alias("preco_medio_compra"),
                        pl.when(pl.col("qtd_vendas_total") > 0)
                        .then(pl.col("total_vendas") / pl.col("qtd_vendas_total"))
                        .otherwise(None)
                        .alias("preco_medio_venda"),
                        pl.col("total_compras").alias("total_entradas"),
                        pl.col("total_vendas").alias("total_saidas"),
                        (pl.col("total_compras") + pl.col("total_vendas")).alias("total_movimentacao"),
                    ]
                )
            )

        df_agrup = (
            df_agrup
            .drop(colunas_metricas, strict=False)
            .join(df_metricas, on="id_agrupado", how="left")
            .with_columns(
                [
                    pl.col("total_compras").cast(pl.Float64, strict=False).fill_null(0.0),
                    pl.col("qtd_compras_total").cast(pl.Float64, strict=False).fill_null(0.0),
                    pl.col("total_vendas").cast(pl.Float64, strict=False).fill_null(0.0),
                    pl.col("qtd_vendas_total").cast(pl.Float64, strict=False).fill_null(0.0),
                    pl.col("preco_medio_compra").cast(pl.Float64, strict=False),
                    pl.col("preco_medio_venda").cast(pl.Float64, strict=False),
                    pl.col("total_entradas").cast(pl.Float64, strict=False).fill_null(0.0),
                    pl.col("total_saidas").cast(pl.Float64, strict=False).fill_null(0.0),
                    pl.col("total_movimentacao").cast(pl.Float64, strict=False).fill_null(0.0),
                ]
            )
        )

        df_agrup.drop("lista_chave_produto", strict=False).write_parquet(path_agrup)
        contexto_base = {"cnpj": cnpj, "fluxo": "recalcular_valores_totais"}
        if reprocessar_referencias:
            self._executar_etapa_tempo("produtos_final", lambda: self.recalcular_produtos_final(cnpj), progresso, contexto=contexto_base)
            self._executar_etapa_tempo(
                "referencias_produtos",
                lambda: self.recalcular_referencias_produtos(cnpj, progresso=progresso, reset_timings=False),
                progresso,
                contexto=contexto_base,
            )
        self._registrar_tempo("recalcular_totais_total", perf_counter() - inicio_total, progresso, contexto=contexto_base)
        return True

    def reprocessar_agregacao(self, cnpj: str, progresso=None) -> bool:
        """
        Reprocessa toda a cadeia da agregacao em uma unica operacao:
        - padroes dos grupos
        - totais de compras/vendas
        - produtos_final
        - fontes *_agr
        - precos medios por unidade agregada
        - fatores de conversao
        - c170_xml / c176_xml
        - mov_estoque
        - aba_mensal
        - aba_anual
        """
        self.ultimo_tempo_etapas = {}
        inicio_total = perf_counter()
        contexto_base = {"cnpj": cnpj, "fluxo": "reprocessar_agregacao"}

        if calcular_fatores_conversao is None:
            raise ImportError("Nao foi possivel importar fatores_conversao.py.")
        if gerar_c170_xml is None:
            raise ImportError("Nao foi possivel importar c170_xml.py.")
        if gerar_c176_xml is None:
            raise ImportError("Nao foi possivel importar c176_xml.py.")
        if gerar_movimentacao_estoque is None:
            raise ImportError("Nao foi possivel importar movimentacao_estoque.py.")
        if gerar_calculos_mensais is None:
            raise ImportError("Nao foi possivel importar calculos_mensais.py.")
        if gerar_calculos_anuais is None:
            raise ImportError("Nao foi possivel importar calculos_anuais.py.")

        ok_padroes = bool(
            self.recalcular_todos_padroes(
                cnpj,
                progresso=progresso,
                reprocessar_referencias=False,
                reset_timings=False,
            )
        )
        ok_totais = bool(
            self.recalcular_valores_totais(
                cnpj,
                progresso=progresso,
                reprocessar_referencias=False,
                reset_timings=False,
            )
        ) if ok_padroes else False
        ok_final = self._executar_etapa_tempo("produtos_final", lambda: self.recalcular_produtos_final(cnpj), progresso, contexto=contexto_base) if ok_totais else False
        ok_fontes = self._executar_etapa_tempo("fontes_agr", lambda: self.refazer_tabelas_agr(cnpj), progresso, contexto=contexto_base) if ok_final else False
        ok_precos = self._executar_etapa_tempo(
            "precos_medios_produtos_final",
            lambda: bool(calcular_precos_medios_produtos_final(cnpj)) if calcular_precos_medios_produtos_final is not None else True,
            progresso,
            contexto=contexto_base,
        ) if ok_fontes else False
        ok_fatores = self._executar_etapa_tempo("fatores_conversao", lambda: bool(calcular_fatores_conversao(cnpj)), progresso, contexto=contexto_base) if ok_precos else False
        ok_c170 = self._executar_etapa_tempo("c170_xml", lambda: bool(gerar_c170_xml(cnpj)), progresso, contexto=contexto_base) if ok_fatores else False
        ok_c176 = self._executar_etapa_tempo("c176_xml", lambda: bool(gerar_c176_xml(cnpj)), progresso, contexto=contexto_base) if ok_c170 else False
        ok_mov = self._executar_etapa_tempo("mov_estoque", lambda: bool(gerar_movimentacao_estoque(cnpj)), progresso, contexto=contexto_base) if ok_c176 else False
        ok_mensal = self._executar_etapa_tempo("calculos_mensais", lambda: bool(gerar_calculos_mensais(cnpj)), progresso, contexto=contexto_base) if ok_mov else False
        ok_anual = self._executar_etapa_tempo("calculos_anuais", lambda: bool(gerar_calculos_anuais(cnpj)), progresso, contexto=contexto_base) if ok_mensal else False

        ok_total = bool(ok_padroes and ok_totais and ok_final and ok_fontes and ok_precos and ok_fatores and ok_c170 and ok_c176 and ok_mov and ok_mensal and ok_anual)
        self._registrar_tempo(
            "reprocessar_agregacao_total",
            perf_counter() - inicio_total,
            progresso,
            contexto={**contexto_base, "sucesso": ok_total},
        )
        return ok_total
