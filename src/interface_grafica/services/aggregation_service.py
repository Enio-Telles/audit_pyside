import os

try:
    import psutil
except ImportError:
    psutil = None
import json
import re
from pathlib import Path
from time import perf_counter
import shutil
from datetime import datetime

import polars as pl
from utilitarios.perf_monitor import registrar_evento_performance
from utilitarios.text import expr_normalizar_descricao

from interface_grafica.config import CNPJ_ROOT


try:
    from transformacao.rastreabilidade_produtos.produtos_agrupados import (
        calcular_atributos_padrao,
    )
except ImportError:
    calcular_atributos_padrao = None


try:
    from transformacao.produtos_final_v2 import (
        produtos_agrupados as inicializar_produtos_agrupados,
    )
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
    from transformacao.precos_medios_produtos_final import (
        calcular_precos_medios_produtos_final,
    )
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

try:
    from transformacao.calculos_periodo_pkg import gerar_calculos_periodos
except ImportError:
    gerar_calculos_periodos = None

try:
    from transformacao.ressarcimento_st_pkg import (
        executar_pipeline_ressarcimento_st as gerar_ressarcimento_st,
    )
except ImportError:
    gerar_ressarcimento_st = None

try:
    from transformacao.resumo_global import gerar_aba_resumo_global
except ImportError:
    gerar_aba_resumo_global = None

try:
    from transformacao.produtos_selecionados import gerar_aba_produtos_selecionados
except ImportError:
    gerar_aba_produtos_selecionados = None

try:
    from utilitarios.salvar_para_parquet import salvar_para_parquet
except ImportError:
    salvar_para_parquet = None

from utilitarios.compat import ensure_id_aliases


class ServicoAgregacao:
    # Cache global de DataFrames por CNPJ
    _global_df_cache: dict[str, dict[Path, pl.DataFrame]] = {}

    def _ler_parquet_com_cache(
        self,
        path: Path,
        cache: dict,
        nome: str,
        progresso=None,
        cnpj: str = None,
        tentar_reprocessar=True,
    ) -> pl.DataFrame:
        """Lê Parquet com cache em memória e logs de tempo/tamanho."""

        def meminfo():
            if psutil is not None:
                proc = psutil.Process(os.getpid())
                mem = proc.memory_info().rss / (1024 * 1024)
                return f"{mem:.1f} MB"
            return "psutil N/A"

        # Cache global por CNPJ
        if cnpj is not None:
            gcache = self._global_df_cache.setdefault(cnpj, {})
            if path in gcache:
                if progresso:
                    progresso(f"[CACHE][GLOBAL] {nome}: {path.name} | Mem: {meminfo()}")
                return gcache[path]

        if path in cache:
            if progresso:
                progresso(f"[CACHE][LOCAL] {nome}: {path.name} | Mem: {meminfo()}")
            return cache[path]

        if not path.exists():
            if progresso:
                progresso(f"[ERRO] Arquivo não encontrado: {path}")
            raise FileNotFoundError(f"Arquivo não encontrado: {path}")

        try:
            if progresso:
                progresso(f"[MEM] Antes de ler {nome}: {meminfo()}")
            t0 = perf_counter()
            df = pl.read_parquet(path)
            t1 = perf_counter()
            cache[path] = df
            if cnpj is not None:
                self._global_df_cache[cnpj][path] = df
            if progresso:
                progresso(
                    f"[LOAD] {nome}: {path.name} | {df.height} linhas, {df.width} cols | {t1-t0:.2f}s | Mem: {meminfo()}"
                )
            return df
        except Exception as exc:
            if progresso:
                progresso(f"[ERRO] Falha ao ler {nome}: {exc}")
            # Remove do cache se corrompido
            if cnpj is not None:
                self._global_df_cache[cnpj].pop(path, None)
            cache.pop(path, None)
            # Tenta reprocessar automaticamente se permitido
            if tentar_reprocessar:
                if progresso:
                    progresso(f"[INFO] Tentando reprocessar {nome} automaticamente...")
                ok = self._tentar_reprocessar_parquet(path, nome, cnpj, progresso)
                if ok:
                    # Tenta ler novamente, mas não recursivo
                    return self._ler_parquet_com_cache(
                        path, cache, nome, progresso, cnpj, tentar_reprocessar=False
                    )
                else:
                    if progresso:
                        progresso(
                            f"[ERRO] Falha ao reprocessar {nome}. Aborte apenas esta etapa."
                        )
            raise

    def _tentar_reprocessar_parquet(
        self, path: Path, nome: str, cnpj: str, progresso=None
    ) -> bool:
        """Tenta reprocessar o arquivo Parquet corrompido, se possível."""
        # Mapear nome para função de geração
        funcoes = {
            "item_unidades": getattr(self, "_regerar_item_unidades", None),
            "descricao_produtos": getattr(self, "_regerar_descricao_produtos", None),
            "produtos_final": getattr(self, "_regerar_produtos_final", None),
            "produtos_agrupados": getattr(self, "_regerar_produtos_agrupados", None),
        }
        func = funcoes.get(nome)
        if func is not None:
            try:
                return func(cnpj, progresso)
            except Exception as exc:
                if progresso:
                    progresso(f"[ERRO] Falha ao reprocessar {nome}: {exc}")
                return False
        if progresso:
            progresso(
                f"[ERRO] Não há função de reprocessamento automático para {nome}."
            )
        return False

    def _regerar_item_unidades(self, cnpj, progresso=None):
        if progresso:
            progresso("[REPROCESSANDO] item_unidades...")
        # Chame a função real do pipeline aqui
        try:
            # Prefer the compat proxy which re-exports the real implementation
            from transformacao.item_unidades import item_unidades

            ok = bool(item_unidades(cnpj))
            if progresso:
                progresso(f"[OK] item_unidades reprocessado: {ok}")
            return ok
        except ImportError:
            if progresso:
                progresso("[ERRO] Módulo transformacao.item_unidades não disponível.")
            return False
        except Exception as exc:
            if progresso:
                progresso(f"[ERRO] Falha ao reprocessar item_unidades: {exc}")
            return False

    def _regerar_descricao_produtos(self, cnpj, progresso=None):
        if progresso:
            progresso("[REPROCESSANDO] descricao_produtos...")
        try:
            # Use the compatibility proxy which points to the canonical implementation
            from transformacao.descricao_produtos import descricao_produtos

            ok = bool(descricao_produtos(cnpj))
            if progresso:
                progresso(f"[OK] descricao_produtos reprocessado: {ok}")
            return ok
        except ImportError:
            if progresso:
                progresso("[ERRO] Módulo transformacao.descricao_produtos não disponível.")
            return False
        except Exception as exc:
            if progresso:
                progresso(f"[ERRO] Falha ao reprocessar descricao_produtos: {exc}")
            return False

    def _regerar_produtos_final(self, cnpj, progresso=None):
        if progresso:
            progresso("[REPROCESSANDO] produtos_final...")
        try:
            # Try the legacy proxy which should call the v2 generator
            try:
                from transformacao.produtos_final import gerar_produtos_final
            except Exception:
                gerar_produtos_final = None

            if gerar_produtos_final is not None:
                ok = bool(gerar_produtos_final(cnpj))
                if progresso:
                    progresso(f"[OK] produtos_final reprocessado via gerar_produtos_final: {ok}")
                return ok

            # Fallbacks: attempt available helpers imported at module level
            if inicializar_produtos_agrupados is not None:
                ok = bool(inicializar_produtos_agrupados(cnpj))
                if progresso:
                    progresso(f"[OK] produtos_final reprocessado via inicializar_produtos_agrupados: {ok}")
                return ok

            if progresso:
                progresso("[ERRO] Nenhuma função disponível para gerar produtos_final.")
            return False
        except Exception as exc:
            if progresso:
                progresso(f"[ERRO] Falha ao reprocessar produtos_final: {exc}")
            return False

    def _regerar_produtos_agrupados(self, cnpj, progresso=None):
        if progresso:
            progresso("[REPROCESSANDO] produtos_agrupados...")
        try:
            # Prefer the initializer imported from produtos_final_v2 if present
            if inicializar_produtos_agrupados is not None:
                ok = bool(inicializar_produtos_agrupados(cnpj))
                if progresso:
                    progresso(f"[OK] produtos_agrupados reprocessado: {ok}")
                return ok

            # As a fallback, try the legacy gerar_produtos_final
            try:
                from transformacao.produtos_final import gerar_produtos_final
            except Exception:
                gerar_produtos_final = None

            if gerar_produtos_final is not None:
                ok = bool(gerar_produtos_final(cnpj))
                if progresso:
                    progresso(f"[OK] produtos_agrupados reprocessado via gerar_produtos_final: {ok}")
                return ok

            if progresso:
                progresso("[ERRO] Nenhuma função disponível para gerar produtos_agrupados.")
            return False
        except Exception as exc:
            if progresso:
                progresso(f"[ERRO] Falha ao reprocessar produtos_agrupados: {exc}")
            return False

    """
    Gerencia a tabela produtos_agrupados e as derivacoes da camada _agr.
    """

    def _tabelas_base_existem(self, cnpj: str) -> bool:
        pasta = CNPJ_ROOT / cnpj / "analises" / "produtos"
        reqs = [
            pasta / f"item_unidades_{cnpj}.parquet",
            pasta / f"descricao_produtos_{cnpj}.parquet",
            pasta / f"produtos_final_{cnpj}.parquet",
        ]
        return all(p.exists() for p in reqs)

    def __init__(self) -> None:
        self.ultimo_tempo_etapas: dict[str, float] = {}

    def _registrar_tempo(
        self, nome: str, duracao: float, progresso=None, contexto: dict | None = None
    ) -> None:
        self.ultimo_tempo_etapas[nome] = duracao
        registrar_evento_performance(
            f"aggregation_service.{nome}", duracao, contexto or {}
        )
        if progresso:
            progresso(f"OK {nome} em {duracao:.2f}s")

    def _executar_etapa_tempo(
        self, nome: str, funcao, progresso=None, contexto: dict | None = None
    ):
        def meminfo():
            try:
                import os
                import psutil

                proc = psutil.Process(os.getpid())
                mem = proc.memory_info().rss / (1024 * 1024)
                return f"{mem:.1f} MB"
            except Exception:
                return "psutil N/A"

        if progresso:
            progresso(f"Iniciando {nome}... | Mem: {meminfo()}")
        inicio = perf_counter()
        resultado = funcao()
        if progresso:
            progresso(f"Finalizou {nome} | Mem: {meminfo()}")
        self._registrar_tempo(
            nome, perf_counter() - inicio, progresso, contexto=contexto
        )
        return resultado

    def resumo_tempos(self) -> str:
        if not self.ultimo_tempo_etapas:
            return ""
        return " | ".join(
            f"{nome}: {duracao:.2f}s"
            for nome, duracao in self.ultimo_tempo_etapas.items()
        )

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
        _pasta_ressarc = pasta_prod.parent / "ressarcimento_st"
        # Consider every analytical table derived from mov_estoque so manual
        # regrouping is reflected consistently across monthly, annual and
        # period views.
        derivados = {
            "calculos_mensais": pasta_prod / f"aba_mensal_{cnpj}.parquet",
            "calculos_anuais": pasta_prod / f"aba_anual_{cnpj}.parquet",
            "calculos_periodos": pasta_prod / f"aba_periodos_{cnpj}.parquet",
        }
        return [
            etapa
            for etapa, caminho in derivados.items()
            if not caminho.exists() or caminho.stat().st_mtime_ns <= mov_mtime
        ]

    @staticmethod
    def _expr_normalizar_descricao(coluna: str) -> pl.Expr:
        return expr_normalizar_descricao(coluna)

    @staticmethod
    def _primeira_descricao_por_chaves(
        df_prod: pl.DataFrame, chaves: list[str]
    ) -> str | None:
        if not chaves:
            return None
        df_desc = (
            df_prod.filter(pl.col("chave_item").is_in(chaves))
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
        cols = (
            df.collect_schema().names() if isinstance(df, pl.LazyFrame) else df.columns
        )
        if "lista_co_sefin" in cols:
            casts.append(
                pl.col("lista_co_sefin")
                .cast(pl.List(pl.Utf8), strict=False)
                .alias("lista_co_sefin")
            )
        if "co_sefin_padrao" in cols:
            casts.append(
                pl.col("co_sefin_padrao")
                .cast(pl.Utf8, strict=False)
                .alias("co_sefin_padrao")
            )
        for col in ["ncm_padrao", "cest_padrao", "gtin_padrao"]:
            if col in cols:
                casts.append(pl.col(col).cast(pl.Utf8, strict=False).alias(col))
        for col in [
            "lista_ncm",
            "lista_cest",
            "lista_gtin",
            "lista_descricoes",
            "lista_desc_compl",
        ]:
            if col in cols:
                casts.append(
                    pl.col(col).cast(pl.List(pl.Utf8), strict=False).alias(col)
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
        return [
            str(item).strip()
            for item in itens
            if item is not None and str(item).strip()
        ]

    @classmethod
    def _coletar_lista_coluna(cls, df: pl.DataFrame, coluna: str) -> list[str]:
        if df.is_empty() or coluna not in df.columns:
            return []

        series = df.get_column(coluna)
        if series.dtype.is_nested():
            series = series.explode()

        series_clean = series.cast(pl.Utf8, strict=False).str.strip_chars().drop_nulls()

        return series_clean.filter(series_clean != "").unique().sort().to_list()

    @staticmethod
    def _deduplicar_preservando_ordem(valores: list[str]) -> list[str]:
        vistos: set[str] = set()
        saida: list[str] = []
        for valor in valores:
            texto = str(valor).strip() if valor is not None else ""
            if not texto or texto in vistos:
                continue
            vistos.add(texto)
            saida.append(texto)
        return saida

    @classmethod
    def _normalizar_lista_ids_agrupados(
        cls, valor, id_padrao: str | None = None
    ) -> list[str]:
        ids = cls._normalizar_lista_textos(valor)
        if not ids and id_padrao:
            ids = [str(id_padrao).strip()]
        return cls._deduplicar_preservando_ordem(ids)

    @classmethod
    def _garantir_rastreabilidade_agregacao(cls, df: pl.DataFrame) -> pl.DataFrame:
        cols = (
            df.collect_schema().names() if isinstance(df, pl.LazyFrame) else df.columns
        )
        if df.is_empty() or "id_agrupado" not in cols:
            return df

        expressoes: list[pl.Expr] = []
        if "ids_origem_agrupamento" not in cols:
            expressoes.append(
                pl.concat_list(
                    [pl.col("id_agrupado").cast(pl.Utf8, strict=False)]
                ).alias("ids_origem_agrupamento")
            )
        else:
            expressoes.append(
                pl.col("ids_origem_agrupamento")
                .cast(pl.List(pl.Utf8), strict=False)
                .alias("ids_origem_agrupamento")
            )

        if "lista_itens_agrupados" not in cols:
            if "lista_descricoes" in cols:
                expressoes.append(
                    pl.col("lista_descricoes")
                    .cast(pl.List(pl.Utf8), strict=False)
                    .alias("lista_itens_agrupados")
                )
            else:
                expressoes.append(
                    pl.lit([])
                    .cast(pl.List(pl.Utf8), strict=False)
                    .alias("lista_itens_agrupados")
                )
        else:
            expressoes.append(
                pl.col("lista_itens_agrupados")
                .cast(pl.List(pl.Utf8), strict=False)
                .alias("lista_itens_agrupados")
            )

        df_rastreavel = df.with_columns(expressoes)

        # Materializar e normalizar por linha para garantir ordem deterministica
        # e deduplicacao preservando a primeira ocorrencia.
        ids_col = (
            df_rastreavel.get_column("ids_origem_agrupamento").to_list()
            if "ids_origem_agrupamento" in df_rastreavel.columns
            else [None] * df_rastreavel.height
        )
        id_agrupados_col = df_rastreavel.get_column("id_agrupado").to_list()

        normalized_ids: list[list[str]] = []
        for orig, id_agr in zip(ids_col, id_agrupados_col, strict=True):
            if orig is None:
                normalized = [str(id_agr).strip()]
            else:
                # Orig can be nested Series or lists.
                if isinstance(orig, pl.Series):
                    items = orig.to_list()
                elif isinstance(orig, (list, tuple)):
                    items = list(orig)
                else:
                    items = [orig]
                # Normalize strings and drop empties
                cleaned = [
                    str(x).strip() for x in items if x is not None and str(x).strip()
                ]
                cleaned = cls._deduplicar_preservando_ordem(cleaned)
                normalized = cleaned if cleaned else [str(id_agr).strip()]
            normalized_ids.append(normalized)

        # Reanexar colunas normalizadas
        df_rastreavel = df_rastreavel.with_columns(
            [
                pl.Series(normalized_ids).alias("ids_origem_agrupamento"),
                pl.col("lista_itens_agrupados")
                .cast(pl.List(pl.Utf8), strict=False)
                .alias("lista_itens_agrupados"),
            ]
        )

        return df_rastreavel

    @staticmethod
    def _garantir_colunas_lista_agregacao(df: pl.DataFrame) -> pl.DataFrame:
        exprs = []
        cols = (
            df.collect_schema().names() if isinstance(df, pl.LazyFrame) else df.columns
        )
        for coluna in [
            "lista_ncm",
            "lista_cest",
            "lista_gtin",
            "lista_descricoes",
            "lista_desc_compl",
            "lista_codigo_fonte",
            "lista_codigos",
            "ids_origem_agrupamento",
            "lista_itens_agrupados",
        ]:
            if coluna not in cols:
                exprs.append(
                    pl.lit([]).cast(pl.List(pl.Utf8), strict=False).alias(coluna)
                )
        # M3: Garantir versao_agrupamento
        if "versao_agrupamento" not in cols:
            exprs.append(pl.lit(1).cast(pl.Int64).alias("versao_agrupamento"))
        df_saida = df.with_columns(exprs) if exprs else df
        return ServicoAgregacao._garantir_rastreabilidade_agregacao(df_saida)

    @staticmethod
    def _padronizar_chaves_prod(df: pl.DataFrame) -> pl.DataFrame:
        """
        Define chave_item como chave canonica interna.
        Mantem chave_produto como coluna legado.
        """
        try:
            cols = df.collect_schema().names()
        except (AttributeError, TypeError):
            cols = df.columns

        updates = []

        if "chave_item" not in cols and "id_descricao" in cols:
            updates.append(pl.col("id_descricao").alias("chave_item"))
        if "chave_item" not in cols and "chave_produto" in cols:
            updates.append(pl.col("chave_produto").alias("chave_item"))

        if "chave_produto" not in cols and "id_descricao" in cols:
            updates.append(pl.col("id_descricao").alias("chave_produto"))
        if "chave_produto" not in cols and "chave_item" in cols:
            updates.append(pl.col("chave_item").alias("chave_produto"))

        if updates:
            df = df.with_columns(updates)
        return df

    def caminho_tabela_agregadas(self, cnpj: str) -> Path:
        return (
            CNPJ_ROOT
            / cnpj
            / "analises"
            / "produtos"
            / f"produtos_agrupados_{cnpj}.parquet"
        )

    def caminho_tabela_editavel(self, cnpj: str) -> Path:
        """
        A tabela superior da aba de agregacao deve usar a mesma base da
        tabela inferior: produtos_agrupados, sempre com id_agrupado.
        """
        return self.caminho_tabela_agregadas(cnpj)

    def caminho_tabela_base(self, cnpj: str) -> Path:
        return (
            CNPJ_ROOT
            / cnpj
            / "analises"
            / "produtos"
            / f"descricao_produtos_{cnpj}.parquet"
        )

    def caminho_itens_unidades(self, cnpj: str) -> Path:
        return (
            CNPJ_ROOT / cnpj / "analises" / "produtos" / f"item_unidades_{cnpj}.parquet"
        )

    def caminho_log_agregacoes(self, cnpj: str) -> Path:
        return (
            CNPJ_ROOT / cnpj / "analises" / "produtos" / f"log_agregacoes_{cnpj}.json"
        )

    def caminho_tabela_final(self, cnpj: str) -> Path:
        return (
            CNPJ_ROOT
            / cnpj
            / "analises"
            / "produtos"
            / f"produtos_final_{cnpj}.parquet"
        )

    def caminho_tabela_id_agrupados(self, cnpj: str) -> Path:
        return (
            CNPJ_ROOT / cnpj / "analises" / "produtos" / f"id_agrupados_{cnpj}.parquet"
        )

    @staticmethod
    def _ler_parquet_colunas(path: Path, colunas: list[str]) -> pl.DataFrame:
        if not path.exists():
            return pl.DataFrame()
        schema_cols = pl.read_parquet_schema(path).names()
        selecionadas = [col for col in colunas if col in schema_cols]
        if not selecionadas:
            return pl.DataFrame()
        return pl.read_parquet(path, columns=selecionadas)

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
            "lista_desc_compl",
            "lista_codigo_fonte",
            "lista_codigos",
            "ids_origem_agrupamento",
            "lista_itens_agrupados",
        }

    @staticmethod
    def _alinhar_registros_por_schema(
        registros: list[dict], schema: dict[str, pl.DataType]
    ) -> pl.DataFrame:
        if not registros:
            return pl.DataFrame(schema=schema)

        registros_alinhados: list[dict] = []
        for registro in registros:
            linha: dict = {}
            for coluna, dtype in schema.items():
                valor = registro.get(coluna)
                if valor is None and dtype.is_nested():
                    valor = []
                linha[coluna] = valor
            registros_alinhados.append(linha)
        return pl.DataFrame(registros_alinhados, schema=schema)

    def _salvar_tabela_agregada_e_mapa(
        self, cnpj: str, df_resultado: pl.DataFrame
    ) -> None:
        """
        Salva a tabela mestre e a ponte.

        M3: Incrementa `versao_agrupamento` a cada salvamento.
        R3: Expande a ponte com `codigo_fonte` e `descricao_normalizada`.
        """
        df_resultado = self._garantir_colunas_lista_agregacao(
            self._promover_tipos_sefin(df_resultado)
        )

        cols_res = (
            df_resultado.collect_schema().names()
            if isinstance(df_resultado, pl.LazyFrame)
            else df_resultado.columns
        )

        # M3: Incrementar versao_agrupamento
        if "versao_agrupamento" not in cols_res:
            df_resultado = df_resultado.with_columns(
                pl.lit(1).cast(pl.Int64).alias("versao_agrupamento")
            )
            cols_res = (
                df_resultado.collect_schema().names()
                if isinstance(df_resultado, pl.LazyFrame)
                else df_resultado.columns
            )
        else:
            # Incrementar apenas se nao foi definido explicitamente pelo caller
            max_versao = df_resultado["versao_agrupamento"].max()
            if max_versao is not None and max_versao > 0:
                nova_versao = max_versao + 1
                df_resultado = df_resultado.with_columns(
                    pl.lit(nova_versao).cast(pl.Int64).alias("versao_agrupamento")
                )
            else:
                df_resultado = df_resultado.with_columns(
                    pl.lit(1).cast(pl.Int64).alias("versao_agrupamento")
                )

        df_resultado_sem_chaves = df_resultado.drop("lista_chave_produto", strict=False)
        # Garantir compatibilidade de nomes de id ao persistir (dual-write)
        df_to_write = ensure_id_aliases(df_resultado_sem_chaves)
        df_to_write.write_parquet(self.caminho_tabela_agregadas(cnpj))

        if "lista_chave_produto" in cols_res:
            # R3: Ponte expandida com codigo_fonte e descricao_normalizada
            # Tentar enriquecer ponte com informacoes de produtos_final
            path_final = self.caminho_tabela_final(cnpj)
            path_base = self.caminho_tabela_base(cnpj)

            df_map = (
                df_resultado.select(["id_agrupado", "lista_chave_produto"])
                .explode("lista_chave_produto")
                .rename({"lista_chave_produto": "chave_produto"})
                .drop_nulls("chave_produto")
            )

            # Enriquecer com descricao_normalizada via base
            if path_base.exists():
                schema_base = pl.read_parquet_schema(path_base).names()
                cols_base = [
                    "chave_item" if "chave_item" in schema_base else "id_descricao"
                ]
                if "descricao_normalizada" in schema_base:
                    cols_base.append("descricao_normalizada")

                lf_base_info = pl.scan_parquet(path_base).select(cols_base)
                df_base_info = lf_base_info.collect()
                if (
                    "chave_item" not in df_base_info.columns
                    and "id_descricao" in df_base_info.columns
                ):
                    df_base_info = df_base_info.rename({"id_descricao": "chave_item"})

                if "descricao_normalizada" in df_base_info.columns:
                    df_map = df_map.join(
                        df_base_info,
                        left_on="chave_produto",
                        right_on="chave_item",
                        how="left",
                    )
                    df_map = df_map.drop("chave_item", strict=False)

            # Enriquecer com codigo_fonte via produtos_final
            if path_final.exists():
                schema_final = pl.read_parquet_schema(path_final).names()
                if "id_descricao" in schema_final and "codigo_fonte" in schema_final:
                    lf_cf = pl.scan_parquet(path_final).select(
                        ["id_descricao", "codigo_fonte"]
                    )
                    df_cf = lf_cf.collect()
                    df_map = df_map.join(
                        df_cf,
                        left_on="chave_produto",
                        right_on="id_descricao",
                        how="left",
                    )
                    df_map = df_map.drop("id_descricao", strict=False)

            # Garantir colunas obrigatorias da ponte
            for col in ["codigo_fonte", "descricao_normalizada"]:
                if col not in df_map.columns:
                    df_map = df_map.with_columns(pl.lit(None, dtype=pl.Utf8).alias(col))

            arq_pont = (
                CNPJ_ROOT
                / cnpj
                / "analises"
                / "produtos"
                / f"map_produto_agrupado_{cnpj}.parquet"
            )
            # Garantir alias `id_agregado` na ponte e persistir ambas colunas
            df_map = ensure_id_aliases(df_map)
            df_map = df_map.select(
                [
                    pl.col("chave_produto").cast(pl.Utf8),
                    pl.col("id_agrupado").cast(pl.Utf8),
                    pl.col("id_agregado").cast(pl.Utf8),
                    pl.col("codigo_fonte").cast(pl.Utf8),
                    pl.col("descricao_normalizada").cast(pl.Utf8),
                ]
            )
            df_map.write_parquet(arq_pont)

    def salvar_mapa_manual(
        self, cnpj: str, df_manual: pl.DataFrame, reprocessar: bool = True, progresso=None
    ) -> bool:
        """
        Persiste o mapa manual oficial `mapa_agrupamento_manual_<cnpj>.parquet`.

        Regras:
        - aceita colunas `id_descricao` e/ou `descricao_normalizada` e requer `id_agrupado`.
        - grava arquivo canônico em `analises/produtos` e salva auditoria de linhas sem match.
        - opcionalmente reprocessa `produtos_final` e triggers de referencia.
        """
        cnpj = self._sanitizar_cnpj(cnpj)
        pasta_analises = CNPJ_ROOT / cnpj / "analises" / "produtos"
        caminho_manual = pasta_analises / f"mapa_agrupamento_manual_{cnpj}.parquet"

        if df_manual is None:
            raise ValueError("df_manual is required")

        if not isinstance(df_manual, pl.DataFrame):
            try:
                df_manual = pl.DataFrame(df_manual)
            except Exception as e:
                raise ValueError(f"Cannot convert df_manual to DataFrame: {e}")

        # Keep only relevant columns
        colunas_validas = [c for c in ["id_descricao", "descricao_normalizada", "id_agrupado"] if c in df_manual.columns]
        if "id_agrupado" not in df_manual.columns:
            raise ValueError("Mapa manual deve conter a coluna 'id_agrupado'.")

        df_out = df_manual.select(colunas_validas).with_columns(
            pl.col("id_agrupado").cast(pl.Utf8, strict=False).alias("id_agrupado")
        )

        # Ensure directory exists and save with helper if available
        ok = False
        # Snapshot existing manual map before overwriting
        try:
            self._snapshot_manual_map(cnpj, caminho_manual)
        except Exception:
            # Snapshot failure should not block saving
            pass

        if salvar_para_parquet:
            ok = salvar_para_parquet(df_out, pasta_analises, f"mapa_agrupamento_manual_{cnpj}.parquet")
        else:
            pasta_analises.mkdir(parents=True, exist_ok=True)
            df_out.write_parquet(caminho_manual)
            ok = True

        # Auditoria: registrar linhas do mapa manual que nao correspondem a descricao_produtos
        unmatched_count = 0
        try:
            path_base = self.caminho_tabela_base(cnpj)
            if path_base.exists():
                df_base = pl.read_parquet(path_base)
                df_existentes = df_base.select([c for c in ["id_descricao", "descricao_normalizada"] if c in df_base.columns]).unique()
                cols_join = [c for c in ["id_descricao", "descricao_normalizada"] if c in df_out.columns]
                if cols_join:
                    df_auditoria = df_out.join(df_existentes, on=cols_join, how="anti")
                    if not df_auditoria.is_empty():
                        if salvar_para_parquet:
                            salvar_para_parquet(df_auditoria, pasta_analises, f"auditoria_mapa_agrupamento_manual_sem_match_{cnpj}.parquet")
                        else:
                            df_auditoria.write_parquet(pasta_analises / f"auditoria_mapa_agrupamento_manual_sem_match_{cnpj}.parquet")
                        unmatched_count = df_auditoria.height
        except Exception:
            # Auditoria eh recomendada, mas nao deve impedir a persistencia do mapa manual
            unmatched_count = 0

        # Registrar historico de atualizacao do mapa manual
        try:
            self._registrar_log(
                cnpj,
                {
                    "tipo": "mapa_manual_atualizado",
                    "qtd": int(df_out.height),
                    "qtd_unmatched": int(unmatched_count),
                },
            )
        except Exception:
            pass

        # Opcional: reprocessar derivacoes mais proximas
        if reprocessar:
            try:
                # Recalcula produtos_final que consome o mapa manual
                self.recalcular_produtos_final(cnpj)
            except Exception:
                pass

        return ok

    def _snapshot_manual_map(self, cnpj: str, caminho_manual: Path) -> Path | None:
        """
        Cria um snapshot (copia) do arquivo `mapa_agrupamento_manual_<cnpj>.parquet`
        antes de ser sobrescrito. Retorna o Path do snapshot ou None se
        não havia arquivo anterior.
        """
        cnpj = self._sanitizar_cnpj(cnpj)
        if not caminho_manual.exists():
            return None
        pasta_analises = CNPJ_ROOT / cnpj / "analises" / "produtos"
        snapshots_dir = pasta_analises / "snapshots"
        snapshots_dir.mkdir(parents=True, exist_ok=True)
        ts = datetime.now().strftime("%Y%m%dT%H%M%S")
        snap_name = f"mapa_agrupamento_manual_{cnpj}_{ts}.parquet"
        snap_path = snapshots_dir / snap_name
        try:
            shutil.copyfile(caminho_manual, snap_path)
            try:
                self._registrar_log(
                    cnpj,
                    {"tipo": "mapa_manual_snapshot", "snapshot": snap_path.name},
                )
            except Exception:
                pass
            return snap_path
        except Exception:
            return None

    def limpar_snapshots_mapa_manual(
        self, cnpj: str, keep_last: int = 10, keep_days: int = 180, dry_run: bool = False
    ) -> int:
        """
        Limpa snapshots antigos gerados para `mapa_agrupamento_manual_<cnpj>_*.parquet`.

        - `keep_last`: número mínimo de snapshots a manter (mais recentes)
        - `keep_days`: número de dias de retenção mínima (snapshots mais velhos que
          esse período podem ser removidos)

        Retorna a quantidade de arquivos removidos.
        """
        from datetime import datetime, timedelta

        cnpj = self._sanitizar_cnpj(cnpj)
        pasta_analises = CNPJ_ROOT / cnpj / "analises" / "produtos"
        snapshots_dir = pasta_analises / "snapshots"
        if not snapshots_dir.exists():
            return 0

        # Collect snapshot files matching pattern and sort by modification time (newest first)
        snaps = sorted(
            list(snapshots_dir.glob(f"mapa_agrupamento_manual_{cnpj}_*.parquet")),
            key=lambda p: p.stat().st_mtime,
            reverse=True,
        )

        removed = 0
        threshold = datetime.now() - timedelta(days=keep_days)

        for idx, path in enumerate(snaps):
            # Keep the most recent `keep_last` snapshots
            if idx < keep_last:
                continue
            try:
                mtime = datetime.fromtimestamp(path.stat().st_mtime)
                # Remove only if older than threshold. If keep_days is 0 or
                # negative the threshold logic still applies; callers can use
                # `dry_run=True` to preview removals without deleting files.
                if mtime < threshold:
                    if not dry_run:
                        path.unlink()
                    removed += 1
            except Exception:
                # Ignore individual failures
                continue

        if removed:
            try:
                self._registrar_log(cnpj, {"tipo": "limpeza_snapshots", "removed": int(removed)})
            except Exception:
                pass

        return int(removed)

    def listar_snapshots_mapa_manual(self, cnpj: str) -> list[Path]:
        pasta_analises = CNPJ_ROOT / self._sanitizar_cnpj(cnpj) / "analises" / "produtos"
        snapshots_dir = pasta_analises / "snapshots"
        if not snapshots_dir.exists():
            return []
        snaps = [p for p in snapshots_dir.iterdir() if p.is_file()]
        return sorted(snaps, key=lambda p: p.stat().st_mtime, reverse=True)

    def reverter_mapa_manual(self, cnpj: str, snapshot_name: str | None = None) -> bool:
        """
        Restaura o mapa manual a partir do snapshot especificado ou do mais
        recente se nenhum for informado.
        """
        cnpj = self._sanitizar_cnpj(cnpj)
        pasta_analises = CNPJ_ROOT / cnpj / "analises" / "produtos"
        caminho_manual = pasta_analises / f"mapa_agrupamento_manual_{cnpj}.parquet"
        snapshots_dir = pasta_analises / "snapshots"
        if not snapshots_dir.exists():
            return False
        if snapshot_name:
            snap_path = snapshots_dir / snapshot_name
            if not snap_path.exists():
                raise FileNotFoundError(f"Snapshot nao encontrado: {snapshot_name}")
        else:
            snaps = self.listar_snapshots_mapa_manual(cnpj)
            if not snaps:
                return False
            snap_path = snaps[0]
        try:
            shutil.copyfile(snap_path, caminho_manual)
            try:
                self._registrar_log(cnpj, {"tipo": "mapa_manual_revertido", "snapshot": snap_path.name})
            except Exception:
                pass
            return True
        except Exception:
            return False

    def _regenerar_tabela_agregadas_legada(self, cnpj: str) -> bool:
        """
        Recria a camada de agregacao quando o parquet legado nao possui
        colunas canonicas de descricoes.
        """
        path_agrup = self.caminho_tabela_agregadas(cnpj)
        schema_cols = self._obter_schema_parquet(path_agrup)
        if {"lista_descricoes", "lista_desc_compl"}.issubset(schema_cols):
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
        if not {"lista_descricoes", "lista_desc_compl"}.issubset(schema_regenerado):
            return False

        return not (
            path_id_agrupados.exists()
            and not {
                "lista_descricoes",
                "lista_desc_compl",
            }.issubset(self._obter_schema_parquet(path_id_agrupados))
        )

    def garantir_metricas_tabela_agregadas(self, cnpj: str) -> bool:
        path = self.garantir_tabela_agregadas(cnpj, criar_se_ausente=True)
        if not path.exists():
            return False

        if not self._regenerar_tabela_agregadas_legada(cnpj):
            return False

        schema_cols = self._obter_schema_parquet(path)
        colunas_esperadas = (
            set(self._colunas_metricas_agregacao())
            | self._colunas_descricoes_agregacao()
        )
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

        self.recalcular_valores_totais(
            cnpj, reprocessar_referencias=False, reset_timings=False
        )
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
        arq_pont = (
            CNPJ_ROOT
            / cnpj
            / "analises"
            / "produtos"
            / f"map_produto_agrupado_{cnpj}.parquet"
        )
        if arq_pont.exists() and "lista_chave_produto" not in df_agrup.columns:
            df_pont = pl.read_parquet(arq_pont)
            df_list = df_pont.group_by("id_agrupado").agg(
                pl.col("chave_produto").alias("lista_chave_produto")
            )
            df_agrup = df_agrup.join(df_list, on="id_agrupado", how="left")
        return self._garantir_colunas_lista_agregacao(df_agrup)

    def agregar_linhas(self, cnpj: str, ids_agrupados_selecionados: list[str]) -> dict:
        """
        Une multiplos grupos (linhas de produtos_agrupados) em um so.
        """
        if calcular_atributos_padrao is None:
            raise ImportError("Nao foi possivel importar produtos_agrupados.py.")

        df = self._garantir_colunas_lista_agregacao(
            self._promover_tipos_sefin(
                self.carregar_tabela_agregadas(cnpj, criar_se_ausente=True)
            )
        )
        df_base = self._ler_parquet_colunas(
            self.caminho_itens_unidades(cnpj),
            ["descricao", "ncm", "cest", "gtin", "co_sefin_item", "fontes", "fonte"],
        ).with_columns(
            self._expr_normalizar_descricao("descricao").alias(
                "descricao_normalizada_temp"
            )
        )

        ids_agrupados_selecionados = self._deduplicar_preservando_ordem(
            ids_agrupados_selecionados
        )
        id_destino = ids_agrupados_selecionados[0] if ids_agrupados_selecionados else ""

        df_para_unir = df.filter(
            pl.col("id_agrupado").is_in(ids_agrupados_selecionados)
        )
        df_restante = df.filter(
            ~pl.col("id_agrupado").is_in(ids_agrupados_selecionados)
        )

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
                    "id_descricao",
                    "chave_item",
                    "chave_produto",
                    "descricao_normalizada",
                    "descricao",
                    "lista_desc_compl",
                    "lista_codigo_fonte",
                    "lista_codigos",
                    "lista_ncm",
                    "lista_cest",
                    "lista_gtin",
                    "fontes",
                ],
            )
        )
        df_prod_sel = df_prod_link.filter(pl.col("chave_item").is_in(todas_chaves_proc))
        lista_desc_norm = df_prod_sel["descricao_normalizada"].to_list()

        df_base_filtered = df_base.filter(
            pl.col("descricao_normalizada_temp").is_in(lista_desc_norm)
        ).drop("descricao_normalizada_temp")

        padrao = calcular_atributos_padrao(df_base_filtered)
        descr_fallback = self._primeira_descricao_por_chaves(
            df_prod_link, todas_chaves_proc
        )

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
                df_base_filtered.explode("fontes")
                .drop_nulls("fontes")
                .select(
                    pl.col("fontes")
                    .cast(pl.Utf8, strict=False)
                    .str.strip_chars()
                    .alias("fonte")
                )
                .filter(pl.col("fonte") != "")
                .unique()
                .sort("fonte")
                .get_column("fonte")
                .to_list()
            )
        if not lista_fontes and "fonte" in df_base_filtered.columns:
            lista_fontes = (
                df_base_filtered.select(
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
                df_prod_sel.explode("fontes")
                .drop_nulls("fontes")
                .select(
                    pl.col("fontes")
                    .cast(pl.Utf8, strict=False)
                    .str.strip_chars()
                    .alias("fonte")
                )
                .filter(pl.col("fonte") != "")
                .unique()
                .sort("fonte")
                .get_column("fonte")
                .to_list()
            )
        lista_fontes = sorted(list(set(lista_fontes)))
        lista_ncm = self._coletar_lista_coluna(
            df_prod_sel, "lista_ncm"
        ) or self._coletar_lista_coluna(df_base_filtered, "ncm")
        lista_cest = self._coletar_lista_coluna(
            df_prod_sel, "lista_cest"
        ) or self._coletar_lista_coluna(df_base_filtered, "cest")
        lista_gtin = self._coletar_lista_coluna(
            df_prod_sel, "lista_gtin"
        ) or self._coletar_lista_coluna(df_base_filtered, "gtin")
        lista_descricoes = self._coletar_lista_coluna(
            df_prod_sel, "descricao"
        ) or self._coletar_lista_coluna(df_base_filtered, "descricao")
        lista_desc_compl = self._coletar_lista_coluna(df_prod_sel, "lista_desc_compl")
        lista_codigo_fonte = self._coletar_lista_coluna(df_prod_sel, "lista_codigo_fonte")
        lista_codigos = self._coletar_lista_coluna(df_prod_sel, "lista_codigos")
        lista_itens_agrupados = (
            self._coletar_lista_coluna(df_prod_sel, "descricao") or lista_descricoes
        )

        # Definir `ids_origem_agrupamento` de forma deterministica conforme a selecao
        # do usuário: preserva a ordem apresentada em `ids_agrupados_selecionados`.
        ids_origem_agrupamento = [
            str(x).strip() for x in ids_agrupados_selecionados if str(x).strip()
        ]
        ids_origem_agrupamento = self._deduplicar_preservando_ordem(
            ids_origem_agrupamento
        )

        nova_linha = {
            "id_agrupado": id_destino,
            "lista_chave_produto": todas_chaves_proc,
            "descr_padrao": padrao.get("descr_padrao") or descr_fallback,
            "ncm_padrao": padrao.get("ncm_padrao"),
            "cest_padrao": padrao.get("cest_padrao"),
            "gtin_padrao": padrao.get("gtin_padrao"),
            "lista_ncm": lista_ncm,
            "lista_cest": lista_cest,
            "lista_gtin": lista_gtin,
            "lista_descricoes": lista_descricoes,
            "lista_desc_compl": lista_desc_compl,
            "lista_codigo_fonte": lista_codigo_fonte,
            "lista_codigos": lista_codigos,
            "ids_origem_agrupamento": ids_origem_agrupamento,
            "lista_itens_agrupados": lista_itens_agrupados,
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

        # M3: Preservar versao_agrupamento (sera incrementado em _salvar_tabela_agregada_e_mapa)
        if "versao_agrupamento" in df.columns:
            nova_linha["versao_agrupamento"] = df["versao_agrupamento"].max()

        df_nova = pl.DataFrame([nova_linha], schema=df.schema)
        df_resultado = pl.concat([df_nova, df_restante])
        grupos_origem = df_para_unir.to_dicts()
        self._salvar_tabela_agregada_e_mapa(cnpj, df_resultado)

        # Persistir mapa manual derivado da agregacao para permitir rastreabilidade
        try:
            # df_prod_sel contem as descricoes/ids relacionadas aos itens agrupados
            if "df_prod_sel" in locals() and not df_prod_sel.is_empty():
                df_manual = (
                    df_prod_sel.select(
                        [
                            c
                            for c in ["id_descricao", "descricao_normalizada"]
                            if c in df_prod_sel.columns
                        ]
                    )
                    .unique()
                    .with_columns(pl.lit(id_destino).cast(pl.Utf8).alias("id_agrupado"))
                )
                try:
                    # Nao reprocessar aqui; a agregacao ja fara os recalculos sequenciais.
                    self.salvar_mapa_manual(cnpj, df_manual, reprocessar=False)
                except Exception as _e:
                    try:
                        self._registrar_log(
                            cnpj,
                            {
                                "tipo": "mapa_manual_persist_failed",
                                "erro": str(_e),
                            },
                        )
                    except Exception:
                        pass
        except Exception:
            # Não deve impedir a conclusão da agregação
            pass

        if not self.recalcular_valores_totais(
            cnpj, reprocessar_referencias=False, reset_timings=False
        ):
            raise RuntimeError(
                "Falha ao recalcular totais e precos medios da agregacao."
            )

        # Mantem tabelas derivadas sincronizadas apos revisao manual de agrupamentos.
        self.recalcular_produtos_final(cnpj)
        self.recalcular_referencias_produtos(cnpj)

        self._registrar_log(
            cnpj,
            {
                "tipo": "agregacao",
                "id_destino": id_destino,
                "ids_unidos": ids_agrupados_selecionados,
                "ids_origem_agrupamento": ids_origem_agrupamento,
                "nova_descr": nova_linha["descr_padrao"],
                "lista_itens_agrupados": lista_itens_agrupados,
                "grupos_origem": grupos_origem,
            },
        )

        return {"success": True}

    def recalcular_produtos_final(self, cnpj: str) -> bool:
        path_base = self.caminho_tabela_base(cnpj)
        path_agrup = self.caminho_tabela_agregadas(cnpj)
        path_pont = (
            CNPJ_ROOT
            / cnpj
            / "analises"
            / "produtos"
            / f"map_produto_agrupado_{cnpj}.parquet"
        )
        path_final = self.caminho_tabela_final(cnpj)

        if not path_base.exists() or not path_agrup.exists():
            return False

        lf_base = pl.scan_parquet(path_base)
        df_base = self._padronizar_chaves_prod(lf_base.collect())
        lf_agrup = pl.scan_parquet(path_agrup)
        df_agrup = self._promover_tipos_sefin(lf_agrup.collect())

        if path_pont.exists():
            lf_pont = pl.scan_parquet(path_pont)
            df_pont = lf_pont.collect().with_columns(
                pl.col("chave_produto").cast(pl.Utf8, strict=False)
            )
        elif "lista_chave_produto" in df_agrup.columns:
            df_pont = (
                df_agrup.select(["id_agrupado", "lista_chave_produto"])
                .explode("lista_chave_produto")
                .rename({"lista_chave_produto": "chave_produto"})
                .drop_nulls("chave_produto")
            )
        else:
            return False

        if "lista_chave_produto" not in df_agrup.columns:
            df_list = df_pont.group_by("id_agrupado").agg(
                pl.col("chave_produto").alias("lista_chave_produto")
            )
            df_agrup = df_agrup.join(df_list, on="id_agrupado", how="left")

        if "co_sefin_agr" not in df_agrup.columns:
            df_agrup = df_agrup.with_columns(
                pl.col("lista_co_sefin")
                .cast(pl.List(pl.Utf8), strict=False)
                .list.join(", ")
                .alias("co_sefin_agr")
            )

        df_map = (
            df_agrup.select(
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
                + (
                    ["versao_agrupamento"]
                    if "versao_agrupamento" in df_agrup.columns
                    else []
                )
            )
            .explode("lista_chave_produto")
            .rename({"lista_chave_produto": "chave_item"})
        )

        df_final = (
            df_base.join(df_map, on="chave_item", how="left")
            .with_columns(
                [
                    pl.coalesce([pl.col("descr_padrao"), pl.col("descricao")]).alias(
                        "descricao_final"
                    ),
                    pl.coalesce(
                        [pl.col("ncm_padrao"), pl.col("lista_ncm").list.first()]
                    ).alias("ncm_final"),
                    pl.coalesce(
                        [pl.col("cest_padrao"), pl.col("lista_cest").list.first()]
                    ).alias("cest_final"),
                    pl.coalesce(
                        [pl.col("gtin_padrao"), pl.col("lista_gtin").list.first()]
                    ).alias("gtin_final"),
                    pl.coalesce(
                        [
                            pl.col("co_sefin_padrao"),
                            pl.col("lista_co_sefin_agr").list.first(),
                            pl.col("lista_co_sefin").list.first(),
                        ]
                    ).alias("co_sefin_final"),
                    pl.coalesce(
                        [
                            pl.col("lista_unidades_agr").list.first(),
                            pl.col("lista_unid").list.first(),
                        ]
                    ).alias("unid_ref_sugerida"),
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

    def recalcular_referencias_agr(
        self, cnpj: str, progresso=None, reset_timings: bool = True
    ) -> bool:
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
        - aba_periodos
        """
        if reset_timings:
            self.ultimo_tempo_etapas = {}
        inicio_total = perf_counter()
        contexto_base = {"cnpj": cnpj, "fluxo": "recalcular_referencias_agr"}
        ok_final = self._executar_etapa_tempo(
            "produtos_final",
            lambda: self.recalcular_produtos_final(cnpj),
            progresso,
            contexto=contexto_base,
        )
        ok_fontes = self._executar_etapa_tempo(
            "fontes_agr",
            lambda: self.refazer_tabelas_agr(cnpj),
            progresso,
            contexto=contexto_base,
        )
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
        if gerar_calculos_periodos is None:
            raise ImportError("Nao foi possivel importar calculos_periodos.py.")

        ok_fatores = (
            self._executar_etapa_tempo(
                "fatores_conversao",
                lambda: bool(calcular_fatores_conversao(cnpj)),
                progresso,
                contexto=contexto_base,
            )
            if (ok_final and ok_fontes)
            else False
        )
        ok_c170 = (
            self._executar_etapa_tempo(
                "c170_xml",
                lambda: bool(gerar_c170_xml(cnpj)),
                progresso,
                contexto=contexto_base,
            )
            if ok_fatores
            else False
        )
        ok_c176 = (
            self._executar_etapa_tempo(
                "c176_xml",
                lambda: bool(gerar_c176_xml(cnpj)),
                progresso,
                contexto=contexto_base,
            )
            if ok_c170
            else False
        )
        ok_mov = (
            self._executar_etapa_tempo(
                "mov_estoque",
                lambda: bool(gerar_movimentacao_estoque(cnpj)),
                progresso,
                contexto=contexto_base,
            )
            if ok_c176
            else False
        )
        ok_mensal = (
            self._executar_etapa_tempo(
                "calculos_mensais",
                lambda: bool(gerar_calculos_mensais(cnpj)),
                progresso,
                contexto=contexto_base,
            )
            if ok_mov
            else False
        )
        ok_anual = (
            self._executar_etapa_tempo(
                "calculos_anuais",
                lambda: bool(gerar_calculos_anuais(cnpj)),
                progresso,
                contexto=contexto_base,
            )
            if ok_mensal
            else False
        )
        ok_periodos = (
            self._executar_etapa_tempo(
                "calculos_periodos",
                lambda: bool(gerar_calculos_periodos(cnpj)),
                progresso,
                contexto=contexto_base,
            )
            if ok_anual
            else False
        )
        ok_total = bool(
            ok_final
            and ok_fontes
            and ok_fatores
            and ok_c170
            and ok_c176
            and ok_mov
            and ok_mensal
            and ok_anual
            and ok_periodos
        )
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

    def recalcular_referencias_produtos(
        self, cnpj: str, progresso=None, reset_timings: bool = True
    ) -> bool:
        """Alias legado para recalcular_referencias_agr."""
        return self.recalcular_referencias_agr(
            cnpj, progresso=progresso, reset_timings=reset_timings
        )

    def recalcular_mov_estoque(
        self, cnpj: str, progresso=None, reset_timings: bool = True
    ) -> bool:
        """
        Recalcula artefatos diretamente afetados por ajustes manuais em fatores de conversao,
        incluindo a tabela de periodos derivada de mov_estoque.
        """
        if gerar_c176_xml is None:
            raise ImportError("Nao foi possivel importar c176_xml.py.")
        if gerar_movimentacao_estoque is None:
            raise ImportError("Nao foi possivel importar movimentacao_estoque.py.")
        if gerar_calculos_mensais is None:
            raise ImportError("Nao foi possivel importar calculos_mensais.py.")
        if gerar_calculos_anuais is None:
            raise ImportError("Nao foi possivel importar calculos_anuais.py.")
        if gerar_calculos_periodos is None:
            raise ImportError("Nao foi possivel importar calculos_periodos.py.")

        if reset_timings:
            self.ultimo_tempo_etapas = {}
        inicio_total = perf_counter()
        contexto_base = {"cnpj": cnpj, "fluxo": "recalcular_mov_estoque"}
        ok_c176 = self._executar_etapa_tempo(
            "c176_xml",
            lambda: bool(gerar_c176_xml(cnpj)),
            progresso,
            contexto=contexto_base,
        )
        ok_mov = (
            self._executar_etapa_tempo(
                "mov_estoque",
                lambda: bool(gerar_movimentacao_estoque(cnpj)),
                progresso,
                contexto=contexto_base,
            )
            if ok_c176
            else False
        )
        ok_mensal = (
            self._executar_etapa_tempo(
                "calculos_mensais",
                lambda: bool(gerar_calculos_mensais(cnpj)),
                progresso,
                contexto=contexto_base,
            )
            if ok_mov
            else False
        )
        ok_anual = (
            self._executar_etapa_tempo(
                "calculos_anuais",
                lambda: bool(gerar_calculos_anuais(cnpj)),
                progresso,
                contexto=contexto_base,
            )
            if ok_mensal
            else False
        )
        ok_periodos = (
            self._executar_etapa_tempo(
                "calculos_periodos",
                lambda: bool(gerar_calculos_periodos(cnpj)),
                progresso,
                contexto=contexto_base,
            )
            if ok_anual
            else False
        )
        ok_total = bool(ok_c176 and ok_mov and ok_mensal and ok_anual and ok_periodos)
        self._registrar_tempo(
            "recalcular_mov_estoque_total",
            perf_counter() - inicio_total,
            progresso,
            contexto={**contexto_base, "sucesso": ok_total},
        )
        return ok_total

    def recalcular_resumos_estoque(
        self, cnpj: str, progresso=None, reset_timings: bool = True
    ) -> bool:
        """
        Recalcula as tabelas analiticas derivadas de mov_estoque quando estiverem ausentes
        ou defasadas: mensal, anual e periodos.
        """
        if gerar_calculos_mensais is None:
            raise ImportError("Nao foi possivel importar calculos_mensais.py.")
        if gerar_calculos_anuais is None:
            raise ImportError("Nao foi possivel importar calculos_anuais.py.")
        if gerar_calculos_periodos is None:
            raise ImportError("Nao foi possivel importar calculos_periodos.py.")

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
                progresso(
                    "Mensal, anual e periodos ja estao sincronizados com mov_estoque."
                )
            return True

        ok_mensal = True
        ok_anual = True
        ok_periodos = True
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
        if "calculos_periodos" in artefatos_defasados:
            ok_periodos = self._executar_etapa_tempo(
                "calculos_periodos",
                lambda: bool(gerar_calculos_periodos(cnpj)),
                progresso,
                contexto=contexto_base,
            )

        ok_total = bool(ok_mensal and ok_anual and ok_periodos)
        self._registrar_tempo(
            "recalcular_resumos_estoque_total",
            perf_counter() - inicio_total,
            progresso,
            contexto={
                **contexto_base,
                "sucesso": ok_total,
                "etapas": artefatos_defasados,
            },
        )
        return ok_total

    def _carregar_historico_agregacoes(self, cnpj: str) -> list[dict]:
        log_path = self.caminho_log_agregacoes(cnpj)
        if log_path.exists():
            try:
                with open(log_path, "r", encoding="utf-8") as f:
                    dados = json.load(f)
                    if isinstance(dados, list):
                        return dados
            except Exception:
                return []
        return []

    def _salvar_historico_agregacoes(self, cnpj: str, historico: list[dict]) -> None:
        log_path = self.caminho_log_agregacoes(cnpj)
        with open(log_path, "w", encoding="utf-8") as f:
            json.dump(historico, f, indent=2, ensure_ascii=False)

    def _registrar_log(self, cnpj: str, entrada: dict):
        historico = self._carregar_historico_agregacoes(cnpj)

        from datetime import datetime

        entrada["timestamp"] = datetime.now().isoformat()
        historico.append(entrada)
        self._salvar_historico_agregacoes(cnpj, historico)

    def ler_linhas_log(self, cnpj: str = "") -> list:
        if not cnpj:
            return []
        return self._carregar_historico_agregacoes(cnpj)

    def _localizar_ultima_agregacao_reversivel(
        self, cnpj: str, id_agrupado: str
    ) -> tuple[int, dict] | tuple[None, None]:
        historico = self._carregar_historico_agregacoes(cnpj)
        for indice in range(len(historico) - 1, -1, -1):
            entrada = historico[indice]
            if (
                entrada.get("tipo") == "agregacao"
                and entrada.get("id_destino") == id_agrupado
                and not entrada.get("revertida", False)
            ):
                return indice, entrada
        return None, None

    def reverter_agrupamento(self, cnpj: str, id_agrupado: str) -> dict:
        df_atual = self.carregar_tabela_agregadas(cnpj, criar_se_ausente=True)
        if df_atual.is_empty():
            raise ValueError("Nao ha tabela de agregacao para reverter.")

        if id_agrupado not in set(
            df_atual.get_column("id_agrupado").cast(pl.Utf8, strict=False).to_list()
        ):
            raise ValueError(
                f"id_agrupado nao encontrado na agregacao atual: {id_agrupado}"
            )

        indice_log, entrada = self._localizar_ultima_agregacao_reversivel(
            cnpj, id_agrupado
        )
        if entrada is None:
            raise ValueError(
                "Nao foi encontrado historico reversivel para o agrupamento selecionado."
            )

        grupos_origem = entrada.get("grupos_origem") or []
        if len(grupos_origem) < 2:
            raise ValueError(
                "O historico do agrupamento nao possui dados suficientes para reversao."
            )

        ids_origem = self._deduplicar_preservando_ordem(
            [
                str(item).strip()
                for item in (entrada.get("ids_unidos") or [])
                if str(item).strip()
            ]
        )
        ids_existentes = set(
            df_atual.filter(
                pl.col("id_agrupado").cast(pl.Utf8, strict=False) != id_agrupado
            )
            .get_column("id_agrupado")
            .cast(pl.Utf8, strict=False)
            .to_list()
        )
        conflito_ids = [item for item in ids_origem if item in ids_existentes]
        if conflito_ids:
            raise ValueError(
                "Nao foi possivel reverter porque alguns ids de origem ja existem na tabela atual: "
                + ", ".join(conflito_ids)
            )

        df_restante = df_atual.filter(
            pl.col("id_agrupado").cast(pl.Utf8, strict=False) != id_agrupado
        )
        df_origem = self._alinhar_registros_por_schema(grupos_origem, df_atual.schema)
        df_resultado = pl.concat([df_restante, df_origem], how="vertical_relaxed")
        self._salvar_tabela_agregada_e_mapa(cnpj, df_resultado)

        if not self.recalcular_valores_totais(
            cnpj, reprocessar_referencias=False, reset_timings=False
        ):
            raise RuntimeError(
                "Falha ao recalcular totais e precos medios apos reverter agrupamento."
            )

        self.recalcular_produtos_final(cnpj)
        self.recalcular_referencias_produtos(cnpj)

        historico = self._carregar_historico_agregacoes(cnpj)
        historico[indice_log]["revertida"] = True
        from datetime import datetime

        historico[indice_log]["revertida_em"] = datetime.now().isoformat()
        self._salvar_historico_agregacoes(cnpj, historico)

        self._registrar_log(
            cnpj,
            {
                "tipo": "desagrupacao",
                "id_destino": id_agrupado,
                "ids_restaurados": ids_origem,
                "lista_itens_agrupados": entrada.get("lista_itens_agrupados") or [],
            },
        )

        return {
            "success": True,
            "id_destino": id_agrupado,
            "ids_restaurados": ids_origem,
            "qtd_grupos_restaurados": len(ids_origem),
        }

    def carregar_tabela_editavel(self, cnpj: str, progresso=None) -> Path:
        """
        Carrega a tabela de agregação editável, agora com leitura paralela das tabelas base.
        """
        import concurrent.futures

        if not self._tabelas_base_existem(cnpj):
            raise FileNotFoundError(
                f"Tabelas base ausentes para o CNPJ {cnpj}.\n\n"
                f"Gere as tabelas: item_unidades, descricao_produtos e produtos_final antes de abrir a agregacao."
            )
        cache = {}
        pasta = CNPJ_ROOT / cnpj / "analises" / "produtos"
        paths = {
            "item_unidades": pasta / f"item_unidades_{cnpj}.parquet",
            "descricao_produtos": pasta / f"descricao_produtos_{cnpj}.parquet",
            "produtos_final": pasta / f"produtos_final_{cnpj}.parquet",
        }

        # Leitura paralela das tabelas base
        def ler(nome_path):
            nome, path = nome_path
            return (
                nome,
                self._ler_parquet_com_cache(path, cache, nome, progresso, cnpj=cnpj),
            )

        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
            futs = {executor.submit(ler, item): item[0] for item in paths.items()}
            for fut in concurrent.futures.as_completed(futs):
                try:
                    nome, df = fut.result()
                    cache[paths[nome]] = df
                except Exception as exc:
                    if progresso:
                        progresso(f"[ERRO] Falha ao ler {futs[fut]}: {exc}")
                    raise
        # Só então tenta garantir agregação
        path_agr = self.garantir_tabela_agregadas(cnpj, criar_se_ausente=True)
        if not path_agr.exists():
            raise FileNotFoundError(
                "Tabela produtos_agrupados nao encontrada. Gere ou recalcule a camada de agregacao antes de abrir a aba."
            )
        self._ler_parquet_com_cache(
            path_agr, cache, "produtos_agrupados", progresso, cnpj=cnpj
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
                    "id_descricao",
                    "chave_item",
                    "chave_produto",
                    "descricao_normalizada",
                    "descricao",
                    "lista_desc_compl",
                    "lista_ncm",
                    "lista_cest",
                    "lista_gtin",
                    "lista_co_sefin",
                    "lista_unid",
                    "fontes",
                ],
            )
        )
        df_base = self._ler_parquet_colunas(
            path_base,
            ["descricao", "fonte", "fontes", "ncm", "cest", "gtin", "co_sefin_item"],
        ).with_columns(
            self._expr_normalizar_descricao("descricao").alias(
                "descricao_normalizada_temp"
            )
        )

        # Optimized: Pre-calculate string normalization outside redundant loops to preserve vectorization
        df_base = df_base.with_columns(
            self._expr_normalizar_descricao("descricao").alias("descricao_normalizada")
        )

        # Optimized: Vectorized calculation of lists and fallback descriptions
        df_prod_exp = df_prod.select(
            [
                "chave_item",
                "descricao",
                "descricao_normalizada",
                "lista_co_sefin",
                "lista_unid",
                "lista_ncm",
                "lista_cest",
                "lista_gtin",
                "lista_desc_compl",
            ]
        )

        df_agrup_exp = (
            df_agrup.select(["id_agrupado", "lista_chave_produto"])
            .explode("lista_chave_produto")
            .rename({"lista_chave_produto": "chave_item"})
        )
        df_joined = df_agrup_exp.join(df_prod_exp, on="chave_item", how="left")

        # Vectorized aggregation of lists (NCM, CEST, GTIN, Descricoes, etc.)
        df_aggr_lists = (
            df_joined.group_by("id_agrupado")
            .agg(
                pl.col("lista_co_sefin")
                .explode()
                .unique()
                .sort()
                .drop_nulls()
                .alias("lista_co_sefin"),
                pl.col("lista_unid")
                .explode()
                .unique()
                .sort()
                .drop_nulls()
                .alias("lista_unidades"),
                pl.col("lista_ncm")
                .explode()
                .unique()
                .sort()
                .drop_nulls()
                .alias("lista_ncm"),
                pl.col("lista_cest")
                .explode()
                .unique()
                .sort()
                .drop_nulls()
                .alias("lista_cest"),
                pl.col("lista_gtin")
                .explode()
                .unique()
                .sort()
                .drop_nulls()
                .alias("lista_gtin"),
                pl.col("descricao")
                .unique()
                .sort()
                .drop_nulls()
                .alias("lista_descricoes"),
                pl.col("lista_desc_compl")
                .explode()
                .unique()
                .sort()
                .drop_nulls()
                .alias("lista_desc_compl"),
            )
            .with_columns(
                pl.col("lista_co_sefin").list.len().gt(1).alias("co_sefin_divergentes"),
                pl.col("lista_co_sefin")
                .cast(pl.List(pl.Utf8))
                .list.join(", ")
                .alias("co_sefin_agr"),
            )
        )

        # descr_fallback (first non-empty description)
        df_fallback = (
            df_joined.filter(
                pl.col("descricao")
                .cast(pl.Utf8, strict=False)
                .fill_null("")
                .str.strip_chars()
                != ""
            )
            .group_by("id_agrupado")
            .agg(pl.col("descricao").first().alias("descr_fallback"))
        )

        # Mapping for df_base filtering
        df_mapping = (
            df_joined.select(["id_agrupado", "descricao_normalizada"])
            .unique()
            .drop_nulls()
        )
        df_base_mapped = df_base.join(df_mapping, on="descricao_normalizada")

        # Partition df_base by group for efficient access
        dict_base_parts = df_base_mapped.partition_by("id_agrupado", as_dict=True)

        # Optimized: Indexed dictionaries for O(1) lookups
        dict_aggr = {row["id_agrupado"]: row for row in df_aggr_lists.to_dicts()}
        dict_fallback = {
            row["id_agrupado"]: row["descr_fallback"] for row in df_fallback.to_dicts()
        }

        registros = []
        for row in df_agrup.to_dicts():
            id_grp = row["id_agrupado"]

            # Use pre-filtered DataFrame from dictionary — check with is_empty() to avoid DataFrame ambiguity in boolean context
            df_base_filtered_raw = dict_base_parts.get((id_grp,))
            if df_base_filtered_raw is None or df_base_filtered_raw.is_empty():
                df_base_filtered = df_base.filter(pl.lit(False))
            else:
                df_base_filtered = df_base_filtered_raw

            padrao = calcular_atributos_padrao(df_base_filtered)

            # Retrieve pre-calculated vectorized values using O(1) dictionary lookups
            aggr_info = dict_aggr.get(id_grp, {})
            descr_fallback = dict_fallback.get(id_grp)

            row["descr_padrao"] = (
                padrao.get("descr_padrao") or row.get("descr_padrao") or descr_fallback
            )
            row["ncm_padrao"] = padrao.get("ncm_padrao")
            row["cest_padrao"] = padrao.get("cest_padrao")
            row["gtin_padrao"] = padrao.get("gtin_padrao")

            row["lista_ncm"] = aggr_info.get("lista_ncm") or self._coletar_lista_coluna(
                df_base_filtered, "ncm"
            )
            row["lista_cest"] = aggr_info.get(
                "lista_cest"
            ) or self._coletar_lista_coluna(df_base_filtered, "cest")
            row["lista_gtin"] = aggr_info.get(
                "lista_gtin"
            ) or self._coletar_lista_coluna(df_base_filtered, "gtin")
            row["lista_descricoes"] = aggr_info.get(
                "lista_descricoes"
            ) or self._coletar_lista_coluna(df_base_filtered, "descricao")
            row["lista_desc_compl"] = aggr_info.get("lista_desc_compl", [])
            row["co_sefin_padrao"] = padrao.get("co_sefin_padrao")

            row["lista_co_sefin"] = aggr_info.get("lista_co_sefin", [])
            row["co_sefin_agr"] = aggr_info.get("co_sefin_agr", "")
            row["co_sefin_divergentes"] = aggr_info.get("co_sefin_divergentes", False)
            row["lista_unidades"] = aggr_info.get("lista_unidades", [])

            registros.append(row)

        df_novo = pl.DataFrame(registros, schema=df_agrup.schema)
        df_novo.drop("lista_chave_produto", strict=False).write_parquet(path_agrup)
        contexto_base = {"cnpj": cnpj, "fluxo": "recalcular_todos_padroes"}
        if reprocessar_referencias:
            self._executar_etapa_tempo(
                "produtos_final",
                lambda: self.recalcular_produtos_final(cnpj),
                progresso,
                contexto=contexto_base,
            )
            self._executar_etapa_tempo(
                "referencias_produtos",
                lambda: self.recalcular_referencias_produtos(
                    cnpj, progresso=progresso, reset_timings=False
                ),
                progresso,
                contexto=contexto_base,
            )
        self._registrar_tempo(
            "recalcular_todos_padroes_total",
            perf_counter() - inicio_total,
            progresso,
            contexto=contexto_base,
        )
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
            self._ler_parquet_colunas(
                path_prod,
                [
                    "id_descricao",
                    "chave_item",
                    "chave_produto",
                    "descricao_normalizada",
                    "descricao",
                ],
            )
        )
        df_base = self._ler_parquet_colunas(
            path_base,
            ["descricao", "compras", "qtd_compras", "vendas", "qtd_vendas"],
        )
        # Normalizar descrição para permitir joins estáveis entre produtos e itens
        df_base = df_base.with_columns(
            self._expr_normalizar_descricao("descricao").alias("descricao_normalizada")
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
                df_metricas = df_agrup.select("id_agrupado").with_columns(
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
            else:
                df_metricas = pl.DataFrame(  # noqa: F841
                    schema={
                        "id_agrupado": pl.Utf8,
                        **{col: pl.Float64 for col in colunas_metricas},
                    }
                )
        else:
            df_desc_por_grupo = (  # noqa: F841
                df_agrup.select(["id_agrupado", "lista_chave_produto"])
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

        # Optimized: Vectorized calculation of totals
        cols_agrup = (
            df_agrup.collect_schema().names()
            if isinstance(df_agrup, pl.LazyFrame)
            else df_agrup.columns
        )
        if "lista_chave_produto" in cols_agrup:
            # Mapear por descrição normalizada para evitar faltas de coluna 'descricao' em df_prod
            df_mapping = (
                df_agrup.select(["id_agrupado", "lista_chave_produto"])
                .explode("lista_chave_produto")
                .rename({"lista_chave_produto": "chave_item"})
                .join(
                    df_prod.select(["chave_item", "descricao_normalizada"]),
                    on="chave_item",
                )
                .select(["id_agrupado", "descricao_normalizada"])
                .unique()
            )

            df_totais = (
                df_base.select(
                    [
                        "descricao_normalizada",
                        "compras",
                        "qtd_compras",
                        "vendas",
                        "qtd_vendas",
                    ]
                )
                .join(df_mapping, on="descricao_normalizada")
                .group_by("id_agrupado")
                .agg(
                    [
                        pl.col("compras").fill_null(0).sum().alias("total_compras"),
                        pl.col("vendas").fill_null(0).sum().alias("total_vendas"),
                        pl.col("qtd_compras")
                        .fill_null(0)
                        .sum()
                        .alias("qtd_compras_total"),
                        pl.col("qtd_vendas")
                        .fill_null(0)
                        .sum()
                        .alias("qtd_vendas_total"),
                    ]
                )
            )
            # Calcular preços médios protegendo divisão por zero
            df_totais = df_totais.with_columns(
                (
                    pl.when(pl.col("qtd_compras_total") > 0)
                    .then(pl.col("total_compras") / pl.col("qtd_compras_total"))
                    .otherwise(pl.lit(None))
                ).alias("preco_medio_compra"),
                (
                    pl.when(pl.col("qtd_vendas_total") > 0)
                    .then(pl.col("total_vendas") / pl.col("qtd_vendas_total"))
                    .otherwise(pl.lit(None))
                ).alias("preco_medio_venda"),
            )
            # Campos auxiliares de entradas/saidas e movimentacao total
            df_totais = df_totais.with_columns(
                pl.col("total_compras").alias("total_entradas"),
                pl.col("total_vendas").alias("total_saidas"),
                (pl.col("total_compras") + pl.col("total_vendas")).alias(
                    "total_movimentacao"
                ),
            )

            colunas_metricas_obsoletas = set(colunas_metricas)
            colunas_metricas_obsoletas.update(
                f"{coluna}_right" for coluna in colunas_metricas
            )
            cols_to_drop = [
                coluna
                for coluna in df_agrup.columns
                if coluna in colunas_metricas_obsoletas
            ]
            if cols_to_drop:
                df_agrup = df_agrup.drop(cols_to_drop)

            df_agrup = df_agrup.join(
                df_totais, on="id_agrupado", how="left"
            ).with_columns(
                [
                    pl.col("total_compras").fill_null(0.0),
                    pl.col("total_vendas").fill_null(0.0),
                ]
            )
        else:
            if "total_compras" not in cols_agrup:
                df_agrup = df_agrup.with_columns(pl.lit(0.0).alias("total_compras"))
            if "total_vendas" not in cols_agrup:
                df_agrup = df_agrup.with_columns(pl.lit(0.0).alias("total_vendas"))

        df_agrup.drop("lista_chave_produto", strict=False).write_parquet(path_agrup)
        contexto_base = {"cnpj": cnpj, "fluxo": "recalcular_valores_totais"}
        if reprocessar_referencias:
            self._executar_etapa_tempo(
                "produtos_final",
                lambda: self.recalcular_produtos_final(cnpj),
                progresso,
                contexto=contexto_base,
            )
            self._executar_etapa_tempo(
                "referencias_produtos",
                lambda: self.recalcular_referencias_produtos(
                    cnpj, progresso=progresso, reset_timings=False
                ),
                progresso,
                contexto=contexto_base,
            )
        self._registrar_tempo(
            "recalcular_totais_total",
            perf_counter() - inicio_total,
            progresso,
            contexto=contexto_base,
        )
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
        if gerar_calculos_periodos is None:
            raise ImportError("Nao foi possivel importar calculos_periodos.py.")

        ok_padroes = bool(
            self.recalcular_todos_padroes(
                cnpj,
                progresso=progresso,
                reprocessar_referencias=False,
                reset_timings=False,
            )
        )
        ok_totais = (
            bool(
                self.recalcular_valores_totais(
                    cnpj,
                    progresso=progresso,
                    reprocessar_referencias=False,
                    reset_timings=False,
                )
            )
            if ok_padroes
            else False
        )
        ok_final = (
            self._executar_etapa_tempo(
                "produtos_final",
                lambda: self.recalcular_produtos_final(cnpj),
                progresso,
                contexto=contexto_base,
            )
            if ok_totais
            else False
        )
        ok_fontes = (
            self._executar_etapa_tempo(
                "fontes_agr",
                lambda: self.refazer_tabelas_agr(cnpj),
                progresso,
                contexto=contexto_base,
            )
            if ok_final
            else False
        )
        ok_precos = (
            self._executar_etapa_tempo(
                "precos_medios_produtos_final",
                lambda: (
                    bool(calcular_precos_medios_produtos_final(cnpj))
                    if calcular_precos_medios_produtos_final is not None
                    else True
                ),
                progresso,
                contexto=contexto_base,
            )
            if ok_fontes
            else False
        )
        ok_fatores = (
            self._executar_etapa_tempo(
                "fatores_conversao",
                lambda: bool(calcular_fatores_conversao(cnpj)),
                progresso,
                contexto=contexto_base,
            )
            if ok_precos
            else False
        )
        ok_c170 = (
            self._executar_etapa_tempo(
                "c170_xml",
                lambda: bool(gerar_c170_xml(cnpj)),
                progresso,
                contexto=contexto_base,
            )
            if ok_fatores
            else False
        )
        ok_c176 = (
            self._executar_etapa_tempo(
                "c176_xml",
                lambda: bool(gerar_c176_xml(cnpj)),
                progresso,
                contexto=contexto_base,
            )
            if ok_c170
            else False
        )
        ok_mov = (
            self._executar_etapa_tempo(
                "mov_estoque",
                lambda: bool(gerar_movimentacao_estoque(cnpj)),
                progresso,
                contexto=contexto_base,
            )
            if ok_c176
            else False
        )
        ok_mensal = (
            self._executar_etapa_tempo(
                "calculos_mensais",
                lambda: bool(gerar_calculos_mensais(cnpj)),
                progresso,
                contexto=contexto_base,
            )
            if ok_mov
            else False
        )
        ok_anual = (
            self._executar_etapa_tempo(
                "calculos_anuais",
                lambda: bool(gerar_calculos_anuais(cnpj)),
                progresso,
                contexto=contexto_base,
            )
            if ok_mensal
            else False
        )
        ok_periodos = (
            self._executar_etapa_tempo(
                "calculos_periodos",
                lambda: bool(gerar_calculos_periodos(cnpj)),
                progresso,
                contexto=contexto_base,
            )
            if ok_anual
            else False
        )
        ok_ressarc = (
            self._executar_etapa_tempo(
                "ressarcimento_st",
                lambda: bool(gerar_ressarcimento_st(cnpj)),
                progresso,
                contexto=contexto_base,
            )
            if ok_periodos
            else False
        )
        ok_resumo = (
            self._executar_etapa_tempo(
                "aba_resumo_global",
                lambda: bool(gerar_aba_resumo_global(cnpj)),
                progresso,
                contexto=contexto_base,
            )
            if ok_ressarc
            else False
        )
        ok_sel = (
            self._executar_etapa_tempo(
                "aba_produtos_selecionados",
                lambda: bool(gerar_aba_produtos_selecionados(cnpj)),
                progresso,
                contexto=contexto_base,
            )
            if ok_resumo
            else False
        )

        ok_total = bool(
            ok_padroes
            and ok_totais
            and ok_final
            and ok_fontes
            and ok_precos
            and ok_fatores
            and ok_c170
            and ok_c176
            and ok_mov
            and ok_mensal
            and ok_anual
            and ok_periodos
            and ok_ressarc
            and ok_resumo
            and ok_sel
        )
        self._registrar_tempo(
            "reprocessar_agregacao_total",
            perf_counter() - inicio_total,
            progresso,
            contexto={**contexto_base, "sucesso": ok_total},
        )
        return ok_total
