"""
Serviço de agregação de produtos.
Refatorado para nomes em Português e integração com c:\\funcoes.
"""
from __future__ import annotations

import hashlib
import json
import re
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

import polars as pl

from fiscal_app.config import AGGREGATION_LOG_FILE, CNPJ_ROOT, CFOP_BI_PATH
from fiscal_app.utils.text import natural_sort_key, normalize_text

CODE_ENTRY_RE = re.compile(r"\[(.*?);\s*(\d+)\]")


@dataclass
class ResultadoAgregacao:
    """Resultado da agregação de linhas."""
    caminho_destino: Path
    linha_agregada: dict[str, Any]
    chaves_removidas: list[tuple[str, str]]


class ServicoAgregacao:
    """Serviço para gerenciar a agregação manual de produtos."""

    def __init__(self, arquivo_log: Path = AGGREGATION_LOG_FILE):
        self.arquivo_log = arquivo_log
        self.arquivo_log.parent.mkdir(parents=True, exist_ok=True)

    @staticmethod
    def _escolher_moda(valores: list[Any]) -> str | None:
        limpos = [str(v).strip() for v in valores if v not in (None, "", []) and str(v).strip()]
        if not limpos:
            return None
        counts = Counter(limpos)
        max_count = max(counts.values())
        candidatos = [valor for valor, count in counts.items() if count == max_count]
        return sorted(candidatos, key=natural_sort_key)[0]

    @staticmethod
    def caminho_tabela_editavel(cnpj: str) -> Path:
        """Caminho para a tabela final editável do CNPJ."""
        return CNPJ_ROOT / cnpj / "analises" / "produtos" / f"tabela_produtos_editavel_{cnpj}.parquet"

    @staticmethod
    def caminho_tabela_origem(cnpj: str) -> Path:
        """Caminho para a tabela de descrições gerada pelo pipeline."""
        return CNPJ_ROOT / cnpj / "analises" / "produtos" / f"tabela_descricoes_{cnpj}.parquet"

    def carregar_tabela_editavel(self, cnpj: str) -> Path:
        """Copia a tabela de descrições para a tabela editável se esta não existir."""
        origem = self.caminho_tabela_origem(cnpj)
        destino = self.caminho_tabela_editavel(cnpj)

        if destino.exists():
            return destino

        if not origem.exists():
            raise FileNotFoundError(f"Tabela de descrições não encontrada em: {origem}")

        df = pl.read_parquet(origem)
        # Garante que as colunas básicas existam
        if "verificado" not in df.columns:
            df = df.with_columns(pl.lit(False).alias("verificado"))
            
        df.write_parquet(destino, compression="snappy")
        return destino

    @staticmethod
    def _garantir_lista(valor: Any) -> list[str]:
        if valor is None:
            return []
        if isinstance(valor, list):
            return [str(item) for item in valor if item not in (None, "")]
        text = str(valor).strip()
        if not text:
            return []
        return [text]

    @staticmethod
    def _analisar_codigos(valores_brutos: list[Any]) -> list[tuple[str, int, str]]:
        parsed: list[tuple[str, int, str]] = []
        for raw in valores_brutos:
            for entry in ServicoAgregacao._garantir_lista(raw):
                match = CODE_ENTRY_RE.match(entry)
                if match:
                    codigo = match.group(1).strip()
                    freq = int(match.group(2))
                    parsed.append((codigo, freq, entry))
                else:
                    parsed.append((entry, 1, entry))
        return parsed

    @staticmethod
    def _escolher_moda(valores: list[Any]) -> str | None:
        limpos = [str(v).strip() for v in valores if v not in (None, "", []) and str(v).strip()]
        if not limpos:
            return None
        counts = Counter(limpos)
        max_count = max(counts.values())
        candidatos = [valor for valor, count in counts.items() if count == max_count]
        return sorted(candidatos, key=natural_sort_key)[0]

    @staticmethod
    def _mesclar_colunas_lista(linhas: list[dict[str, Any]], coluna: str) -> list[str]:
        valores = []
        for linha in linhas:
            valores.extend(ServicoAgregacao._garantir_lista(linha.get(coluna)))
        return sorted(set(valores), key=natural_sort_key)

    def construir_linha_agregada(
        self,
        linhas: list[dict[str, Any]],
        descricao_resultante: str | None = None,
        descricao_normalizada_resultante: str | None = None,
    ) -> dict[str, Any]:
        """Cria uma nova linha consolidada a partir de múltiplas linhas selecionadas."""
        if len(linhas) < 2:
            raise ValueError("Selecione pelo menos duas linhas para agregar.")

        # Consolidação de Códigos
        dados_codigos = self._analisar_codigos([r.get("lista_codigos") for r in linhas])
        if not dados_codigos:
            # Fallback para cod_normalizado se lista_codigos estiver vazia
            dados_codigos = self._analisar_codigos([r.get("lista_cod_normalizado") for r in linhas])
            
        freq_codigos: Counter[str] = Counter()
        entradas_originais: dict[str, int] = {}
        for codigo, freq, _ in dados_codigos:
            freq_codigos[codigo] += freq
            entradas_originais[codigo] = max(entradas_originais.get(codigo, 0), freq)

        codigo_padrao = "0"
        if freq_codigos:
            max_freq = max(freq_codigos.values())
            top_codes = [c for c, f in freq_codigos.items() if f == max_freq]
            codigo_padrao = sorted(top_codes, key=natural_sort_key)[0]

        # Descrições
        desc = (descricao_resultante or self._escolher_moda([r.get("descricao") for r in linhas]) or linhas[0].get("descricao") or "").strip()
        desc_norm = (descricao_normalizada_resultante or normalize_text(desc)).strip()

        # Descrição Padrão (a que possui mais palavras)
        desc_padrao = linhas[0].get("descricao_padrao") or linhas[0].get("descricao") or ""
        max_palavras = 0
        for r in linhas:
            dn = str(r.get("descricao") or "")
            count = len(dn.split())
            if count > max_palavras:
                max_palavras = count
                desc_padrao = dn

        # Unir lista de itens (chave_item_individualizado)
        todas_chaves_itens = []
        for r in linhas:
            todas_chaves_itens.extend(self._garantir_lista(r.get("lista_chave_item_individualizado")))
        lista_final_itens = sorted(list(set(todas_chaves_itens)))

        # Gerar chave_produto (MD5 das chaves de itens ordenadas)
        texto_chave = "".join(lista_final_itens)
        chave_produto = hashlib.md5(texto_chave.encode()).hexdigest()

        mesclados_codigos = [f"[{c}; {entradas_originais[c]}]" for c in sorted(entradas_originais, key=natural_sort_key)]

        # Recalcular valores padrão (moda dos itens originais)
        def _get_moda_lista(coluna):
            valores = []
            for r in linhas:
                v = r.get(coluna)
                if v:
                    if isinstance(v, list):
                        valores.extend(v)
                    else:
                        valores.append(v)
            return self._escolher_moda(valores)

        lista_sefin = self._mesclar_colunas_lista(linhas, "lista_co_sefin_inferido")

        agregada = {
            "chave_produto": chave_produto,
            "descricao": desc,
            "lista_chave_item_individualizado": lista_final_itens,
            "lista_codigos": mesclados_codigos,
            "codigo_padrao": codigo_padrao,
            "descricao_padrao": desc_padrao,
            "ncm_padrao": _get_moda_lista("lista_ncm") or _get_moda_lista("ncm_padrao"),
            "cest_padrao": _get_moda_lista("lista_cest") or _get_moda_lista("cest_padrao"),
            "gtin_padrao": _get_moda_lista("lista_gtin") or _get_moda_lista("gtin_padrao"),
            "tipo_item_padrao": _get_moda_lista("lista_tipo_item") or _get_moda_lista("tipo_item_padrao"),
            "unid_padrao": _get_moda_lista("lista_unidades") or _get_moda_lista("lista_unids") or _get_moda_lista("unid_padrao"),
            "co_sefin_agr": self._escolher_moda(lista_sefin),
            "co_sefin_agr_divergente": len(set([str(s) for s in lista_sefin if s])) > 1,
            "lista_tipo_item": self._mesclar_colunas_lista(linhas, "lista_tipo_item"),
            "lista_ncm": self._mesclar_colunas_lista(linhas, "lista_ncm"),
            "lista_cest": self._mesclar_colunas_lista(linhas, "lista_cest"),
            "lista_gtin": self._mesclar_colunas_lista(linhas, "lista_gtin"),
            "lista_unids": self._mesclar_colunas_lista(linhas, "lista_unids"),
            "lista_unidades": self._mesclar_colunas_lista(linhas, "lista_unidades"),
            "lista_co_sefin_inferido": lista_sefin,
            "total_entradas": sum([float(r.get("total_entradas") or 0) for r in linhas]),
            "total_saidas": sum([float(r.get("total_saidas") or 0) for r in linhas]),
            "verificado": True, # Muda para true ao agregar manualmente
        }
        
        # Copiar outros campos que podem existir
        opcionais = ["lista_descr_compl"]
        for op in opcionais:
            if op in linhas[0]:
                agregada[op] = self._mesclar_colunas_lista(linhas, op)

        return agregada

    def agregar_linhas(
        self,
        cnpj: str,
        linhas_selecionadas: list[dict[str, Any]],
        descricao_resultante: str | None = None,
        descricao_normalizada_resultante: str | None = None,
    ) -> ResultadoAgregacao:
        """Executa a agregação e salva na tabela editável."""
        destino = self.carregar_tabela_editavel(cnpj)
        df_atual = pl.read_parquet(destino)
        
        # Identifica chaves únicas das linhas a remover
        # Usamos descricao (ou chave_produto se disponível)
        chaves_removidas = []
        for r in linhas_selecionadas:
            chaves_removidas.append((str(r.get("descricao") or ""), str(r.get("chave_produto") or "")))

        linha_agregada = self.construir_linha_agregada(linhas_selecionadas, descricao_resultante, descricao_normalizada_resultante)

        key_set = set(chaves_removidas)
        linhas_mantidas = []
        for r in df_atual.iter_rows(named=True):
            key = (str(r.get("descricao") or ""), str(r.get("chave_produto") or ""))
            if key not in key_set:
                linhas_mantidas.append(r)

        linhas_atualizadas = linhas_mantidas + [linha_agregada]
        
        # Garante que o esquema seja compatível
        try:
            df_novo = pl.DataFrame(linhas_atualizadas, schema=df_atual.schema)
        except Exception:
            # Fallback se colunas novas apareceram
            df_novo = pl.DataFrame(linhas_atualizadas)
            
        df_novo.write_parquet(destino, compression="snappy")

        self._registrar_log(cnpj=cnpj, destino=destino, originais=linhas_selecionadas, agregada=linha_agregada)
        return ResultadoAgregacao(caminho_destino=destino, linha_agregada=linha_agregada, chaves_removidas=chaves_removidas)

    def _registrar_log(self, cnpj: str, destino: Path, originais: list[dict[str, Any]], agregada: dict[str, Any]) -> None:
        payload = {
            "timestamp": datetime.now().isoformat(timespec="seconds"),
            "cnpj": cnpj,
            "arquivo_destino": str(destino),
            "linhas_origem": [
                {
                    "descricao": r.get("descricao"),
                    "chave_produto": r.get("chave_produto"),
                    "codigo_padrao": r.get("codigo_padrao"),
                }
                for r in originais
            ],
            "resultado": agregada,
        }
        with self.arquivo_log.open("a", encoding="utf-8") as f:
            f.write(json.dumps(payload, ensure_ascii=False) + "\n")

    def recalcular_todos_padroes(self, cnpj: str) -> bool:
        """
        Recalcula campos padrão (NCM, CEST, GTIN, UNID, SEFIN) de TODA a tabela editável
        baseado na moda dos itens que compõem cada grupo.
        """
        destino = self.caminho_tabela_editavel(cnpj)
        arq_itens = CNPJ_ROOT / cnpj / "analises" / "produtos" / f"tab_itens_caract_normalizada_{cnpj}.parquet"
        
        if not destino.exists() or not arq_itens.exists():
            return False

        df_edit = pl.read_parquet(destino)
        df_itens = pl.read_parquet(arq_itens)
        
        # Mapeamento: Chave_item -> Características
        campos_recalc = ["ncm", "cest", "gtin", "tipo_item", "unidade", "co_sefin_inferido"]
        item_map = df_itens.select(["chave_item_individualizado"] + [c for c in campos_recalc if c in df_itens.columns])

        # Vamos iterar sobre cada linha do editável (ou usar explode/join que é mais eficiente no Polars)
        # 1. Explode a lista de chaves
        df_exploded = df_edit.select(["chave_produto", "lista_chave_item_individualizado"]).explode("lista_chave_item_individualizado")
        
        # 2. Join com os itens originais
        df_join = df_exploded.join(item_map, left_on="lista_chave_item_individualizado", right_on="chave_item_individualizado", how="left")
        
        # 3. Agrupa por chave_produto e calcula moda
        def _get_moda(s: pl.Series) -> str | None:
            return self._escolher_moda(s.to_list())

        agg_exprs = []
        mapping_padrao = {
            "ncm": "ncm_padrao",
            "cest": "cest_padrao",
            "gtin": "gtin_padrao",
            "tipo_item": "tipo_item_padrao",
            "unidade": "unid_padrao",
            "co_sefin_inferido": "co_sefin_agr"
        }

        for orig, dest in mapping_padrao.items():
            if orig in df_join.columns:
                agg_exprs.append(
                    pl.col(orig).map_elements(_get_moda, return_dtype=pl.String).alias(dest)
                )

        df_recalc = df_join.group_by("chave_produto").agg(agg_exprs)
        
        # 4. Atualiza a tabela editável
        # Removemos os campos antigos e joinamos os novos
        df_sem_padroes = df_edit.drop([c for c in mapping_padrao.values() if c in df_edit.columns])
        df_novo = df_sem_padroes.join(df_recalc, on="chave_produto", how="left")
        
        df_novo.write_parquet(destino, compression="snappy")
        return True

    def recalcular_valores_totais(self, cnpj: str) -> bool:
        """
        Calcula tot_v_entradas e tot_v_saidas para cada chave_produto + unidade.
        Filtra CFOP por operacao_mercantil == 'X' (campo sisaudit_estoque).
        """
        from aux_leitura_notas import ler_nfe_nfce, ler_c170
        
        destino = self.caminho_tabela_editavel(cnpj)
        if not destino.exists():
            return False

        # 1. Carrega CFOPs válidos (Mercantil)
        df_cfop = pl.read_parquet(CFOP_BI_PATH).filter(pl.col("sisaudit_estoque") == "X")
        cfops_validos = df_cfop.select("co_cfop").to_series().to_list()

        # 2. Carrega Transações
        df_c170 = ler_c170(cnpj).filter(pl.col("co_cfop").is_in(cfops_validos))
        df_nfe = ler_nfe_nfce(cnpj).filter(pl.col("co_cfop").is_in(cfops_validos))

        # 3. Calcula Valores
        # Entradas (C170): IND_OPER == 0, VALOR_ITEM
        df_ent = (
            df_c170.filter(pl.col("ind_oper") == 0)
            .group_by(["codigo", "descricao", "unid_med"])
            .agg(pl.col("valor_item").sum().alias("v_ent"))
        )

        # Saídas (NFe/NFCe): V_item = PROD_VPROD + PROD_VFRETE + PROD_VSEG + PROD_VOUTRO - PROD_VDESC
        # Note: aux_leitura_notas may have different column names, we use standard logic
        df_sai_raw = df_nfe.filter(pl.col("ind_oper") == 1)
        
        # Garante colunas para a fórmula
        for c in ["prod_vprod", "prod_vfrete", "prod_vseg", "prod_voutro", "prod_vdesc"]:
            if c not in df_sai_raw.columns:
                df_sai_raw = df_sai_raw.with_columns(pl.lit(0.0).alias(c))

        df_sai_calc = df_sai_raw.with_columns(
            (pl.col("prod_vprod") + pl.col("prod_vfrete") + pl.col("prod_vseg") + 
             pl.col("prod_voutro") - pl.col("prod_vdesc")).alias("_v_calc")
        )

        df_sai = (
            df_sai_calc.group_by(["codigo", "descricao", "unid_med"])
            .agg(pl.col("_v_calc").sum().alias("v_sai"))
        )

        # 4. Join com Item -> chave_item_individualizado
        arq_itens = CNPJ_ROOT / cnpj / "analises" / "produtos" / f"tab_itens_caract_normalizada_{cnpj}.parquet"
        df_itens = pl.read_parquet(arq_itens).select(["codigo", "descricao", "chave_item_individualizado"])
        
        df_res_ent = df_ent.join(df_itens, on=["codigo", "descricao"], how="left")
        df_res_sai = df_sai.join(df_itens, on=["codigo", "descricao"], how="left")

        # 5. Join com Editable -> chave_produto
        df_edit = pl.read_parquet(destino)
        df_map = df_edit.select(["chave_produto", "lista_chave_item_individualizado"]).explode("lista_chave_item_individualizado")

        df_v_ent = df_res_ent.join(df_map, left_on="chave_item_individualizado", right_on="lista_chave_item_individualizado", how="inner")
        df_v_sai = df_res_sai.join(df_map, left_on="chave_item_individualizado", right_on="lista_chave_item_individualizado", how="inner")

        # 6. Agrupa por chave_produto + unidade (unid_med) e formata
        def _format_list(df, col_val, col_name):
            return (
                df.group_by(["chave_produto", "unid_med"])
                .agg(pl.col(col_val).sum())
                .sort(["chave_produto", "unid_med"])
                .with_columns(
                    ("[" + pl.col("unid_med") + "; " + pl.col(col_val).cast(pl.Int64).cast(pl.String) + "]").alias("_str")
                )
                .group_by("chave_produto")
                .agg(pl.col("_str").unique().sort().str.join("; ").alias(col_name))
            )

        df_final_ent = _format_list(df_v_ent, "v_ent", "tot_v_entradas")
        df_final_sai = _format_list(df_v_sai, "v_sai", "tot_v_saidas")

        # 7. Update editable table
        df_edit = df_edit.drop([c for c in ["tot_v_entradas", "tot_v_saidas"] if c in df_edit.columns])
        df_edit = df_edit.join(df_final_ent, on="chave_produto", how="left")
        df_edit = df_edit.join(df_final_sai, on="chave_produto", how="left")
        
        df_edit.write_parquet(destino, compression="snappy")
        return True

    def ler_linhas_log(self, cnpj: str | None = None, limite: int = 200) -> list[dict[str, Any]]:
        """
        Lê as últimas linhas do log, opcionalmente filtrando por CNPJ.
        Retorna uma lista de dicionários com os resultados das agregações.
        """
        if not self.arquivo_log.exists():
            return []
            
        resultados = []
        try:
            with self.arquivo_log.open("r", encoding="utf-8") as f:
                for line in f:
                    if not line.strip():
                        continue
                    try:
                        data = json.loads(line)
                        if cnpj is None or data.get("cnpj") == cnpj:
                            res = data.get("resultado")
                            if res:
                                # Adiciona metadados se necessário
                                res["_timestamp"] = data.get("timestamp")
                                resultados.append(res)
                    except json.JSONDecodeError:
                        continue
        except Exception:
            return []
            
        return resultados[-limite:]
