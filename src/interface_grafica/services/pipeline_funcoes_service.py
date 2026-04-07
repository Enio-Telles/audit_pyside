"""
Servico de pipeline que orquestra:
1. Extracao Oracle - executa SQLs selecionados
2. Geracao de tabelas - executa funcoes em src/transformacao

Salva:
- Parquets brutos em dados/CNPJ/<cnpj>/arquivos_parquet/
- Tabelas finais em dados/CNPJ/<cnpj>/analises/produtos/
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path
from time import perf_counter
from typing import Any, Callable

import polars as pl

from extracao.extracao_oracle_eficiente import descobrir_consultas_sql, executar_extracao_oracle
from interface_grafica.config import CNPJ_ROOT, SQL_DIR

from utilitarios.sql_catalog import list_sql_entries

from utilitarios.extrair_parametros import extrair_parametros_sql


@dataclass
class ResultadoPipeline:
    """Resultado da execucao completa do pipeline."""

    ok: bool
    cnpj: str
    mensagens: list[str] = field(default_factory=list)
    arquivos_gerados: list[str] = field(default_factory=list)
    erros: list[str] = field(default_factory=list)
    tempos: dict[str, float] = field(default_factory=dict)


@dataclass
class ResultadoGeracaoTabelas:
    """Resultado da fase de geracao de tabelas."""

    ok: bool
    geradas: list[str] = field(default_factory=list)
    erros: list[str] = field(default_factory=list)
    tempos: dict[str, float] = field(default_factory=dict)


TABELAS_DISPONIVEIS: list[dict[str, str]] = [
    {
        "id": "tb_documentos",
        "nome": "Consolidacao de Documentos",
        "descricao": "Unifica cabecalhos de NFe, NFCe e C100",
        "modulo": "tabela_documentos",
        "funcao": "gerar_tabela_documentos",
    },
    {
        "id": "item_unidades",
        "nome": "1. Item Unidades",
        "descricao": "Base completa item x unidade a partir de C170, Bloco H, NFe e NFCe",
        "modulo": "item_unidades",
        "funcao": "gerar_item_unidades",
    },
    {
        "id": "itens",
        "nome": "2. Itens",
        "descricao": "Consolida item_unidades em itens por descricao normalizada",
        "modulo": "itens",
        "funcao": "gerar_itens",
    },
    {
        "id": "descricao_produtos",
        "nome": "3. Descricao Produtos",
        "descricao": "Agrupa itens por descricao_normalizada e gera listas de atributos",
        "modulo": "descricao_produtos",
        "funcao": "gerar_descricao_produtos",
    },
    {
        "id": "produtos_final",
        "nome": "4. Produtos Final",
        "descricao": "Gera produtos_agrupados, mapa e produtos_final a partir da camada de descricoes",
        "modulo": "produtos_final_v2",
        "funcao": "gerar_produtos_final",
    },
    {
        "id": "fontes_produtos",
        "nome": "5. Fontes Agrupadas",
        "descricao": "Gera c170_agr, bloco_h_agr, NFe_agr e NFCe_agr com id_agrupado obrigatorio",
        "modulo": "fontes_produtos",
        "funcao": "gerar_fontes_produtos",
    },
    {
        "id": "fatores_conversao",
        "nome": "6. Fatores de Conversao",
        "descricao": "Calculo de multiplicadores de unidade baseado em preco medio",
        "modulo": "fatores_conversao",
        "funcao": "calcular_fatores_conversao",
    },
    {
        "id": "c170_xml",
        "nome": "7. C170 XML",
        "descricao": "Padroniza o C170 com apoio do XML de NFe/NFCe e fallback dos dados do proprio C170",
        "modulo": "c170_xml",
        "funcao": "gerar_c170_xml",
    },
    {
        "id": "c176_xml",
        "nome": "8. C176 XML",
        "descricao": "Reinterpreta o C176 usando a saida como referencia do id_agrupado e converte para unid_ref",
        "modulo": "c176_xml",
        "funcao": "gerar_c176_xml",
    },
    {
        "id": "movimentacao_estoque",
        "nome": "9. Movimentacao de Estoque (Final)",
        "descricao": "Gera a tabela final consolidada com pauta Sefin e inventario",
        "modulo": "movimentacao_estoque",
        "funcao": "gerar_movimentacao_estoque",
    },
    {
        "id": "calculos_mensais",
        "nome": "10. Calculos Mensais",
        "descricao": "Gera a aba mensal resumida a partir da movimentacao de estoque",
        "modulo": "calculos_mensais",
        "funcao": "gerar_calculos_mensais",
    },
    {
        "id": "calculos_anuais",
        "nome": "11. Calculos Anuais",
        "descricao": "Gera a aba anual resumida a partir da movimentacao de estoque",
        "modulo": "calculos_anuais",
        "funcao": "gerar_calculos_anuais",
    },
]


class ServicoExtracao:
    """Executa consultas SQL Oracle e salva os resultados como Parquet."""

    def __init__(self, consultas_dir: Path | list[Path] | None = None, cnpj_root: Path = CNPJ_ROOT):
        if consultas_dir is None:
            candidatos = [SQL_DIR]
        elif isinstance(consultas_dir, list):
            candidatos = [Path(item) for item in consultas_dir]
        else:
            candidatos = [Path(consultas_dir)]

        self.consultas_dirs = [diretorio for idx, diretorio in enumerate(candidatos) if diretorio not in candidatos[:idx]]
        self.consultas_dir = self.consultas_dirs[0] if self.consultas_dirs else SQL_DIR
        self.cnpj_root = cnpj_root

    def listar_consultas(self) -> list[str]:
        return [entry.sql_id for entry in list_sql_entries()]

    def pasta_cnpj(self, cnpj: str) -> Path:
        return self.cnpj_root / cnpj

    def pasta_parquets(self, cnpj: str) -> Path:
        pasta = self.pasta_cnpj(cnpj) / "arquivos_parquet"
        pasta.mkdir(parents=True, exist_ok=True)
        return pasta

    def pasta_produtos(self, cnpj: str) -> Path:
        pasta = self.pasta_cnpj(cnpj) / "analises" / "produtos"
        pasta.mkdir(parents=True, exist_ok=True)
        return pasta

    @staticmethod
    def sanitizar_cnpj(cnpj: str) -> str:
        digitos = re.sub(r"\D", "", cnpj or "")
        if len(digitos) not in {11, 14}:
            raise ValueError("Informe um CPF com 11 digitos ou um CNPJ com 14 digitos.")
        return digitos

    def apagar_dados_cnpj(self, cnpj: str) -> bool:
        import shutil

        pasta = self.pasta_cnpj(cnpj)
        if not pasta.exists():
            return False
        for sub in ("arquivos_parquet", "analises"):
            caminho = pasta / sub
            if caminho.exists():
                shutil.rmtree(caminho)
        return True

    def apagar_cnpj(self, cnpj: str) -> bool:
        import shutil

        pasta = self.pasta_cnpj(cnpj)
        if not pasta.exists():
            return False
        shutil.rmtree(pasta)
        return True

    def obter_data_entrega_reg0000(self, cnpj: str) -> str | None:
        pasta = self.pasta_parquets(cnpj)
        arquivo = pasta / f"reg_0000_{cnpj}.parquet"
        if not arquivo.exists():
            return None
        try:
            df = pl.read_parquet(arquivo, columns=["data_entrega"])
            if df.is_empty():
                return None
            maior_data = df.select(pl.col("data_entrega").max()).to_series()[0]
            if maior_data:
                try:
                    return maior_data.strftime("%d/%m/%Y")
                except AttributeError:
                    return str(maior_data)
        except Exception:
            return None
        return None

    @staticmethod
    def extrair_parametros(sql_text: str) -> set[str]:
        return extrair_parametros_sql(sql_text)

    @staticmethod
    def montar_binds(sql_text: str, valores: dict[str, Any]) -> dict[str, Any]:
        parametros = extrair_parametros_sql(sql_text)
        valores_lower = {k.lower(): v for k, v in valores.items()}
        binds: dict[str, Any] = {}
        for nome in parametros:
            binds[nome] = valores_lower.get(nome.lower())
        return binds

    def executar_consultas(
        self,
        cnpj: str,
        consultas: list[str | Path],
        data_limite: str | None = None,
        progresso: Callable[[str], None] | None = None,
    ) -> list[str]:
        def _msg(texto: str) -> None:
            if progresso:
                progresso(texto)

        cnpj = self.sanitizar_cnpj(cnpj)
        pasta = self.pasta_parquets(cnpj)
        arquivos: list[str] = []

        resultados = executar_extracao_oracle(
            cnpj_input=cnpj,
            data_limite_input=data_limite,
            consultas_selecionadas=consultas,
            pasta_saida_base=pasta,
            diretorios_sql=self.consultas_dirs,
            max_workers=1,
            progresso=_msg,
        )
        arquivos.extend(
            str(resultado.arquivo_saida)
            for resultado in resultados
            if resultado.ok and resultado.arquivo_saida is not None
        )

        return arquivos


class ServicoTabelas:
    """Executa as funcoes de geracao em src/transformacao."""

    PREFIXOS_ANALISE_LEGADOS = (
        "produtos_unidades_",
        "produtos_",
        "produtos_itens_",
    )
    TAGS_BRUTOS_LEGADOS = (
        "_produtos_",
        "_enriquecido_",
        "_sem_id_agrupado_",
    )

    @staticmethod
    def listar_tabelas() -> list[dict[str, str]]:
        return TABELAS_DISPONIVEIS[:]

    @staticmethod
    def limpar_arquivos_legados(cnpj: str) -> None:
        pasta_analises = CNPJ_ROOT / cnpj / "analises" / "produtos"
        pasta_brutos = CNPJ_ROOT / cnpj / "arquivos_parquet"

        if not pasta_analises.exists():
            pasta_analises = None

        if pasta_analises is not None:
            for path in pasta_analises.glob("*.parquet"):
                nome = path.name
                if nome.startswith(("produtos_agrupados_", "produtos_final_")):
                    continue
                if nome.startswith(ServicoTabelas.PREFIXOS_ANALISE_LEGADOS) or "_sem_id_agrupado_" in nome:
                    try:
                        path.unlink()
                    except Exception:
                        pass

        if pasta_brutos.exists():
            for path in pasta_brutos.glob("*.parquet"):
                if any(tag in path.name for tag in ServicoTabelas.TAGS_BRUTOS_LEGADOS):
                    try:
                        path.unlink()
                    except Exception:
                        pass

    @staticmethod
    def gerar_tabelas(
        cnpj: str,
        tabelas_selecionadas: list[str],
        progresso: Callable[[str], None] | None = None,
    ) -> ResultadoGeracaoTabelas:
        def _msg(texto: str) -> None:
            if progresso:
                progresso(texto)

        cnpj = re.sub(r"\D", "", cnpj)
        ServicoTabelas.limpar_arquivos_legados(cnpj)
        pasta_cnpj = CNPJ_ROOT / cnpj
        geradas: list[str] = []
        erros: list[str] = []
        tempos: dict[str, float] = {}

        ordem = [
            "tb_documentos",
            "item_unidades",
            "itens",
            "descricao_produtos",
            "produtos_final",
            "fontes_produtos",
            "fatores_conversao",
            "c170_xml",
            "c176_xml",
            "movimentacao_estoque",
            "calculos_mensais",
            "calculos_anuais",
        ]

        for tab_id in ordem:
            if tab_id not in tabelas_selecionadas:
                continue

            info = next((t for t in TABELAS_DISPONIVEIS if t["id"] == tab_id), None)
            if info is None:
                continue

            _msg(f"Gerando {info['nome']}...")
            inicio_etapa = perf_counter()
            try:
                funcao = _importar_funcao_tabela(info["modulo"], info["funcao"])
                resultado = funcao(cnpj, pasta_cnpj)
            except Exception as exc:
                tempos[tab_id] = perf_counter() - inicio_etapa
                erro = f"Erro ao gerar {info['nome']}: {exc}"
                erros.append(erro)
                _msg(erro)
                break

            tempos[tab_id] = perf_counter() - inicio_etapa
            if resultado:
                geradas.append(tab_id)
                _msg(f"OK {info['nome']} gerada com sucesso em {tempos[tab_id]:.2f}s.")
                continue

            erro = f"Geracao interrompida em {info['nome']}: a etapa retornou False."
            erros.append(erro)
            _msg(erro)
            break

        return ResultadoGeracaoTabelas(
            ok=not erros,
            geradas=geradas,
            erros=erros,
            tempos=tempos,
        )


def _importar_funcao_tabela(nome_modulo: str, nome_funcao: str) -> Callable:
    import importlib

    modulo = importlib.import_module(f"transformacao.{nome_modulo}")
    return getattr(modulo, nome_funcao)


class ServicoPipelineCompleto:
    """Orquestra extracao Oracle + geracao de tabelas."""

    def __init__(self):
        self.servico_extracao = ServicoExtracao()
        self.servico_tabelas = ServicoTabelas()

    def executar_completo(
        self,
        cnpj: str,
        consultas: list[str | Path],
        tabelas: list[str],
        data_limite: str | None = None,
        progresso: Callable[[str], None] | None = None,
    ) -> ResultadoPipeline:
        cnpj = ServicoExtracao.sanitizar_cnpj(cnpj)
        resultado = ResultadoPipeline(ok=True, cnpj=cnpj)

        def _msg(texto: str) -> None:
            resultado.mensagens.append(texto)
            if progresso:
                progresso(texto)

        if consultas:
            _msg(f"=== Fase 1: Extracao Oracle ({len(consultas)} consultas) ===")
            inicio_extracao = perf_counter()
            try:
                arquivos = self.servico_extracao.executar_consultas(
                    cnpj,
                    consultas,
                    data_limite,
                    _msg,
                )
                resultado.arquivos_gerados.extend(arquivos)
                resultado.tempos["extracao_oracle"] = perf_counter() - inicio_extracao
                _msg(f"Tempo da extracao Oracle: {resultado.tempos['extracao_oracle']:.2f}s")
            except Exception as exc:
                resultado.erros.append(f"Falha na extracao: {exc}")
                resultado.ok = False
                return resultado

        if tabelas:
            _msg(f"=== Fase 2: Geracao de tabelas ({len(tabelas)} selecionadas) ===")
            inicio_tabelas = perf_counter()
            try:
                resultado_tabelas = self.servico_tabelas.gerar_tabelas(cnpj, tabelas, _msg)
                resultado.arquivos_gerados.extend(resultado_tabelas.geradas)
                resultado.tempos.update({f"tabela::{k}": v for k, v in resultado_tabelas.tempos.items()})
                resultado.tempos["geracao_tabelas"] = perf_counter() - inicio_tabelas
                _msg(f"Tempo da geracao de tabelas: {resultado.tempos['geracao_tabelas']:.2f}s")
                if not resultado_tabelas.ok:
                    resultado.ok = False
                    resultado.erros.extend(resultado_tabelas.erros)
            except Exception as exc:
                resultado.erros.append(f"Falha na geracao de tabelas: {exc}")
                resultado.ok = False

        if resultado.ok:
            _msg(f"=== Pipeline concluido para CNPJ {cnpj} ===")
        else:
            _msg(f"=== Pipeline interrompido para CNPJ {cnpj} ===")

        return resultado
