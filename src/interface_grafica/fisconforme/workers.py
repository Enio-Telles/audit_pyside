from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Dict, List, Optional

from PySide6.QtCore import QObject, QRunnable, QThread, Signal

from .extracao import limpar_cnpj
from .extracao_cadastral import extrair_e_salvar_cadastral
from .gerar_notificacoes import DIR_SAIDA_NOTIFICACOES, gerar_notificacao_para_cnpj

logger = logging.getLogger(__name__)

_CACHE_DADOS_CADASTRAIS: Dict[str, Dict[str, Any]] = {}


class LookupSignals(QObject):
    finished = Signal(str, object)


class BuscaRazaoSocialTask(QRunnable):
    """Busca dados cadastrais com reaproveitamento de cache local."""

    def __init__(self, cnpj: str):
        super().__init__()
        self.cnpj = cnpj
        self.signals = LookupSignals()
        self.setAutoDelete(False)

    def run(self):
        if self.cnpj in _CACHE_DADOS_CADASTRAIS:
            self.signals.finished.emit(self.cnpj, _CACHE_DADOS_CADASTRAIS[self.cnpj])
            return

        try:
            dados = extrair_e_salvar_cadastral(self.cnpj)
            if dados:
                _CACHE_DADOS_CADASTRAIS[self.cnpj] = dados
            self.signals.finished.emit(self.cnpj, dados)
        except Exception as exc:  # pragma: no cover - caminho operacional
            logger.error("Erro ao buscar dados cadastrais para %s: %s", self.cnpj, exc)
            self.signals.finished.emit(self.cnpj, None)


class WorkerThread(QThread):
    """Processamento em lote das notificações, desacoplado da UI."""

    progresso = Signal(int, int)
    log_msg = Signal(str)
    cnpj_resultado = Signal(str, bool, str)
    concluido = Signal(dict)

    def __init__(
        self,
        lista_cnpjs: List[str],
        dados_manuais: Dict[str, str],
        periodo_analise: tuple,
        arquivo_dsf: Optional[Path] = None,
        diretorio_saida: Optional[Path] = None,
    ):
        super().__init__()
        self.lista_cnpjs = lista_cnpjs
        self.dados_manuais = dados_manuais
        self.periodo_analise = periodo_analise
        self.arquivo_dsf = arquivo_dsf
        self.diretorio_saida = diretorio_saida or DIR_SAIDA_NOTIFICACOES
        self._cancelar = False

    def cancelar(self):
        self._cancelar = True

    def run(self):
        total = len(self.lista_cnpjs)
        dsf_num = self.dados_manuais.get("DSF", "").strip()
        dir_saida = self.diretorio_saida / dsf_num if dsf_num else self.diretorio_saida
        dir_saida.mkdir(parents=True, exist_ok=True)

        self.log_msg.emit(f"Iniciando processamento de {total} CNPJ(s)...")
        self.log_msg.emit(f"Período: {self.periodo_analise[0]} a {self.periodo_analise[1]}")
        if dsf_num:
            self.log_msg.emit(f"DSF: {dsf_num}")
        self.log_msg.emit(f"Saída: {dir_saida}")

        resumo = {
            "total": total,
            "sucessos": 0,
            "falhas": 0,
            "resultados": {},
            "arquivos_gerados": [],
            "diretorio_saida": dir_saida,
        }

        for i, cnpj in enumerate(self.lista_cnpjs, start=1):
            if self._cancelar:
                self.log_msg.emit("⚠️ Processamento cancelado pelo usuário.")
                break

            self.progresso.emit(i, total)
            cnpj_limpo = limpar_cnpj(cnpj)
            self.log_msg.emit(f"\n{'─' * 50}")
            self.log_msg.emit(f"[{i}/{total}] Processando CNPJ: {cnpj_limpo}")

            try:
                resultado = gerar_notificacao_para_cnpj(
                    cnpj=cnpj,
                    dados_manuais=self.dados_manuais,
                    dados_tabela=None,
                    diretorio_saida=dir_saida,
                    forcar_reatribuicao=False,
                    periodo_analise=self.periodo_analise,
                )
                resumo["resultados"][cnpj_limpo] = resultado

                if resultado["sucesso"]:
                    resumo["sucessos"] += 1
                    resumo["arquivos_gerados"].append(resultado["arquivo_saida"])
                    detalhe = str(resultado["arquivo_saida"])
                    self.log_msg.emit(f"  ✅ Notificação gerada: {detalhe}")
                    self.cnpj_resultado.emit(cnpj_limpo, True, detalhe)
                else:
                    resumo["falhas"] += 1
                    erro = resultado.get("erro", "Erro desconhecido")
                    etapa = resultado.get("etapa_falha", "?")
                    detalhe = f"[{etapa}] {erro}"
                    self.log_msg.emit(f"  ❌ Falha na etapa '{etapa}': {erro}")
                    self.cnpj_resultado.emit(cnpj_limpo, False, detalhe)
            except Exception as exc:  # pragma: no cover - caminho operacional
                resumo["falhas"] += 1
                self.log_msg.emit(f"  ❌ Erro inesperado: {exc}")
                self.cnpj_resultado.emit(cnpj_limpo, False, str(exc))

        self.log_msg.emit(f"\n{'═' * 50}")
        self.log_msg.emit(
            f"Concluído: {resumo['sucessos']} sucesso(s), {resumo['falhas']} falha(s)"
        )
        self.concluido.emit(resumo)
