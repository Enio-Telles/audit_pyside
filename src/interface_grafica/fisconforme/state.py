from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional


@dataclass
class CNPJRecord:
    """Representa um contribuinte exibido na grade da etapa de seleção."""

    seq: int
    cnpj: str
    razao_social: str = ""
    municipio: str = ""
    uf: str = ""
    situacao: str = ""
    origem: str = ""
    status: str = "Aguardando consulta"
    valido: bool = False
    nome_fantasia: str = ""
    regime: str = ""
    tooltip: str = ""
    erro: str = ""
    carregando: bool = False

    @property
    def municipio_uf(self) -> str:
        if self.municipio and self.uf:
            return f"{self.municipio}/{self.uf}"
        return self.municipio or self.uf or ""


@dataclass
class ProcessingResult:
    """Resultado de uma tentativa de geração de notificação."""

    cnpj: str
    sucesso: bool
    detalhe: str


@dataclass
class WizardState:
    """Estado único compartilhado entre as páginas do wizard."""

    db_config: Dict[str, str] = field(default_factory=dict)
    cnpj_records: List[CNPJRecord] = field(default_factory=list)
    auditor_data: Dict[str, str] = field(default_factory=dict)
    pdf_dsf: Optional[Path] = None
    periodo_inicio: str = "01/2021"
    periodo_fim: str = "12/2025"
    diretorio_saida: Optional[Path] = None
    logs_processamento: List[str] = field(default_factory=list)
    resultados_processamento: List[ProcessingResult] = field(default_factory=list)
    resumo_processamento: Dict[str, Any] = field(default_factory=dict)
    etapa_atual: int = 0

    def cnpjs_validos(self) -> List[str]:
        return [registro.cnpj for registro in self.cnpj_records if registro.valido]

    def resumo_sidebar(self) -> Dict[str, str]:
        total_cnpjs = len(self.cnpjs_validos())
        dsf = self.auditor_data.get("DSF", "—")
        auditor = self.auditor_data.get("AUDITOR", "—")
        periodo = f"{self.periodo_inicio} a {self.periodo_fim}"
        return {
            "CNPJs": str(total_cnpjs),
            "DSF": dsf or "—",
            "Auditor": auditor or "—",
            "Período": periodo,
        }

