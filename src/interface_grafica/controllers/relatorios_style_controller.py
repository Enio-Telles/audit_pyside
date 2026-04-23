from __future__ import annotations

from PySide6.QtGui import QFont


class RelatoriosStyleControllerMixin:
    def _aba_mensal_foreground(self, row: dict, _col_name: str):
        entradas_desacob = float(row.get("entradas_desacob") or 0)
        icms_entr = float(row.get("ICMS_entr_desacob") or 0)
        if entradas_desacob > 0 or icms_entr > 0:
            return "#fff7ed"
        return "#f5f5f5"
    def _aba_mensal_background(self, row: dict, _col_name: str):
        entradas_desacob = float(row.get("entradas_desacob") or 0)
        icms_entr = float(row.get("ICMS_entr_desacob") or 0)
        if entradas_desacob > 0 or icms_entr > 0:
            return "#5b3a06"
        mes = int(row.get("mes") or 0)
        return "#1f1f1f" if (mes % 2) == 0 else "#262626"
    def _aba_anual_foreground(self, row: dict, _col_name: str):
        entradas_desacob = float(row.get("entradas_desacob") or 0)
        saidas_desacob = float(row.get("saidas_desacob") or 0)
        estoque_final_desacob = float(row.get("estoque_final_desacob") or 0)
        if entradas_desacob > 0 or saidas_desacob > 0 or estoque_final_desacob > 0:
            return "#fff7ed"
        return "#f5f5f5"
    def _aba_anual_background(self, row: dict, _col_name: str):
        entradas_desacob = float(row.get("entradas_desacob") or 0)
        saidas_desacob = float(row.get("saidas_desacob") or 0)
        estoque_final_desacob = float(row.get("estoque_final_desacob") or 0)
        if entradas_desacob > 0 or saidas_desacob > 0 or estoque_final_desacob > 0:
            return "#5b3a06"
        val = str(row.get("id_agregado", ""))
        import hashlib

        h = int(hashlib.md5(val.encode()).hexdigest(), 16)
        return "#1f1f1f" if (h % 2) == 0 else "#262626"
    def _mov_estoque_foreground(self, row: dict, _col_name: str):
        tipo = str(row.get("Tipo_operacao") or "").upper()
        if float(row.get("entr_desac_anual") or 0) > 0:
            return "#fdba74"
        if str(row.get("excluir_estoque", "")).strip().upper() in {
            "TRUE",
            "1",
            "S",
            "Y",
            "SIM",
        }:
            return "#94a3b8"
        if "ESTOQUE FINAL" in tipo:
            return "#fde68a"
        if "ESTOQUE INICIAL" in tipo:
            return "#bfdbfe"
        if "ENTRADA" in tipo:
            return "#93c5fd"
        if "SAIDA" in tipo:
            return "#fca5a5"
        return None
    def _mov_estoque_font(self, row: dict, _col_name: str):
        if float(row.get("entr_desac_anual") or 0) > 0:
            fonte = QFont()
            fonte.setBold(True)
            return fonte
        return None
    def _mov_estoque_background(self, row: dict, _col_name: str):
        tipo = str(row.get("Tipo_operacao") or "").upper()
        if float(row.get("entr_desac_anual") or 0) > 0:
            return "#431407"
        if str(row.get("excluir_estoque", "")).strip().upper() in {
            "TRUE",
            "1",
            "S",
            "Y",
            "SIM",
        }:
            return "#1e293b"
        if str(row.get("mov_rep", "")).strip().upper() in {
            "TRUE",
            "1",
            "S",
            "Y",
            "SIM",
        }:
            return "#111827"
        if "ESTOQUE FINAL" in tipo:
            return "#3f2f10"
        if "ESTOQUE INICIAL" in tipo:
            return "#0f172a"
        if "ENTRADA" in tipo:
            return "#10213f"
        if "SAIDA" in tipo:
            return "#3b1212"
        return None
    def _formatar_resumo_filtros(self, pares: list[tuple[str, str]]) -> str:
        ativos = [f"{rotulo}: {valor}" for rotulo, valor in pares if valor]
        return "Filtros ativos: " + (" | ".join(ativos) if ativos else "nenhum")
