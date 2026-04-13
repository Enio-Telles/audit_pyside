from pathlib import Path

from transformacao.ressarcimento_st_pkg.credito_icms_item import gerar_credito_icms_item
from transformacao.ressarcimento_st_pkg.insumos_oracle import (
    gerar_fronteira_item,
    gerar_insumos_oracle_ressarcimento,
    gerar_rateio_frete_cte,
    gerar_st_material_ate_2022,
    gerar_vigencia_sefin,
)
from transformacao.ressarcimento_st_pkg.ressarcimento_st_conciliacao import gerar_ressarcimento_st_conciliacao
from transformacao.ressarcimento_st_pkg.ressarcimento_st_item import gerar_ressarcimento_st_item
from transformacao.ressarcimento_st_pkg.ressarcimento_st_mensal import gerar_ressarcimento_st_mensal


def executar_pipeline_ressarcimento_st(cnpj: str, pasta_cnpj: Path | None = None) -> bool:
    ok_insumos = gerar_insumos_oracle_ressarcimento(cnpj, pasta_cnpj)
    ok_credito = gerar_credito_icms_item(cnpj, pasta_cnpj) if ok_insumos else False
    ok_item = gerar_ressarcimento_st_item(cnpj, pasta_cnpj) if ok_credito else False
    ok_mensal = gerar_ressarcimento_st_mensal(cnpj, pasta_cnpj) if ok_item else False
    ok_conc = gerar_ressarcimento_st_conciliacao(cnpj, pasta_cnpj) if ok_mensal else False
    return ok_insumos and ok_credito and ok_item and ok_mensal and ok_conc
