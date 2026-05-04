"""Constantes do differential harness: invariantes fiscais e chaves por etapa."""

INVARIANTES_FISCAIS: tuple[str, ...] = (
    "id_agrupado",
    "id_agregado",
    "__qtd_decl_final_audit__",
    "q_conv",
    "q_conv_fisica",
)

CHAVES_POR_ETAPA: dict[str, tuple[str, ...]] = {
    "nfe_agr": ("chave_acesso", "prod_nitem"),
    "nfce_agr": ("chave_acesso", "prod_nitem"),
    "c170_agr": ("chave_nfe_c100", "co_item"),
    "bloco_h_agr": ("chave_acesso", "prod_nitem"),
    "produtos_final": ("id_descricao",),
    "fatores_conversao": ("id_agrupado", "unid"),
    "movimentacao_estoque": ("chave_acesso", "prod_nitem", "id_agrupado"),
    "calculos_mensais": ("id_agregado", "Ano", "Mes"),
    "calculos_anuais": ("id_agregado", "Ano"),
    "calculos_periodos": ("id_agregado", "cod_per"),
}

FONTES_AUDITADAS: tuple[str, ...] = ("nfe", "nfce", "c170", "bloco_h")
