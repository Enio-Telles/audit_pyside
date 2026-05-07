from scripts.audit_categorical_inventory import classify_column


def test_classify_invariant_is_never_categorical() -> None:
    audit = classify_column(
        "c170_xml",
        "id_agrupado",
        "String",
        1_000_000,
        10,
        0,
        ["id_agrupado_auto_abc"],
    )

    assert audit.recommendation == "PROIBIDO_CATEGORIZAR"
    assert audit.risk_flag == "fiscal_invariant"


def test_classify_cfop_as_enum() -> None:
    audit = classify_column("c170_xml", "cfop", "String", 1_000_000, 180, 0, ["5102"])

    assert audit.recommendation == "CATEGORICAL_EM_MEMORIA__ENUM"
    assert audit.dtype_polars_proposto == "pl.Enum(load('cfop_all'))"


def test_classify_ncm_as_dynamic_categorical() -> None:
    audit = classify_column("c170_xml", "ncm", "String", 1_000_000, 8_500, 0, ["12345678"])

    assert audit.recommendation == "CATEGORICAL_EM_MEMORIA__CATEGORICAL"
    assert audit.risk_flag == "ncm_changes_yearly"


def test_classify_boolean_field_as_boolean() -> None:
    audit = classify_column("tb_documentos", "indFinal", "String", 500_000, 2, 0, ["0", "1"])

    assert audit.recommendation == "BOOLEAN_EM_MEMORIA"
    assert audit.dtype_polars_proposto == "pl.Boolean"


def test_classify_ind_mov_inverted_as_enum_not_boolean() -> None:
    audit = classify_column("bloco_h", "IND_MOV", "String", 100_000, 2, 0, ["0", "1"])

    assert audit.recommendation == "CATEGORICAL_EM_MEMORIA__ENUM"
    assert audit.enum_source == "indicador_movimento_sped"
