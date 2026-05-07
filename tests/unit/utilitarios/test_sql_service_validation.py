import pytest
import re
from interface_grafica.services.sql_service import validate_oracle_identifier, ORACLE_IDENTIFIER_ALLOWED

def test_oracle_identifier_regex():
    assert ORACLE_IDENTIFIER_ALLOWED.match("MY_TABLE")
    assert ORACLE_IDENTIFIER_ALLOWED.match("table123")
    assert ORACLE_IDENTIFIER_ALLOWED.match("SCHEMA$NAME")
    assert ORACLE_IDENTIFIER_ALLOWED.match("TBL#NAME")
    assert ORACLE_IDENTIFIER_ALLOWED.match("12345678901234") # CNPJ as digits

    assert not ORACLE_IDENTIFIER_ALLOWED.match("table; drop table users")
    assert not ORACLE_IDENTIFIER_ALLOWED.match("table--")
    assert not ORACLE_IDENTIFIER_ALLOWED.match("table ")
    assert not ORACLE_IDENTIFIER_ALLOWED.match("table' OR '1'='1")

def test_validate_oracle_identifier_success():
    assert validate_oracle_identifier("VALID_NAME") == "VALID_NAME"
    assert validate_oracle_identifier("schema123") == "schema123"

def test_validate_oracle_identifier_failure():
    with pytest.raises(ValueError, match="Identificador Oracle invalido"):
        validate_oracle_identifier("invalid; name")

    with pytest.raises(ValueError, match="Identificador Oracle invalido"):
        validate_oracle_identifier("")

    with pytest.raises(ValueError, match="Identificador Oracle invalido"):
        validate_oracle_identifier("name with spaces")
