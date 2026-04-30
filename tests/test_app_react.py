import app_react


def test_api_status_contract():
    assert app_react._api_status() == {
        "ok": True,
        "versao": "0.4.0",
        "servidor": "app_react",
    }


def test_normalizar_cpf_cnpj_removes_masking():
    assert app_react._normalizar_cpf_cnpj("12.345.678/0001-90") == "12345678000190"
