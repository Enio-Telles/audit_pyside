import json

from interface_grafica.services.registry_service import RegistryService, CNPJRecord


def test_list_records_file_not_exists(tmp_path):
    registry_file = tmp_path / "cnpjs.json"
    service = RegistryService(registry_file=registry_file)
    assert service.list_records() == []


def test_list_records_empty_file(tmp_path):
    registry_file = tmp_path / "cnpjs.json"
    registry_file.write_text("[]", encoding="utf-8")
    service = RegistryService(registry_file=registry_file)
    assert service.list_records() == []


def test_list_records_sorted_multiple(tmp_path):
    registry_file = tmp_path / "cnpjs.json"
    data = [
        {"cnpj": "22222222000122", "added_at": "2023-01-03T10:00:00"},
        {"cnpj": "11111111000111", "added_at": "2023-01-02T10:00:00"},
        {"cnpj": "33333333000133", "added_at": "2023-01-01T10:00:00"},
    ]
    registry_file.write_text(json.dumps(data), encoding="utf-8")
    service = RegistryService(registry_file=registry_file)

    records = service.list_records()

    assert len(records) == 3
    assert records[0].cnpj == "11111111000111"
    assert records[0].added_at == "2023-01-02T10:00:00"
    assert records[1].cnpj == "22222222000122"
    assert records[1].added_at == "2023-01-03T10:00:00"
    assert records[2].cnpj == "33333333000133"
    assert records[2].added_at == "2023-01-01T10:00:00"
    assert all(isinstance(r, CNPJRecord) for r in records)


def test_list_records_with_last_run_at(tmp_path):
    registry_file = tmp_path / "cnpjs.json"
    data = [
        {
            "cnpj": "11111111000111",
            "added_at": "2023-01-01T10:00:00",
            "last_run_at": "2023-01-01T11:00:00",
        },
    ]
    registry_file.write_text(json.dumps(data), encoding="utf-8")
    service = RegistryService(registry_file=registry_file)

    records = service.list_records()
    assert len(records) == 1
    assert records[0].cnpj == "11111111000111"
    assert records[0].last_run_at == "2023-01-01T11:00:00"
