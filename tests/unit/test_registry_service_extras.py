from __future__ import annotations

from pathlib import Path

from interface_grafica.services.registry_service import RegistryService


def test_registry_upsert_new_entry(tmp_path: Path) -> None:
    svc = RegistryService(registry_file=tmp_path / "registry.json")
    record = svc.upsert("12345678000190")
    assert record.cnpj == "12345678000190"
    assert record.last_run_at is None


def test_registry_upsert_second_call_ran_now(tmp_path: Path) -> None:
    svc = RegistryService(registry_file=tmp_path / "registry.json")
    svc.upsert("12345678000190")
    record = svc.upsert("12345678000190", ran_now=True)
    assert record.last_run_at is not None


def test_registry_delete_existing(tmp_path: Path) -> None:
    svc = RegistryService(registry_file=tmp_path / "registry.json")
    svc.upsert("12345678000190")
    result = svc.delete_by_cnpj("12345678000190")
    assert result is True


def test_registry_delete_missing(tmp_path: Path) -> None:
    svc = RegistryService(registry_file=tmp_path / "registry.json")
    result = svc.delete_by_cnpj("99999999999999")
    assert result is False
