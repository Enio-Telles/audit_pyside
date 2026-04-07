from __future__ import annotations

import re
from pathlib import Path

from fastapi import APIRouter, BackgroundTasks, HTTPException
from pydantic import BaseModel


from interface_grafica.services.registry_service import RegistryService
from utilitarios.sql_catalog import list_sql_entries, normalize_sql_id, resolve_sql_path

router = APIRouter()
registry = RegistryService()

_pipeline_status: dict[str, dict] = {}


class PipelineRequest(BaseModel):
    cnpj: str
    consultas: list[str] = []
    tabelas: list[str] = []
    data_limite: str | None = None
    incluir_extracao: bool = False
    incluir_processamento: bool = False


def _sanitize(cnpj: str) -> str:
    return re.sub(r"\D", "", cnpj or "")


def _resolver_consultas(req: PipelineRequest) -> list[str]:
    if req.consultas:
        return req.consultas
    if req.incluir_extracao:
        return ["*"]
    return []


def _resolver_tabelas(req: PipelineRequest) -> list[str]:
    if req.tabelas:
        return req.tabelas
    if req.incluir_processamento:
        return ["*"]
    return []


def _calcular_percentual(
    status: str,
    etapas_concluidas: int,
    total_etapas: int,
) -> int:
    if status in {"done", "error"}:
        return 100
    if total_etapas <= 0:
        return 0
    return min(99, int((etapas_concluidas / total_etapas) * 100))


def _criar_status(
    status: str,
    *,
    progresso: list[str] | None = None,
    erros: list[str] | None = None,
    etapas_concluidas: int = 0,
    total_etapas: int = 0,
    etapa_atual: str | None = None,
    item_atual: str | None = None,
) -> dict:
    return {
        "status": status,
        "progresso": progresso or [],
        "erros": erros or [],
        "percentual": _calcular_percentual(status, etapas_concluidas, total_etapas),
        "etapas_concluidas": etapas_concluidas,
        "total_etapas": total_etapas,
        "etapa_atual": etapa_atual,
        "item_atual": item_atual,
    }


def _resolver_execucao(
    consultas: list[str],
    tabelas: list[str],
) -> tuple[list[Path], list[str]]:
    from interface_grafica.services.pipeline_funcoes_service import (
        ServicoPipelineCompleto,
    )

    svc = ServicoPipelineCompleto()
    consultas = [str(item) for item in consultas]
    consultas_disponiveis = list_sql_entries()
    consultas_por_nome = {entry.path.name.lower(): entry.path for entry in consultas_disponiveis}
    consultas_por_id = {entry.sql_id.lower(): entry.path for entry in consultas_disponiveis}
    tabelas_disponiveis = svc.servico_tabelas.listar_tabelas()
    tabelas_por_id = {item["id"]: item["id"] for item in tabelas_disponiveis}

    if consultas == ["*"]:
        consultas_resolvidas = [entry.path for entry in consultas_disponiveis]
    else:
        consultas_resolvidas = []
        for nome in consultas:
            sql_id = normalize_sql_id(nome)
            if sql_id and sql_id.lower() in consultas_por_id:
                consultas_resolvidas.append(consultas_por_id[sql_id.lower()])
                continue
            caminho = consultas_por_nome.get(nome.lower())
            if caminho is not None:
                consultas_resolvidas.append(caminho)

    if tabelas == ["*"]:
        tabelas_resolvidas = [item["id"] for item in tabelas_disponiveis]
    else:
        tabelas_resolvidas = [tab for tab in tabelas if tab in tabelas_por_id]

    if consultas and not consultas_resolvidas:
        raise ValueError("Nenhuma consulta SQL valida foi selecionada para extracao.")
    if tabelas and not tabelas_resolvidas:
        raise ValueError("Nenhuma tabela valida foi selecionada para processamento.")

    return consultas_resolvidas, tabelas_resolvidas


def _atualizar_status_execucao(
    cnpj: str,
    total_etapas: int,
    mensagem: str,
) -> None:
    atual = _pipeline_status.get(
        cnpj,
        _criar_status("running", total_etapas=total_etapas),
    )
    progresso = [*atual["progresso"], mensagem]
    etapas_concluidas = int(atual.get("etapas_concluidas", 0))
    etapa_atual = atual.get("etapa_atual")
    item_atual = atual.get("item_atual")

    if "Fase 1:" in mensagem:
        etapa_atual = "extracao"
        item_atual = None
    elif "Fase 2:" in mensagem:
        etapa_atual = "processamento"
        item_atual = None
    elif mensagem.startswith("Executando "):
        etapa_atual = "extracao"
        item_atual = mensagem.removeprefix("Executando ").removesuffix("...")
    elif mensagem.startswith("Gerando "):
        etapa_atual = "processamento"
        item_atual = mensagem.removeprefix("Gerando ").removesuffix("...")
    elif mensagem.startswith("OK "):
        etapas_concluidas = min(etapas_concluidas + 1, total_etapas)
        item_atual = None

    _pipeline_status[cnpj] = _criar_status(
        "running",
        progresso=progresso,
        erros=atual["erros"],
        etapas_concluidas=etapas_concluidas,
        total_etapas=total_etapas,
        etapa_atual=etapa_atual,
        item_atual=item_atual,
    )


def _run_pipeline(
    cnpj: str,
    consultas: list[Path],
    tabelas: list[str],
    data_limite: str | None,
    total_etapas: int,
) -> None:
    _pipeline_status[cnpj] = _criar_status(
        "running",
        total_etapas=total_etapas,
        etapa_atual="preparacao",
    )
    try:
        from interface_grafica.services.pipeline_funcoes_service import (
            ServicoPipelineCompleto,
        )

        svc = ServicoPipelineCompleto()
        result = svc.executar_completo(
            cnpj=cnpj,
            consultas=consultas,
            tabelas=tabelas,
            data_limite=data_limite,
            progresso=lambda msg: _atualizar_status_execucao(cnpj, total_etapas, msg),
        )
        if result.ok:
            registry.upsert(cnpj, ran_now=True)
            _pipeline_status[cnpj] = _criar_status(
                "done",
                progresso=_pipeline_status[cnpj]["progresso"],
                erros=[],
                etapas_concluidas=total_etapas,
                total_etapas=total_etapas,
                etapa_atual="concluido",
            )
        else:
            _pipeline_status[cnpj] = _criar_status(
                "error",
                progresso=_pipeline_status[cnpj]["progresso"],
                erros=result.erros,
                etapas_concluidas=int(_pipeline_status[cnpj]["etapas_concluidas"]),
                total_etapas=total_etapas,
                etapa_atual="erro",
                item_atual=_pipeline_status[cnpj].get("item_atual"),
            )
    except Exception as exc:
        atual = _pipeline_status.get(cnpj, _criar_status("error", total_etapas=total_etapas))
        _pipeline_status[cnpj] = _criar_status(
            "error",
            progresso=atual["progresso"],
            erros=[str(exc)],
            etapas_concluidas=int(atual.get("etapas_concluidas", 0)),
            total_etapas=total_etapas,
            etapa_atual="erro",
            item_atual=atual.get("item_atual"),
        )


@router.post("/run")
def run_pipeline(req: PipelineRequest, background_tasks: BackgroundTasks):
    cnpj = _sanitize(req.cnpj)
    if not cnpj:
        raise HTTPException(400, "CNPJ invalido")
    if _pipeline_status.get(cnpj, {}).get("status") == "running":
        raise HTTPException(409, "Pipeline ja em execucao para este CNPJ")

    incluir_extracao = req.incluir_extracao
    incluir_processamento = req.incluir_processamento
    if (
        not incluir_extracao
        and not incluir_processamento
        and not req.consultas
        and not req.tabelas
    ):
        incluir_extracao = True
        incluir_processamento = True

    req_normalizado = req.model_copy(
        update={
            "incluir_extracao": incluir_extracao,
            "incluir_processamento": incluir_processamento,
        }
    )
    consultas = _resolver_consultas(req_normalizado)
    tabelas = _resolver_tabelas(req_normalizado)
    if not consultas and not tabelas:
        raise HTTPException(400, "Nenhuma etapa do pipeline foi selecionada")

    try:
        consultas_resolvidas, tabelas_resolvidas = _resolver_execucao(consultas, tabelas)
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc

    total_etapas = len(consultas_resolvidas) + len(tabelas_resolvidas)
    _pipeline_status[cnpj] = _criar_status(
        "queued",
        total_etapas=total_etapas,
        etapa_atual="fila",
    )
    background_tasks.add_task(
        _run_pipeline,
        cnpj,
        consultas_resolvidas,
        tabelas_resolvidas,
        req.data_limite,
        total_etapas,
    )
    return {"cnpj": cnpj, "status": "queued"}


@router.get("/status/{cnpj}")
def get_status(cnpj: str):
    cnpj = _sanitize(cnpj)
    return _pipeline_status.get(cnpj, _criar_status("idle"))
