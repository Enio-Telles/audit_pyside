from pathlib import Path
import os
import sys

import polars as pl


sys.path.insert(0, str(Path("src").resolve()))

import interface_grafica.services.aggregation_service as aggregation_service_module
from interface_grafica.services.aggregation_service import ServicoAgregacao


def test_recalcular_valores_totais_persiste_medias_sem_duplicar_descricao(monkeypatch, tmp_path: Path):
    cnpj = "12345678000190"
    pasta_prod = tmp_path / cnpj / "analises" / "produtos"
    pasta_prod.mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr(aggregation_service_module, "CNPJ_ROOT", tmp_path)
    monkeypatch.setattr(aggregation_service_module, "registrar_evento_performance", lambda *args, **kwargs: None)

    pl.DataFrame(
        {
            "id_agrupado": ["AGR_1", "AGR_2"],
            "descr_padrao": ["Produto A", "Produto B"],
            "lista_chave_produto": [["desc_1", "desc_2"], ["desc_3"]],
        }
    ).write_parquet(pasta_prod / f"produtos_agrupados_{cnpj}.parquet")

    pl.DataFrame(
        {
            "id_descricao": ["desc_1", "desc_2", "desc_3"],
            "descricao_normalizada": ["PRODUTO A", "PRODUTO A", "PRODUTO B"],
        }
    ).write_parquet(pasta_prod / f"descricao_produtos_{cnpj}.parquet")

    pl.DataFrame(
        {
            "descricao": ["Produto A", "Produto B"],
            "compras": [100.0, 0.0],
            "qtd_compras": [10.0, 0.0],
            "vendas": [60.0, 0.0],
            "qtd_vendas": [3.0, 0.0],
        }
    ).write_parquet(pasta_prod / f"item_unidades_{cnpj}.parquet")

    servico = ServicoAgregacao()

    assert servico.recalcular_valores_totais(cnpj, reprocessar_referencias=False)

    df = pl.read_parquet(pasta_prod / f"produtos_agrupados_{cnpj}.parquet").sort("id_agrupado")
    row_a = df.row(0, named=True)
    row_b = df.row(1, named=True)

    assert row_a["id_agrupado"] == "AGR_1"
    assert row_a["total_compras"] == 100.0
    assert row_a["qtd_compras_total"] == 10.0
    assert row_a["preco_medio_compra"] == 10.0
    assert row_a["total_vendas"] == 60.0
    assert row_a["qtd_vendas_total"] == 3.0
    assert row_a["preco_medio_venda"] == 20.0
    assert row_a["total_entradas"] == 100.0
    assert row_a["total_saidas"] == 60.0
    assert row_a["total_movimentacao"] == 160.0

    assert row_b["id_agrupado"] == "AGR_2"
    assert row_b["total_compras"] == 0.0
    assert row_b["qtd_compras_total"] == 0.0
    assert row_b["preco_medio_compra"] is None
    assert row_b["total_vendas"] == 0.0
    assert row_b["qtd_vendas_total"] == 0.0
    assert row_b["preco_medio_venda"] is None
    assert row_b["total_entradas"] == 0.0
    assert row_b["total_saidas"] == 0.0
    assert row_b["total_movimentacao"] == 0.0


def test_agregar_linhas_recalcula_padroes_com_colunas_item_unidades(monkeypatch, tmp_path: Path):
    cnpj = "12345678000190"
    pasta_prod = tmp_path / cnpj / "analises" / "produtos"
    pasta_prod.mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr(aggregation_service_module, "CNPJ_ROOT", tmp_path)
    monkeypatch.setattr(aggregation_service_module, "registrar_evento_performance", lambda *args, **kwargs: None)

    pl.DataFrame(
        {
            "id_agrupado": ["AGR_1", "AGR_2"],
            "descr_padrao": ["Produto A", "Produto A"],
            "ncm_padrao": ["1000", "1000"],
            "cest_padrao": ["2000", "2000"],
            "gtin_padrao": ["789", "789"],
            "lista_co_sefin": [["2290"], ["2290"]],
            "co_sefin_padrao": ["2290", "2290"],
            "lista_unidades": [["UN"], ["CX"]],
            "co_sefin_divergentes": [False, False],
            "fontes": [["c170"], ["nfe"]],
            "lista_chave_produto": [["desc_1"], ["desc_2"]],
        }
    ).write_parquet(pasta_prod / f"produtos_agrupados_{cnpj}.parquet")

    pl.DataFrame(
        {
            "id_descricao": ["desc_1", "desc_2"],
            "descricao_normalizada": ["PRODUTO A", "PRODUTO A"],
            "descricao": ["Produto A", "Produto A"],
            "lista_desc_compl": [["Garrafa 1L"], ["Caixa 12x1L"]],
            "lista_ncm": [["1000"], ["1000"]],
            "lista_cest": [["2000"], ["2000"]],
            "lista_gtin": [["789"], ["789"]],
            "fontes": [["c170"], ["nfe"]],
        }
    ).write_parquet(pasta_prod / f"descricao_produtos_{cnpj}.parquet")

    pl.DataFrame(
        {
            "descricao": ["Produto A", "Produto A"],
            "ncm": ["1000", "1000"],
            "cest": ["2000", "2000"],
            "gtin": ["789", "789"],
            "co_sefin_item": ["2290", "2290"],
            "fontes": [["c170"], ["nfe"]],
        }
    ).write_parquet(pasta_prod / f"item_unidades_{cnpj}.parquet")

    servico = ServicoAgregacao()
    monkeypatch.setattr(servico, "recalcular_valores_totais", lambda *args, **kwargs: True)
    monkeypatch.setattr(servico, "recalcular_produtos_final", lambda *args, **kwargs: True)
    monkeypatch.setattr(servico, "recalcular_referencias_produtos", lambda *args, **kwargs: True)

    resultado = servico.agregar_linhas(cnpj, ["AGR_1", "AGR_2"])

    assert resultado["success"] is True

    df = pl.read_parquet(pasta_prod / f"produtos_agrupados_{cnpj}.parquet")
    assert df.height == 1
    row = df.row(0, named=True)
    assert row["descr_padrao"] == "Produto A"
    assert row["id_agrupado"] == "AGR_1"
    assert row["ncm_padrao"] == "1000"
    assert row["cest_padrao"] == "2000"
    assert row["gtin_padrao"] == "789"
    assert row["lista_ncm"] == ["1000"]
    assert row["lista_cest"] == ["2000"]
    assert row["lista_gtin"] == ["789"]
    assert row["lista_descricoes"] == ["Produto A"]
    assert row["lista_desc_compl"] == ["Caixa 12x1L", "Garrafa 1L"]
    assert row["ids_origem_agrupamento"] == ["AGR_1", "AGR_2"]
    assert row["lista_itens_agrupados"] == ["Produto A"]

    historico = servico.ler_linhas_log(cnpj)
    assert historico[-1]["tipo"] == "agregacao"
    assert historico[-1]["id_destino"] == "AGR_1"
    assert historico[-1]["ids_unidos"] == ["AGR_1", "AGR_2"]
    assert len(historico[-1]["grupos_origem"]) == 2


def test_garantir_metricas_regenera_schema_legado_com_descricoes(monkeypatch, tmp_path: Path):
    cnpj = "12345678000190"
    pasta_prod = tmp_path / cnpj / "analises" / "produtos"
    pasta_prod.mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr(aggregation_service_module, "CNPJ_ROOT", tmp_path)
    monkeypatch.setattr(aggregation_service_module, "registrar_evento_performance", lambda *args, **kwargs: None)

    pl.DataFrame(
        {
            "id_agrupado": ["AGR_1"],
            "lista_chave_produto": [["desc_1"]],
            "descr_padrao": ["Produto A"],
        }
    ).write_parquet(pasta_prod / f"produtos_agrupados_{cnpj}.parquet")

    pl.DataFrame({"id_descricao": ["desc_1"]}).write_parquet(pasta_prod / f"descricao_produtos_{cnpj}.parquet")
    pl.DataFrame({"id_descricao": ["desc_1"], "id_agrupado": ["AGR_1"]}).write_parquet(
        pasta_prod / f"produtos_final_{cnpj}.parquet"
    )

    chamadas: list[str] = []

    def fake_inicializar(cnpj_recebido: str) -> bool:
        chamadas.append(f"inicializar:{cnpj_recebido}")
        pl.DataFrame(
            {
                "id_agrupado": ["AGR_1"],
                "lista_chave_produto": [["desc_1"]],
                "descr_padrao": ["Produto A"],
                "lista_ncm": [["1000"]],
                "lista_cest": [["2000"]],
                "lista_gtin": [["789"]],
                "lista_descricoes": [["Produto A"]],
                "lista_desc_compl": [["Caixa 12x1L"]],
            }
        ).write_parquet(pasta_prod / f"produtos_agrupados_{cnpj}.parquet")
        return True

    def fake_id_agrupados(cnpj_recebido: str) -> bool:
        chamadas.append(f"id_agrupados:{cnpj_recebido}")
        pl.DataFrame(
            {
                "id_agrupado": ["AGR_1"],
                "descr_padrao": ["Produto A"],
                "lista_descricoes": [["Produto A"]],
                "lista_desc_compl": [["Caixa 12x1L"]],
            }
        ).write_parquet(pasta_prod / f"id_agrupados_{cnpj}.parquet")
        return True

    monkeypatch.setattr(aggregation_service_module, "inicializar_produtos_agrupados", fake_inicializar)
    monkeypatch.setattr(aggregation_service_module, "gerar_id_agrupados", fake_id_agrupados)

    servico = ServicoAgregacao()
    monkeypatch.setattr(
        servico,
        "recalcular_todos_padroes",
        lambda *args, **kwargs: chamadas.append("recalcular_padroes") or True,
    )
    monkeypatch.setattr(
        servico,
        "recalcular_valores_totais",
        lambda *args, **kwargs: chamadas.append("recalcular_totais") or True,
    )

    assert servico.garantir_metricas_tabela_agregadas(cnpj) is True

    schema_agr = pl.read_parquet(pasta_prod / f"produtos_agrupados_{cnpj}.parquet", n_rows=1).columns
    schema_id = pl.read_parquet(pasta_prod / f"id_agrupados_{cnpj}.parquet", n_rows=1).columns

    assert "lista_descricoes" in schema_agr
    assert "lista_desc_compl" in schema_agr
    assert "lista_descricoes" in schema_id
    assert "lista_desc_compl" in schema_id
    assert chamadas == [
        f"inicializar:{cnpj}",
        f"id_agrupados:{cnpj}",
        "recalcular_padroes",
        "recalcular_totais",
    ]


def test_reverter_agrupamento_restabelece_grupos_origem(monkeypatch, tmp_path: Path):
    cnpj = "12345678000190"
    pasta_prod = tmp_path / cnpj / "analises" / "produtos"
    pasta_prod.mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr(aggregation_service_module, "CNPJ_ROOT", tmp_path)
    monkeypatch.setattr(aggregation_service_module, "registrar_evento_performance", lambda *args, **kwargs: None)

    pl.DataFrame(
        {
            "id_agrupado": ["AGR_1", "AGR_2"],
            "descr_padrao": ["Produto A", "Produto B"],
            "lista_chave_produto": [["desc_1"], ["desc_2"]],
            "lista_descricoes": [["Produto A"], ["Produto B"]],
            "lista_desc_compl": [["Comp A"], ["Comp B"]],
            "ids_origem_agrupamento": [["AGR_1"], ["AGR_2"]],
            "lista_itens_agrupados": [["Produto A"], ["Produto B"]],
        }
    ).write_parquet(pasta_prod / f"produtos_agrupados_{cnpj}.parquet")

    pl.DataFrame(
        {
            "id_descricao": ["desc_1", "desc_2"],
            "descricao_normalizada": ["PRODUTO A", "PRODUTO B"],
            "descricao": ["Produto A", "Produto B"],
            "lista_desc_compl": [["Comp A"], ["Comp B"]],
            "lista_ncm": [["1000"], ["2000"]],
            "lista_cest": [["3000"], ["4000"]],
            "lista_gtin": [["789"], ["456"]],
            "fontes": [["c170"], ["nfe"]],
        }
    ).write_parquet(pasta_prod / f"descricao_produtos_{cnpj}.parquet")

    pl.DataFrame(
        {
            "descricao": ["Produto A", "Produto B"],
            "ncm": ["1000", "2000"],
            "cest": ["3000", "4000"],
            "gtin": ["789", "456"],
            "co_sefin_item": ["2290", "2291"],
            "fontes": [["c170"], ["nfe"]],
        }
    ).write_parquet(pasta_prod / f"item_unidades_{cnpj}.parquet")

    servico = ServicoAgregacao()
    monkeypatch.setattr(servico, "recalcular_valores_totais", lambda *args, **kwargs: True)
    monkeypatch.setattr(servico, "recalcular_produtos_final", lambda *args, **kwargs: True)
    monkeypatch.setattr(servico, "recalcular_referencias_produtos", lambda *args, **kwargs: True)

    servico.agregar_linhas(cnpj, ["AGR_1", "AGR_2"])
    resultado = servico.reverter_agrupamento(cnpj, "AGR_1")

    assert resultado["success"] is True
    assert resultado["ids_restaurados"] == ["AGR_1", "AGR_2"]

    df = pl.read_parquet(pasta_prod / f"produtos_agrupados_{cnpj}.parquet").sort("id_agrupado")
    assert df.get_column("id_agrupado").to_list() == ["AGR_1", "AGR_2"]

    historico = servico.ler_linhas_log(cnpj)
    entradas_agregacao = [item for item in historico if item.get("tipo") == "agregacao"]
    entradas_desagrupacao = [item for item in historico if item.get("tipo") == "desagrupacao"]
    assert entradas_agregacao[-1]["revertida"] is True
    assert entradas_desagrupacao[-1]["ids_restaurados"] == ["AGR_1", "AGR_2"]


def test_artefatos_estoque_defasados_identifica_derivados_antigos_ou_ausentes(monkeypatch, tmp_path: Path):
    cnpj = "12345678000190"
    pasta_prod = tmp_path / cnpj / "analises" / "produtos"
    pasta_prod.mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr(aggregation_service_module, "CNPJ_ROOT", tmp_path)
    monkeypatch.setattr(aggregation_service_module, "registrar_evento_performance", lambda *args, **kwargs: None)

    arq_mov = pasta_prod / f"mov_estoque_{cnpj}.parquet"
    arq_mensal = pasta_prod / f"aba_mensal_{cnpj}.parquet"
    pl.DataFrame({"id": [1]}).write_parquet(arq_mov)
    pl.DataFrame({"id": [1]}).write_parquet(arq_mensal)

    os.utime(arq_mensal, ns=(1_000_000_000, 1_000_000_000))
    os.utime(arq_mov, ns=(2_000_000_000, 2_000_000_000))

    servico = ServicoAgregacao()

    assert servico.artefatos_estoque_defasados(cnpj) == ["calculos_mensais", "calculos_anuais"]


def test_recalcular_resumos_estoque_executa_apenas_etapas_defasadas(monkeypatch, tmp_path: Path):
    cnpj = "12345678000190"
    pasta_prod = tmp_path / cnpj / "analises" / "produtos"
    pasta_prod.mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr(aggregation_service_module, "CNPJ_ROOT", tmp_path)
    monkeypatch.setattr(aggregation_service_module, "registrar_evento_performance", lambda *args, **kwargs: None)

    arq_mov = pasta_prod / f"mov_estoque_{cnpj}.parquet"
    arq_mensal = pasta_prod / f"aba_mensal_{cnpj}.parquet"
    arq_anual = pasta_prod / f"aba_anual_{cnpj}.parquet"
    pl.DataFrame({"id": [1]}).write_parquet(arq_mov)
    pl.DataFrame({"id": [1]}).write_parquet(arq_mensal)
    pl.DataFrame({"id": [1]}).write_parquet(arq_anual)

    os.utime(arq_mensal, ns=(1_000_000_000, 1_000_000_000))
    os.utime(arq_mov, ns=(2_000_000_000, 2_000_000_000))
    os.utime(arq_anual, ns=(3_000_000_000, 3_000_000_000))

    chamadas: list[str] = []

    monkeypatch.setattr(
        aggregation_service_module,
        "gerar_calculos_mensais",
        lambda cnpj_recebido: chamadas.append(f"mensal:{cnpj_recebido}") or True,
    )
    monkeypatch.setattr(
        aggregation_service_module,
        "gerar_calculos_anuais",
        lambda cnpj_recebido: chamadas.append(f"anual:{cnpj_recebido}") or True,
    )

    servico = ServicoAgregacao()

    assert servico.recalcular_resumos_estoque(cnpj) is True
    assert chamadas == [f"mensal:{cnpj}"]
