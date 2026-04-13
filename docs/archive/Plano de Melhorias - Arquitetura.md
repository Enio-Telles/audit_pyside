# **Plano de Melhorias: Refatoração Arquitetural (Orientada a Abas)**

**Data:** Março de 2026  
**Última atualização:** 25/03/2026

**Objetivo:** Transitar o projeto "Fiscal Parquet Analyzer" para uma arquitetura modular de "1 Tabela = 1 Função = 1 Arquivo/Pasta", eliminando acoplamentos, melhorando a rastreabilidade e espelhando a estrutura de abas da interface de utilizador (UI).

## **Fase 1: Preparação do Ecossistema e Remoção de Anti-Patterns** ✅

*Foco: Transformar as pastas de scripts soltos em pacotes Python reais.*

* [x] **1.1. Criar ficheiros \_\_init\_\_.py:** em `src/`, `src/transformacao/`, `src/extracao/`, `src/utilitarios/`
* [x] **1.2. Limpar orquestrador\_pipeline.py:** Removido bloco `sys.path.insert`
* [x] **1.3. Atualizar Importações Globais:** 25 ficheiros convertidos para imports absolutos a partir de `src/`

## **Fase 2: Reestruturação de Diretórios em src/transformacao/** ✅

*Foco: Ficheiros movidos para subpastas de domínio com 18 proxy modules para backward compatibility.*

* [x] **2.1. Criar as novas pastas de domínio:**
  * `src/transformacao/tabelas_base/`
  * `src/transformacao/rastreabilidade_produtos/`
  * `src/transformacao/movimentacao_estoque_pkg/`
  * `src/transformacao/calculos_mensais_pkg/`
  * `src/transformacao/calculos_anuais_pkg/`
* [x] **2.2. Migrar ficheiros base:** 6 ficheiros para `tabelas_base/`
* [x] **2.3. Migrar ficheiros de Rastreabilidade:** 9 ficheiros para `rastreabilidade_produtos/`
* [x] **2.4. Modularizar movimentacao\_estoque em pacote:** 5 ficheiros em `movimentacao_estoque_pkg/` (`movimentacao_estoque.py`, `c170_xml.py`, `c176_xml.py`, `co_sefin.py`, `co_sefin_class.py`)
* [x] **2.5. Dividir movimentacao\_estoque.py internamente:** Extrair `_gerar_eventos_estoque` e `_calcular_saldo_estoque_anual` para `calculo_saldos.py`, e lógica de seleção/mapeamento para `mapeamento_fontes.py` ✅

## **Fase 3: Padronização de Contratos** ✅

* [x] **3.1. Refatorar funções principais:** Todas seguem `(cnpj: str, pasta_cnpj: Path | None = None) -> bool`
* [x] **3.2. Isolamento Total:** Zero imports de `interface_grafica` em `transformacao/`

## **Fase 4: Refatoração Dinâmica do Orquestrador** ✅

* [x] **4.1. Criar Registry:** `REGISTO_TABELAS` com 12 `_TabelaRegistro` (lazy import via `importlib`)
* [x] **4.2. Eliminar Hardcodes:** `_ordem_topologica()` com grafo de dependências

## **Fase 5: Otimização Direcionada** ✅

* [x] **5.1. Otimizar cálculo de saldos:** `_calcular_saldo_estoque_anual` usa NumPy arrays (substituiu `to_dicts()`)
* [x] **5.2. Assincronismo na UI:** Confirmado (`PipelineWorker`, `ServiceTaskWorker`, `QueryWorker`)
* [x] **5.3. Logs de Fallback:** Implementados em `fatores_conversao.py` e `precos_medios_produtos_final.py`

---

## Pendente

### 2.5: Subdivisão interna de `movimentacao_estoque.py`

O ficheiro `movimentacao_estoque_pkg/movimentacao_estoque.py` (41KB, ~980 linhas) contém múltiplas responsabilidades que podem ser extraídas:

| Ficheiro proposto | Responsabilidade |
|---|---|
| `calculo_saldos.py` | `_calcular_saldo_estoque_anual`, `_gerar_eventos_estoque` |
| `mapeamento_fontes.py` | `_process_source`, `_resolver_arquivo_origem`, `_parse_expression` |
| `movimentacao_estoque.py` | `gerar_movimentacao_estoque` (orquestração) |