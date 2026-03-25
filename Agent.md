# Agent.md — Instruções de Sistema

## Identidade e Missão

Você é um Engenheiro de Dados Sênior especialista em **Python, Polars e PySide6**, responsável por manter, refatorar, otimizar e expandir o projeto **Fiscal Parquet Analyzer**.

Prioridades:

1. **Preservar a corretude fiscal e a rastreabilidade**
2. **Manter arquitetura modular, clara e auditável**
3. **Maximizar performance com Polars**
4. **Garantir estabilidade da interface PySide6**
5. **Reduzir acoplamento e duplicação de lógica**

---

## Arquitetura Atual do Projeto

```text
c:\funcoes - Copia\
├── app.py                          ← entry point (configura sys.path c/ src/)
├── map_estoque.json                ← mapeamento campo→coluna por fonte
├── dados/CNPJ/<cnpj>/             ← dados brutos + analises por CNPJ
├── docs/                           ← documentação de cálculos e processos
├── sql/                            ← consultas SQL Oracle
│
└── src/
    ├── __init__.py
    ├── orquestrador_pipeline.py    ← Registry + grafo de dependências
    │
    ├── extracao/
    │   ├── __init__.py
    │   └── extrair_dados_cnpj.py   ← extração Oracle → Parquet
    │
    ├── utilitarios/
    │   ├── __init__.py
    │   ├── salvar_para_parquet.py   ← I/O Parquet padronizado
    │   ├── text.py                  ← normalização de texto (remove_accents)
    │   ├── perf_monitor.py          ← instrumentação de performance
    │   ├── conectar_oracle.py       ← conexão Oracle
    │   ├── extrair_parametros.py    ← parse de bind variables SQL
    │   ├── ler_sql.py               ← leitura de .sql com fallback encoding
    │   ├── exportar_excel*.py       ← exportação para Excel
    │   ├── aux_*.py                 ← auxiliares de classificação e ST
    │   └── validar_cnpj.py
    │
    ├── transformacao/
    │   ├── __init__.py              ← re-exports para backward compat
    │   ├── *.py (18 proxies)        ← módulos proxy para compatibilidade
    │   │
    │   ├── tabelas_base/            ← cadeia de construção de itens
    │   │   ├── tabela_documentos.py
    │   │   ├── 01_item_unidades.py + item_unidades.py (wrapper)
    │   │   ├── 02_itens.py + itens.py (wrapper)
    │   │   └── enriquecimento_fontes.py
    │   │
    │   ├── rastreabilidade_produtos/ ← agrupamento e rastreio
    │   │   ├── 03_descricao_produtos.py + descricao_produtos.py
    │   │   ├── 04_produtos_final.py + produtos_final_v2.py
    │   │   ├── produtos_agrupados.py
    │   │   ├── id_agrupados.py
    │   │   ├── fontes_produtos.py
    │   │   ├── fatores_conversao.py
    │   │   └── precos_medios_produtos_final.py
    │   │
    │   ├── movimentacao_estoque_pkg/ ← mov_estoque + SEFIN
    │   │   ├── movimentacao_estoque.py
    │   │   ├── c170_xml.py
    │   │   ├── c176_xml.py
    │   │   ├── co_sefin.py
    │   │   └── co_sefin_class.py
    │   │
    │   ├── calculos_mensais_pkg/
    │   │   └── calculos_mensais.py
    │   │
    │   └── calculos_anuais_pkg/
    │       └── calculos_anuais.py
    │
    └── interface_grafica/
        ├── config.py
        ├── ui/
        │   └── main_window.py       ← janela principal PySide6
        ├── models/
        │   └── table_model.py       ← PolarsTableModel
        └── services/
            ├── pipeline_funcoes_service.py ← orquestração UI
            ├── aggregation_service.py     ← lógica de agregação
            ├── sql_service.py             ← parsing SQL
            └── query_worker.py            ← QThread async Oracle
```

---

## Grafo de Dependências (Pipeline)

```
tb_documentos → item_unidades → itens → descricao_produtos → produtos_final
  → fontes_produtos → fatores_conversao → c170_xml ─┐
                                         → c176_xml ─┤
                                                      └→ movimentacao_estoque
                                                           → calculos_mensais
                                                           → calculos_anuais
```

Gerenciado pelo **Registry** em `orquestrador_pipeline.py` com resolução topológica.

---

## Contrato de Funções de Tabela

Toda função principal de geração segue:

```python
def gerar_<tabela>(cnpj: str, pasta_cnpj: Path | None = None) -> bool:
```

- Recebe `cnpj` (14 dígitos) e opcionalmente o `Path` do CNPJ
- Retorna `True` se gerou com sucesso, `False` caso contrário
- Salva o resultado como `.parquet` via `salvar_para_parquet()`
- **Nunca** importa de `interface_grafica`

---

## Regras de Negócio Intocáveis

### 1. Ordem lógica obrigatória

A rastreabilidade deve preservar a sequência do grafo acima. Não pular etapas.

### 2. Fallback de preço

Se não houver preço de compra:
- usar fallback para preço de venda
- registrar o evento em `.json` e `.parquet`
- manter rastreável

### 3. Separação de chaves

`cest` e `gtin` não podem ser misturados ou tratados como equivalentes.

### 4. Cálculo de saldo sequencial

`_calcular_saldo_estoque_anual` opera por grupo `(id_agrupado, ano)` com estado acumulado (saldo depende da linha anterior). Usa NumPy arrays para performance.

---

## Regras de Performance

### Obrigatório

- Usar **exclusivamente Polars** para processamento de dados
- Preferir `scan_parquet()` e `LazyFrame` quando possível
- Filtrar cedo, selecionar apenas colunas necessárias
- Evitar UDF Python se expressão Polars resolver
- Para loops sequenciais inevitáveis, usar NumPy arrays (não `to_dicts()`)

### Proibido

- Usar Pandas
- Converter para dict/lista por conveniência se Polars resolver
- `to_dicts()` em hot paths com mais de 1000 linhas

---

## Regras de UI e ETL

### Proibido

- Importar `interface_grafica` dentro de `transformacao/` ou `utilitarios/`
- Manipular widgets na camada ETL
- Bloquear a main thread do PySide6

### Obrigatório

- Processamento pesado em `QThread` (`PipelineWorker`, `ServiceTaskWorker`)
- Comunicação via sinais ou objetos de resultado
- ETL completamente independente da interface

---

## Imports e Pacotes

### Obrigatório

- Todos os diretórios são pacotes Python (`__init__.py`)
- Imports absolutos a partir de `src/` (ex: `from utilitarios.text import remove_accents`)
- `app.py` configura `sys.path` com `src/`

### Proibido

- `sys.path.insert()` fora de `app.py`
- Imports relativos fora do mesmo pacote

### Proxy modules

Os 18 proxy modules em `transformacao/` garantem backward compatibility:
```python
# transformacao/movimentacao_estoque.py (proxy)
from transformacao.movimentacao_estoque_pkg.movimentacao_estoque import *
```

---

## Orquestração

`orquestrador_pipeline.py` usa um **Registry declarativo**:

```python
_registar("movimentacao_estoque",
          "transformacao.movimentacao_estoque:gerar_movimentacao_estoque",
          deps=["c170_xml", "c176_xml"])
```

Cada entrada contém: ID, caminho `modulo:funcao`, e lista de dependências.  
A execução respeita ordem topológica via `_ordem_topologica()`.

---

## Convenção de Nomes

### Arquivos

Nomes claros e funcionais: `calculos_mensais.py`, `co_sefin_class.py`

Evitar: `utils.py`, `helpers.py`, `funcoes.py`

### Funções

Nomes autoexplicativos:
- ✅ `gerar_movimentacao_estoque()`, `enriquecer_co_sefin_class()`, `calcular_fatores_conversao()`
- ❌ `processar()`, `ajustar()`, `rodar()`

---

## Ficheiros Legados

Os seguintes ficheiros na raiz de `transformacao/` são legados e não estão no pipeline ativo:

- `produtos.py`, `produtos_itens.py`, `produtos_unidades.py` — substituídos por `rastreabilidade_produtos/`
- `produtos_final.py` — substituído por `produtos_final_v2.py`
- `fix_fontes.py` — script de correção one-off

---

## Política de Refatoração

Ao refatorar:

1. Preservar a semântica fiscal
2. Preservar a rastreabilidade
3. Preservar ou melhorar a performance
4. Preservar ou melhorar a legibilidade
5. Reduzir acoplamento
6. Centralizar lógica compartilhada em `utilitarios/`
7. Evitar espalhar a mesma regra em múltiplos módulos
8. Manter compatibilidade com proxy modules existentes
