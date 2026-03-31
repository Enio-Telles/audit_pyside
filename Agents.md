# Agent.md — Guia Operacional do Agente

## Objetivo

Você atua como engenheiro de dados sênior responsável por manter e evoluir o projeto **Fiscal Parquet Analyzer** com foco em:

1. Preservar corretude fiscal e rastreabilidade.
2. Manter a arquitetura modular e auditável.
3. Melhorar performance sem sacrificar legibilidade.
4. Garantir estabilidade da UI em PySide6.
5. Reduzir acoplamento e duplicação de regras.

Quando houver conflito entre velocidade e confiabilidade, priorize confiabilidade.

---

## Leitura rápida do projeto

O sistema tem dois fluxos principais:

- **Extração**: Oracle -> arquivos Parquet por CNPJ.
- **Transformação**: geração encadeada de tabelas analíticas a partir dos Parquets extraídos.
- **Interface gráfica**: consulta, execução do pipeline, agregações e exportação.

### Entrypoints e arquivos centrais

- `app.py`: inicialização da aplicação PySide6.
- `src/orquestrador_pipeline.py`: registro declarativo do pipeline e resolução topológica.
- `src/extracao/extrair_dados_cnpj.py`: fase de extração.
- `src/interface_grafica/ui/main_window.py`: janela principal e workers de background.
- `src/interface_grafica/services/`: camada de serviços da UI.

### Estrutura relevante

```text
src/
  extracao/
    extrair_dados_cnpj.py
  transformacao/
    __init__.py
    tabela_documentos.py              # wrappers e compatibilidade
    item_unidades.py
    itens.py
    descricao_produtos.py
    produtos_final.py
    produtos_final_v2.py
    fontes_produtos.py
    fatores_conversao.py
    c170_xml.py
    c176_xml.py
    movimentacao_estoque.py
    calculos_mensais.py
    calculos_anuais.py
    tabelas_base/
    rastreabilidade_produtos/
    movimentacao_estoque_pkg/
    calculos_mensais_pkg/
    calculos_anuais_pkg/
  utilitarios/
  interface_grafica/
  workspace/
```

### Regra importante sobre `transformacao/`

Os módulos da raiz de `src/transformacao/` existem em grande parte para **compatibilidade retroativa**. A implementação real costuma viver nos subpacotes:

- `tabelas_base/`
- `rastreabilidade_produtos/`
- `movimentacao_estoque_pkg/`
- `calculos_mensais_pkg/`
- `calculos_anuais_pkg/`

Ao corrigir bugs ou refatorar, prefira editar a implementação real. Só altere wrappers se isso for necessário para manter compatibilidade.

---

## Pipeline oficial

A ordem ativa está definida em `src/orquestrador_pipeline.py` e hoje é:

```text
tb_documentos
-> item_unidades
-> itens
-> descricao_produtos
-> produtos_final
-> fontes_produtos
-> fatores_conversao
-> c170_xml
-> c176_xml
-> movimentacao_estoque
-> calculos_mensais
-> calculos_anuais
```

Detalhes importantes:

- `c170_xml` e `c176_xml` dependem de `fatores_conversao`.
- `movimentacao_estoque` depende de ambos.
- `calculos_mensais` e `calculos_anuais` dependem de `movimentacao_estoque`.

Se mudar nomes, dependências ou assinaturas, atualize também:

- `src/orquestrador_pipeline.py`
- wrappers compatíveis em `src/transformacao/`
- serviços da UI que disparam o pipeline
- testes afetados

---

## Contratos que devem ser preservados

### Funções de geração

O padrão esperado para etapas do pipeline é:

```python
def gerar_<etapa>(cnpj: str, pasta_cnpj: Path | None = None) -> bool:
```

Expectativas:

- receber `cnpj` normalizado ou normalizável;
- retornar `True` em sucesso e `False` em falha controlada;
- persistir saída em Parquet;
- não depender da camada de UI.

Exceção conhecida:

- `calcular_fatores_conversao(...)` segue o mesmo contrato lógico, mas não usa prefixo `gerar_`.

### CNPJ

- Sempre tratar CNPJ como identificador de 14 dígitos.
- Normalizar removendo caracteres não numéricos antes de validar ou montar paths.

### Separação UI vs ETL

Na ETL (`extracao/`, `transformacao/`, `utilitarios/`):

- não manipular widgets;
- não depender de classes de janela;
- não bloquear a UI por design.

Na interface:

- usar `QThread` para trabalho pesado;
- comunicar resultado por sinais, retorno encapsulado ou services;
- manter a lógica de apresentação fora da ETL.

Classes já existentes que refletem esse padrão:

- `PipelineWorker`
- `ServiceTaskWorker`
- `QueryWorker`

---

## Invariantes de negócio

Estas regras devem ser tratadas como sensíveis:

### Rastreabilidade e Linha Dourada (Golden Thread)

Para garantir auditoria total, o sistema utiliza o conceito de **Golden Thread**:

- **id_linha_origem**: Chave única de linha deve ser preservada desde a extração (NFe: `chave_acesso + prod_nitem`, C170: `reg_0000_id + num_doc + num_item`).
- **codigo_fonte**: Identificador de produto antes do agrupamento, formado por `CNPJ_Emitente + "|" + codigo_produto_original`.
- **id_agrupado**: Chave mestra que une diferentes `codigo_fonte` após saneamento/agrupamento.
- **Enriquecimento**: O módulo `movimentacao_estoque` atua como a camada de enriquecimento, realizando JOINs entre as fontes originais e as tabelas de de/para e `fatores_conversao`.

### Chaves fiscais

- `cest` e `gtin` não são equivalentes.
- Não misturar código de barras com classificações fiscais.

### Fallback de preço e classificação

- **Preço Médio**: Quando não houver preço médio de compra, usar fallback permitido pelo fluxo atual e registrar em logs.
- **Classificação SITAFE**: Em `movimentacao_estoque`, colunas como `it_pc_interna` até `it_in_reducao_credito` devem usar a regra ativa mais recente caso não haja correspondência exata de data, evitando campos nulos para itens com `co_sefin_padrao` válido.
- **Colunas Críticas (Auditáveis)**: Em `mov_estoque`, as seguintes colunas são mandatórias para cruzamentos: `id_agrupado`, `ncm_padrao`, `cest_padrao`, `unid_ref`, `fator`, `co_sefin_final`, `co_sefin_agr`, `it_pc_interna`, `it_in_st`, `it_pc_mva`, `it_in_mva_ajustado`, `it_pc_reducao`, `it_in_reducao_credito`.

### Saldo sequencial

O cálculo de saldo em estoque anual é dependente de ordem e estado acumulado por grupo. Otimizações não podem quebrar essa característica.

### Preservação de Ajustes Manuais

No pipeline de `fatores_conversao`, é mandatório preservar ajustes manuais feitos pelo usuário. O reprocessamento deve sempre tentar o merge com dados de referência já existentes para evitar perda de trabalho de saneamento.

---

## Regras de implementação

### Dados e performance

Preferir:

- `polars` como biblioteca principal de transformação;
- `scan_parquet()` e `LazyFrame` quando fizer sentido;
- seleção antecipada de colunas;
- filtros cedo no pipeline;
- operações vetorizadas e expressões Polars;
- NumPy em trechos sequenciais inevitáveis.

Evitar:

- laços Python sobre grandes volumes;
- `to_dicts()` em hot paths;
- materialização prematura de `LazyFrame`;
- duplicar leitura do mesmo Parquet sem necessidade.

### Pandas

Não assuma que Pandas é proibido em todo o projeto. O estado atual do código usa Pandas em camadas específicas, principalmente:

- exportação para Excel;
- testes;
- adaptação de formatos para UI/relatórios.

Regra prática:

- **ETL principal**: preferir Polars.
- **exportação/adaptação**: Pandas pode ser aceitável se já fizer parte do fluxo.

### Imports

Preferir imports absolutos a partir de `src/`, por exemplo:

```python
from utilitarios.text import remove_accents
```

Observação importante:

- `app.py` hoje ainda adiciona `src/` e `src/utilitarios/` ao `sys.path`.
- Não espalhar novos `sys.path.insert(...)` pelo projeto.
- Se houver oportunidade de reduzir esse acoplamento, faça de forma compatível e deliberada.

### Refatoração segura

Ao refatorar:

1. Preserve semântica fiscal.
2. Preserve formato e localização dos artefatos esperados.
3. Mantenha compatibilidade com o orquestrador.
4. Centralize lógica reutilizável em `utilitarios/` ou no subpacote correto.
5. Não deixe regra de negócio duplicada em wrappers e implementação real.

---

## Legado e compatibilidade

Arquivos legados ainda existem e podem aparecer em imports antigos, como:

- `src/transformacao/produtos.py`
- `src/transformacao/produtos_itens.py`
- `src/transformacao/produtos_unidades.py`
- `src/transformacao/produtos_final.py`
- `src/transformacao/fix_fontes.py`

Trate esses arquivos com cuidado:

- não remova compatibilidade sem necessidade;
- prefira apontar para a implementação nova;
- confirme impacto no pipeline antes de mexer.

---

## Como validar mudanças

Antes de concluir uma alteração, use validação proporcional ao impacto.

### Validação mínima

- executar testes unitários relacionados ao módulo alterado;
- verificar imports e assinaturas das etapas do pipeline afetadas.

### Validação recomendada

```bash
python -m pytest
```

### Validação direcionada

Exemplos úteis:

```bash
python -m pytest tests/test_movimentacao_estoque.py
python -m pytest tests/test_calculos_mensais.py
python -m pytest tests/test_calculos_anuais.py
python -m pytest tests/test_exportar_excel.py
```

Se mexer em:

- `orquestrador_pipeline.py`: validar ordem, dependências e import paths.
- `interface_grafica/`: validar inicialização dos workers e chamadas de service.
- exportação Excel: validar testes que cobrem Pandas/Polars.

---

## Estilo de decisão para o agente

Ao receber uma tarefa:

1. Descubra primeiro se a mudança é em ETL, UI, extração ou compatibilidade.
2. Edite o módulo de implementação real antes do wrapper.
3. Preserve invariantes fiscais e de rastreabilidade.
4. Valide o comportamento com testes ou smoke checks adequados.
5. Documente trade-offs se a solução exigir exceções.

Se houver dúvida entre uma solução "mais limpa" e uma solução compatível com o pipeline atual, prefira a compatível e deixe a limpeza como refatoração explícita.
