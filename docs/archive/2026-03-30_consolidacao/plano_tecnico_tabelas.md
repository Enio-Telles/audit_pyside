# Plano Tecnico de Evolucao das Tabelas

Este documento transforma o plano funcional de melhoria de leitura das tabelas em um plano tecnico por arquivo, com foco inicial na aba `Estoque`.

## Objetivos do primeiro lote

1. Padronizar a base de exibicao das tabelas.
2. Melhorar visibilidade e contraste.
3. Tornar filtros mais transparentes para o usuario.
4. Habilitar ordenacao reutilizavel no modelo comum.

## Arquivos e responsabilidades

### `src/interface_grafica/models/table_model.py`

Responsabilidade:

- modelo generico usado pelos `QTableView`
- exibicao de dados
- selecao com checkbox
- edicao de colunas permitidas
- ordenacao reutilizavel
- estilo visual orientado por conteudo

Evolucao prevista:

- consolidar `foreground_resolver` e `background_resolver`
- adicionar `sort()` no modelo base
- expor `get_dataframe()` para manter compatibilidade entre telas
- preparar futura extensao para:
  - fonte em negrito por coluna
  - alinhamento por tipo
  - tooltip customizado

### `src/interface_grafica/ui/main_window.py`

Responsabilidade:

- construcao das abas
- leitura dos parquets
- filtros e sinais da UI
- regras de destaque por conteudo

Evolucao prevista:

- organizar a aba `Estoque` com subabas claras
- manter `mov_estoque` e `aba_anual` apontando para `analises/produtos`
- aplicar estilo por linha na `mov_estoque`
- mostrar resumo de filtros ativos
- usar ordenacao do modelo base
- preparar futuros presets de colunas e filtros salvos

### `src/interface_grafica/services/parquet_service.py`

Responsabilidade:

- leitura de parquet
- filtros estruturados
- paginação da aba principal

Evolucao prevista:

- ampliar operadores de filtro reutilizaveis nas abas de estoque
- concentrar filtros numericos e textuais reutilizaveis
- avaliar uso nas subabas `Estoque` para reduzir logica duplicada de filtro manual

### `docs/plano_tecnico_tabelas.md`

Responsabilidade:

- registrar arquitetura de UI de tabelas
- documentar lotes e proximos passos

## Primeiro lote implementado

### Base de tabela

- `PolarsTableModel` ganhou:
  - `get_dataframe()`
  - `sort()`
  - suporte mantido a resolvers de foreground/background

### Aba `Estoque`

- leitura corrigida das tabelas:
  - `mov_estoque_<cnpj>.parquet`
  - `aba_anual_<cnpj>.parquet`
- filtros alinhados ao schema real
- resumo visual de filtros ativos em:
  - `Tabela mov_estoque`
  - `Tabela anual`
- contraste reforcado por tipo de movimentacao
- destaque de linhas com:
  - `entr_desac_anual > 0`
  - `excluir_estoque`
  - `mov_rep`

## Proximo lote recomendado

1. Extrair um helper comum para filtros locais de subabas.
2. Padronizar ordenacao inicial por aba.
3. Criar presets de colunas para `mov_estoque` e `aba_anual`.
4. Adicionar destaques numericos:
   - saldo negativo
   - custo medio zero
   - desacobertadas
5. Levar o mesmo padrao visual para `Consulta` e `Agregacao`.
