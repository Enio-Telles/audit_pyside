# Plano de Otimizacao e Melhorias

Este plano consolida melhorias tecnicas para o projeto **Fiscal Parquet Analyzer** com base no estado atual do codigo e nas diretrizes de [`Agents.md`](c:\Sistema_pysisde\Agents.md).

O objetivo nao e "modernizar por modernizar". As melhorias abaixo priorizam:

1. preservar corretude fiscal e rastreabilidade;
2. manter compatibilidade com o pipeline oficial;
3. melhorar performance onde o ganho e real;
4. evitar refatoracoes amplas com risco desnecessario na UI e no ETL.

---

## 1. Premissas obrigatorias

Antes de qualquer mudanca, estas regras de `Agents.md` devem orientar a execucao:

- nao quebrar a ordem oficial do pipeline em `src/orquestrador_pipeline.py`;
- editar a implementacao real antes dos wrappers em `src/transformacao/`;
- manter `cest` e `gtin` como atributos distintos;
- nao acoplar ETL com a camada PySide6;
- preferir Polars no ETL principal;
- aceitar Pandas apenas em fluxos onde ele ja e parte legitima do processo, como exportacao Excel e testes;
- validar mudancas de forma proporcional ao risco.

---

## 2. Diagnostico do estado atual

### 2.1. Arquitetura relevante hoje

O pipeline oficial atual e:

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

As implementacoes reais estao concentradas principalmente em:

- `src/transformacao/tabelas_base/`
- `src/transformacao/rastreabilidade_produtos/`
- `src/transformacao/movimentacao_estoque_pkg/`
- `src/transformacao/calculos_mensais_pkg/`
- `src/transformacao/calculos_anuais_pkg/`

### 2.2. Pontos fortes ja existentes

- a aba principal de consulta ja usa `ParquetService` com `scan_parquet()`, filtros e pagina;
- a UI ja usa `QThread` nos fluxos criticos com `PipelineWorker`, `ServiceTaskWorker` e `QueryWorker`;
- existe instrumentacao de performance em `src/utilitarios/perf_monitor.py`;
- o pipeline tem dependencias declarativas e previsiveis.

### 2.3. Gargalos ainda presentes

- muitos modulos ETL ainda usam `pl.read_parquet()` de forma eager mesmo quando filtram ou selecionam poucas colunas;
- varias abas da UI ainda carregam datasets inteiros em memoria com `pl.read_parquet()`;
- parte da logica de agregacao ainda percorre `to_dicts()` em pontos de alto custo;
- nao ha um padrao unico de validacao de schema para entradas e artefatos intermediarios;
- o plano anterior tratava algumas iniciativas como se nao existissem, especialmente paginacao e leitura lazy na camada de consulta.

---

## 3. Principios de priorizacao

As melhorias devem seguir esta ordem:

### 3.1. Primeiro: reduzir custo onde ha impacto direto

Priorizar modulos que:

- leem arquivos grandes;
- filtram ou projetam poucas colunas;
- sao executados em cadeia no pipeline;
- afetam abertura de telas e recarga de tabelas.

### 3.2. Segundo: fortalecer contratos

Priorizar validacoes e observabilidade onde:

- ha risco de quebrar corretude fiscal;
- ha arquivos intermediarios compartilhados entre etapas;
- falhas silenciosas em background dificultam suporte.

### 3.3. Terceiro: padronizar qualidade

Introduzir ferramentas de lint, tipagem e checks automatizados sem bloquear o fluxo de trabalho com uma migracao grande de uma vez.

---

## 4. Plano de execucao

## 4.1. Passo a passo operacional

Este e o roteiro sugerido para execucao pratica, em ordem.

### Etapa 1. Congelar contratos antes de otimizar

Passos:

1. mapear quais artefatos cada etapa do pipeline consome;
2. listar colunas minimas obrigatorias por artefato;
3. registrar essas dependencias no codigo antes de mexer em performance.

Entregaveis:

- utilitarios de validacao de schema;
- primeiras integracoes em etapas centrais do pipeline;
- testes unitarios dos validadores.

Status desta rodada:

- implementado utilitario `src/utilitarios/validacao_schema.py`;
- integrado em `03_descricao_produtos.py`, `04_produtos_final.py` e `movimentacao_estoque.py`;
- adicionados testes em `tests/test_validacao_schema.py`.

### Etapa 2. Medir antes de trocar eager por lazy

Passos:

1. identificar modulo com maior custo de leitura;
2. medir tempo atual;
3. migrar apenas a leitura candidata;
4. medir novamente;
5. manter a mudanca so se houver ganho e preservacao de semantica.

Entregaveis:

- diff pequeno por modulo;
- antes/depois de tempo;
- observacao sobre risco residual.

### Etapa 3. Expandir uso de `ParquetService` na UI

Passos:

1. localizar telas que fazem `pl.read_parquet()` para listagem;
2. separar casos de navegacao dos casos de processamento integral;
3. reaproveitar `ParquetService` nos fluxos de tabela;
4. validar filtros, ordenacao e recarga.

Entregaveis:

- telas com leitura lazy/paginada;
- reducao de consumo de memoria;
- sem regressao em selecao e recarga.

### Etapa 4. Reduzir hot paths com `to_dicts()`

Passos:

1. localizar pontos de `to_dicts()` em caminhos frequentes;
2. classificar se sao administrativos ou de alto volume;
3. substituir os de alto volume por Polars/NumPy;
4. comparar desempenho.

Entregaveis:

- lista de hot paths removidos;
- benchmark simples;
- testes de regressao funcional.

### Etapa 5. Fechar a malha de validacao

Passos:

1. expandir validacao de schema para artefatos mais sensiveis;
2. padronizar mensagens de falha;
3. registrar contexto em logs de erro e performance;
4. revisar testes afetados.

Entregaveis:

- contratos minimos protegidos;
- falhas mais explicativas;
- menor tempo de diagnostico.

## Fase 1. Otimizacao de leitura e materializacao no ETL

### Objetivo

Reduzir I/O e uso de memoria nas etapas de transformacao sem alterar semantica fiscal.

### Escopo prioritario

- `src/transformacao/rastreabilidade_produtos/03_descricao_produtos.py`
- `src/transformacao/rastreabilidade_produtos/04_produtos_final.py`
- `src/transformacao/rastreabilidade_produtos/fontes_produtos.py`
- `src/transformacao/rastreabilidade_produtos/fatores_conversao.py`
- `src/transformacao/movimentacao_estoque_pkg/c170_xml.py`
- `src/transformacao/movimentacao_estoque_pkg/c176_xml.py`
- `src/transformacao/movimentacao_estoque_pkg/movimentacao_estoque.py`
- `src/transformacao/calculos_mensais_pkg/calculos_mensais.py`
- `src/transformacao/calculos_anuais_pkg/calculos_anuais.py`

### Acoes

- substituir `pl.read_parquet()` por `pl.scan_parquet()` quando houver:
  - filtro antecipado;
  - selecao de poucas colunas;
  - joins em pipeline lazy;
  - leitura unica seguida de `collect()` tardio.
- manter `pl.read_parquet()` quando o algoritmo exigir:
  - acesso repetido em memoria;
  - loops sequenciais dependentes de ordem;
  - uso intensivo de arrays NumPy;
  - mutacoes sucessivas em `DataFrame` eager.
- reduzir colunas lidas nas etapas de apoio e enriquecimento.

### Criterios de aceite

- nenhuma mudanca na ordem do pipeline;
- nenhum artefato final muda de nome ou localizacao;
- testes existentes continuam passando;
- tempo de execucao de pelo menos uma etapa pesada melhora de forma mensuravel.

### Risco principal

Migracao cega para lazy em trechos sequenciais pode degradar legibilidade ou introduzir `collect()` desnecessario. A mudanca deve ser pontual e medida.

---

## Fase 2. Otimizacao da aba de agregacao e telas que ainda carregam tudo

### Objetivo

Levar para outras telas o mesmo cuidado de performance que ja existe na camada principal de consulta.

### Escopo prioritario

- `src/interface_grafica/ui/main_window.py`
- `src/interface_grafica/services/aggregation_service.py`

### Acoes

- revisar pontos da UI que ainda fazem `pl.read_parquet()` direto para tabelas grandes;
- mover leituras de tabelas visuais para `ParquetService` quando o uso for navegacao, filtro e listagem;
- limitar cargas eager na agregacao a casos onde a logica exige o dataset completo;
- reduzir recarregamentos desnecessarios apos filtros, reprocessamentos e edicoes;
- revisar operacoes com `to_dicts()` nas rotinas de agregacao e recalculo de padroes.

### Observacao importante

Nao tratar "paginacao" como feature nova global. Ela ja existe na consulta principal. O foco desta fase e estender ou reaproveitar esse padrao nas telas que ainda trabalham de forma eager.

### Criterios de aceite

- abertura da aba de agregacao continua funcional;
- filtros continuam consistentes com os dados exibidos;
- nenhuma regressao em selecao por `id_agrupado` e recarga de historico;
- melhoria perceptivel na abertura de tabelas grandes.

---

## Fase 3. Validacao de schema e contratos de entrada

### Objetivo

Falhar cedo e com mensagens claras quando arquivos de entrada ou intermediarios estiverem incompletos ou incoerentes.

### Escopo prioritario

- `src/extracao/extrair_dados_cnpj.py`
- `src/utilitarios/`
- modulos que leem Parquets base do pipeline

### Acoes

- criar validadores leves e explicitos com base em `df.schema` e colunas obrigatorias;
- padronizar utilitarios como:
  - `garantir_colunas_obrigatorias(df, colunas, contexto)`
  - `garantir_tipos_compativeis(df, schema_esperado, contexto)`
  - `validar_parquet_essencial(path, colunas, contexto)`
- validar especialmente artefatos sensiveis:
  - `item_unidades`
  - `itens`
  - `descricao_produtos`
  - `produtos_final`
  - `produtos_agrupados`
  - `mov_estoque`

### Diretriz

Evitar adicionar dependencias pesadas apenas para validacao. Antes de considerar bibliotecas externas, padronizar uma camada interna pequena e auditavel.

### Criterios de aceite

- mensagens de erro identificam arquivo, etapa e colunas ausentes;
- falhas de schema deixam de aparecer como erro generico distante da causa;
- contratos fundamentais do pipeline ficam documentados no codigo.

---

## Fase 4. Observabilidade, logs e diagnostico operacional

### Objetivo

Melhorar a capacidade de diagnosticar gargalos e falhas de background sem poluir o fluxo principal da aplicacao.

### Escopo prioritario

- `src/utilitarios/perf_monitor.py`
- workers e services da UI
- pontos de orquestracao e reprocessamento

### Acoes

- padronizar eventos de performance por etapa do pipeline e por recarga de tela;
- enriquecer contexto dos logs com:
  - `cnpj`
  - etapa
  - caminho do arquivo
  - volume de linhas
  - tempo total
- registrar falhas de worker com stack trace e contexto de operacao;
- centralizar logs tecnicos em arquivo rotativo apenas se o volume justificar.

### Diretriz

O projeto ja possui trilha de performance. Antes de introduzir novos mecanismos, consolidar o que ja existe.

### Criterios de aceite

- diagnosticos de regressao ficam reproduziveis;
- erros em background deixam rastro suficiente para analise posterior;
- logs continuam legiveis e com baixo acoplamento.

---

## Fase 5. Refino de algoritmos quentes

### Objetivo

Remover pontos de custo alto em loops Python e operacoes listadas fora de necessidade.

### Escopo prioritario

- `src/interface_grafica/services/aggregation_service.py`
- `src/transformacao/rastreabilidade_produtos/produtos_agrupados.py`
- modulos que usam `to_dicts()` em caminhos frequentes

### Acoes

- revisar `to_dicts()` e loops linha a linha em:
  - calculo de padroes;
  - recalculo de agregacoes;
  - montagem de visoes auxiliares;
- substituir por:
  - expressoes Polars;
  - agregacoes vetorizadas;
  - listas de colunas especificas em vez de serializacao completa;
  - arrays NumPy onde a ordem sequencial for inevitavel.

### Diretriz

Nao eliminar `to_dicts()` por dogma. Em trechos pequenos e administrativos ele pode ser aceitavel. O alvo sao hot paths e tabelas grandes.

### Criterios de aceite

- menor tempo nos fluxos de agregacao e recalculo;
- nenhuma mudanca no criterio de agrupamento ou nos padroes fiscais calculados;
- regressao funcional coberta por testes direcionados.

---

## Fase 6. Qualidade de codigo e automacao incremental

### Objetivo

Melhorar previsibilidade de manutencao sem introduzir uma barreira operacional grande de uma vez.

### Acoes

- adotar `ruff` primeiro como linter leve e progressivo;
- aplicar regras iniciais em arquivos novos ou alterados;
- introduzir checagem de tipos gradualmente, com foco inicial em:
  - services da UI;
  - utilitarios compartilhados;
  - orquestrador;
- ampliar testes dirigidos para modulos mais sensiveis do pipeline.

### Sequencia sugerida

1. `ruff check`
2. `ruff format` se o time quiser padronizacao automatica
3. tipagem gradual em funcoes utilitarias e services
4. eventualmente `mypy` em subconjunto do projeto

### Diretriz

Nao impor `mypy` estrito em todo o repositorio de uma vez. Comecar por modulos com contratos mais estaveis.

---

## 5. Backlog priorizado por impacto

### Prioridade alta

- reduzir `read_parquet()` desnecessario no pipeline de transformacao;
- reaproveitar `ParquetService` em telas com carga total excessiva;
- criar validadores internos de schema para artefatos essenciais;
- padronizar logs de falha em workers e reprocessamentos.

### Prioridade media

- reduzir `to_dicts()` em hot paths de agregacao;
- melhorar feedback de progresso em fluxos longos com etapas reais, nao progresso ficticio;
- ampliar cobertura de testes dos modulos mais pesados.

### Prioridade baixa

- avaliacao de ferramentas externas de validacao;
- tipagem estatica mais ampla;
- consolidacao de formatacao automatica em todo o repositorio.

---

## 6. Nao fazer nesta fase

Para evitar risco desnecessario, este plano explicitamente nao recomenda agora:

- reescrever a arquitetura do pipeline;
- remover wrappers legados de `src/transformacao/` sem estudo de impacto;
- migrar toda leitura eager para lazy sem medicao;
- acoplar validacao fiscal a widgets da UI;
- trocar a stack de exportacao Excel apenas por preferencia tecnologica.

---

## 7. Validacao por fase

Cada fase deve terminar com validacao proporcional ao risco.

### Validacao minima

```bash
python -m pytest
```

### Validacao dirigida sugerida

```bash
python -m pytest tests/test_movimentacao_estoque.py
python -m pytest tests/test_calculos_mensais.py
python -m pytest tests/test_calculos_anuais.py
python -m pytest tests/test_exportar_excel.py
```

Quando a mudanca atingir:

- `src/orquestrador_pipeline.py`: validar dependencias e import paths;
- `src/interface_grafica/`: validar inicializacao dos workers e recarga das telas;
- agregacao: validar filtros, recarga das tabelas e persistencia de `id_agrupado`;
- exportacao: validar compatibilidade com fluxos que usam Pandas.

---

## 8. Resultado esperado

Ao final, o projeto deve ficar:

- mais rapido nos fluxos realmente custosos;
- mais previsivel para manutencao;
- mais seguro contra regressao fiscal;
- mais aderente ao guia operacional de `Agents.md`;
- sem ruptura desnecessaria da arquitetura atual.

---

## 9. Checklist executivo

### Concluido nesta rodada

- [x] alinhar o plano com `Agents.md`
- [x] transformar o plano em roteiro por etapas
- [x] criar utilitario de validacao de schema e colunas obrigatorias
- [x] integrar validacao em etapas centrais do pipeline
- [x] expandir `validar_parquet_essencial()` para `fontes_produtos`, `fatores_conversao`, `c170_xml` e `c176_xml`
- [x] reduzir `read_parquet()` desnecessario em `03_descricao_produtos.py` e `04_produtos_final.py`
- [x] eliminar carga eager desnecessaria na abertura da aba de agregacao e no Fio de Ouro
- [x] revisar `to_dicts()` e ampliar `scan_parquet()` com projecao minima em `aggregation_service.py`
- [x] migrar abas anual, mensal e conversao para leitura via `ParquetService` e projecao minima nos enriquecimentos auxiliares
- [x] adicionar testes unitarios iniciais para a nova camada

### Proxima implementacao recomendada

- [ ] medir tempos da agregacao antes/depois usando `perf_monitor`
- [ ] revisar outros `read_parquet()` da UI com foco em `nfe_entrada` e `id_agrupados`

