# Plano de melhoria expressiva de desempenho — audit_pyside

> Prioridade máxima: o desempenho precisa melhorar **muitíssimo**. Este plano foca em mudanças estruturais, não apenas micro-otimizações.
>
> Objetivo secundário: evoluir a análise de similaridade já existente para apoiar a **agregação manual de produtos**, comparando descrição, NCM, CEST, GTIN e códigos fiscais do produto.

---

## 1. Diretriz principal

A aplicação deve parar de operar como se arquivos grandes coubessem confortavelmente na UI/memória. A meta é mudar o comportamento para:

1. **UI sempre responsiva**: nenhuma leitura, filtro, contagem, exportação ou consulta pesada deve rodar na thread principal.
2. **Ler o mínimo possível**: usar `scan_parquet`, seleção de colunas, paginação real e streaming/batches.
3. **Calcular uma vez e reutilizar**: reduzir recomputações, joins repetidos, normalizações repetidas e recálculos completos desnecessários.
4. **Medir tudo que importa**: todo gargalo precisa ter antes/depois com tempo, linhas, colunas e memória.
5. **Otimizar primeiro os maiores gargalos reais**: pipeline de estoque, consultas grandes, carregamento de tabelas e filtros têm prioridade maior que ajustes cosméticos.

---

## 2. Metas agressivas de desempenho

| Área | Meta mínima |
|---|---:|
| Seleção de CNPJ | UI livre em < 300 ms |
| Primeira página de Parquet grande | exibir em < 2 s |
| Troca de página cacheada | < 500 ms |
| Filtro textual em tabela grande | primeira página filtrada em < 1,5 s |
| Consulta Oracle grande | mostrar primeira página sem esperar todas as linhas |
| Pico de memória em CNPJ grande | reduzir >= 50% |
| `movimentacao_estoque` | reduzir >= 50% contra baseline real |
| Cálculos mensal/anual/períodos | reduzir >= 40% contra baseline real |
| Exportação grande | não travar UI; progresso e cancelamento |
| Similaridade | manter UI responsiva e evitar explosão quadrática de pares |

Se uma mudança não contribui para essas metas, ela não deve entrar nas primeiras fases.

---

## 3. Fase 0 — baseline obrigatório e ranking dos gargalos

Antes de refatorar pesado, criar `docs/baseline_performance.md` com três execuções de cada fluxo em CNPJ grande:

- startup até janela visível;
- seleção de CNPJ;
- abertura de Parquet grande;
- troca de página;
- filtro textual;
- abertura de Estoque, Mensal, Anual e Períodos;
- consulta Oracle representativa;
- exportação Excel grande;
- recálculo de conversão;
- recálculo de agregação;
- `movimentacao_estoque`;
- cálculos mensal/anual/períodos;
- análise de similaridade em dataset real.

Métricas obrigatórias:

- duração média, mínima, máxima e p95;
- linhas/colunas processadas;
- CNPJ e arquivo;
- cache hit/miss;
- memória RSS antes/depois;
- thread: `ui`, `worker`, `oracle`, `pipeline`;
- status: `ok`, `error`, `cancelled`.

Melhorar `scripts/resumir_performance.py` para gerar:

- top 20 eventos por tempo total;
- top 20 eventos por pior caso;
- resumo por fluxo;
- resumo por CNPJ;
- p50/p95/p99;
- pico de memória;
- saída Markdown.

**Critério de saída:** ter uma lista ordenada dos 5 maiores gargalos reais. Sem isso, qualquer otimização é chute.

---

## 4. Fase 1 — eliminar travamentos da interface

Esta é a fase mais urgente para percepção de desempenho.

### 4.1. Não abrir arquivo automaticamente ao selecionar CNPJ

A seleção de CNPJ deve apenas preencher a árvore e atualizar o contexto. Não deve abrir automaticamente o primeiro arquivo da árvore.

Ação:

- remover chamada automática para `on_file_activated()` em `refresh_file_tree`;
- mostrar mensagem: “CNPJ selecionado; escolha um arquivo para abrir”;
- pré-carregar schemas somente em worker de baixa prioridade, se necessário.

Impacto esperado: seleção de CNPJ deixa de disparar leitura pesada inesperada.

### 4.2. `reload_table()` fora da thread principal

Criar `TablePageWorker` para executar `ParquetService.get_page()` fora da UI.

Regras:

- cada solicitação recebe `request_id`;
- resultados antigos são descartados se filtro/página/arquivo mudou;
- botões de paginação não disparam leituras concorrentes duplicadas;
- status mostra carregamento;
- erro retorna de forma segura para a UI.

Fluxo desejado:

```text
UI solicita página
  -> cria request_id
  -> worker executa leitura/filtro/slice
  -> worker retorna PageResult
  -> UI aplica resultado somente se request_id ainda for atual
```

### 4.3. Mostrar página antes da contagem total

Contagem total em Parquet grande pode ser cara. A página deve aparecer primeiro.

Novo fluxo:

1. coletar página com `slice(offset, page_size)`;
2. exibir tabela;
3. contar total em worker separado;
4. atualizar label quando terminar.

Enquanto isso:

```text
Página 1 | contando total...
```

### 4.4. Autoajuste de colunas apenas sob demanda

`resizeColumnsToContents()` deve ser proibido em tabelas grandes.

Regra:

- autoajustar só se `linhas <= 200` e `colunas <= 30`;
- senão, aplicar larguras padrão;
- adicionar botão “Ajustar colunas agora”.

### 4.5. Logs paginados

`refresh_logs()` deve ler apenas as últimas 500 ou 1000 linhas. Nunca carregar log inteiro na UI.

---

## 5. Fase 2 — reduzir I/O e memória drasticamente

### 5.1. Projection pushdown em todas as leituras

Toda leitura deve selecionar somente as colunas necessárias:

- colunas visíveis;
- colunas filtradas;
- colunas de ordenação;
- colunas de chave;
- colunas exigidas por tooltip/estilo.

Evitar carregar `df_all_columns` se a tela só precisa de colunas visíveis.

### 5.2. APIs separadas no `ParquetService`

Criar métodos explícitos:

- `get_page_fast(...)` — página sem contagem total;
- `count_rows(...)` — contagem em worker;
- `load_for_export(...)` — exportação fora da UI;
- `load_for_pipeline(...)` — uso interno do pipeline;
- `unique_values_for_column(...)` — filtros/combos sem ler tudo.

`load_dataset()` não deve ser usado como atalho genérico em tela interativa.

### 5.3. Cache com orçamento de memória

Trocar limite por quantidade de entradas por orçamento aproximado em MB:

- page cache: 128 MB;
- dataset cache: 512 MB configurável;
- schema cache: manter amplo;
- limpar caches ao trocar CNPJ se RSS subir demais;
- invalidar por `(path, mtime_ns, size)`.

### 5.4. Valores únicos por coluna

Para filtros e combos:

```python
pl.scan_parquet(path)
  .select(pl.col(col).cast(pl.Utf8).drop_nulls().unique().sort())
  .collect()
```

Nunca ler o DataFrame inteiro apenas para montar lista de valores únicos.

---

## 6. Fase 3 — consultas Oracle em streaming/batches

O `QueryWorker` não deve acumular `all_rows` antes de criar DataFrame.

Estratégia recomendada:

```text
Oracle fetchmany
  -> DataFrame Polars do batch
  -> grava/append em Parquet temporário
  -> UI pagina o Parquet temporário
```

Benefícios:

- primeira página aparece antes;
- menor pico de memória;
- paginação e filtro reutilizam `ParquetService`;
- cancelamento funciona melhor.

Também implementar:

- botão Cancelar;
- `requestInterruption()`;
- `conn.cancel()` quando disponível;
- fechamento seguro de cursor/conexão;
- evento de performance `status="cancelled"`.

---

## 7. Fase 4 — pipeline de estoque e recálculos

Esta fase tende a entregar os maiores ganhos absolutos.

### 7.1. `movimentacao_estoque`

Ações obrigatórias:

- trocar `read_parquet` por `scan_parquet().select(colunas_necessarias)` onde possível;
- definir colunas necessárias a partir de `map_estoque.json`, joins e filtros;
- normalizar `cod_item`/`Cod_item`/`COD_ITEM`, `prod_cprod`, descrição, unidade e tipo de operação uma única vez por fonte;
- reduzir DataFrames de lookup antes dos joins;
- garantir tipos coerentes antes dos joins;
- medir separadamente leitura, normalização, vínculo por código, vínculo por descrição, fator, eventos, saldo e escrita.

### 7.2. Cálculo de saldo

O núcleo Numba já ajuda, mas ainda pode haver custo alto com muitos grupos.

Plano:

1. ordenar por `id_agrupado`, ano/período, data e ordem fiscal;
2. transformar colunas necessárias em arrays NumPy;
3. gerar offsets de grupos;
4. chamar uma função Numba única que percorre todos os grupos;
5. reanexar arrays ao DataFrame.

Só implementar se benchmark confirmar ganho real.

### 7.3. Recálculo parcial

Quando o usuário altera conversão/agregação de poucos produtos:

- identificar `id_agrupado` afetados;
- recalcular só linhas desses produtos quando fiscalmente seguro;
- atualizar mensal/anual/períodos afetados;
- manter fallback “recalcular completo”;
- registrar se o recálculo foi parcial ou completo.

---

## 8. Fase 5 — modelo de tabela mais rápido

### 8.1. Cache de exibição

Ao trocar DataFrame, pré-computar os valores exibidos:

```python
self._display_columns = {
    col: [display_cell(v, col) for v in df.get_column(col).to_list()]
    for col in df.columns
}
```

`data()` deve buscar valores prontos para `DisplayRole` e `ToolTipRole`.

### 8.2. Reduzir acesso `df[row, col]`

Para a página atual, listas por coluna são mais rápidas e reduzem custo de pintura/rolagem.

### 8.3. Estilo por linha, não por célula

Resolvers de foreground/background/font devem:

- cachear resultado por linha e role;
- evitar `row_as_dict()` por célula;
- preferir colunas auxiliares pré-calculadas.

### 8.4. Edição por delta

Em `setData()`, evitar recriar coluna inteira a cada edição.

Usar:

```python
pending_edits[(row_key, coluna)] = valor
```

Aplicar alterações em lote ao salvar.

---

## 9. Similaridade para apoiar agregação manual

A similaridade é importante, mas deve respeitar a prioridade de desempenho: precisa ser eficiente, ranqueada e rastreável.

### 9.1. Regra central

A análise atual deve comparar em conjunto:

- descrição;
- tokens/números extraídos da descrição;
- NCM;
- CEST;
- GTIN;
- códigos fiscais do produto: `cod_item`, `Cod_item`, `COD_ITEM`, `prod_cprod`.

A saída deve ser uma **lista de candidatos para agregação manual**, não uma agregação automática sem revisão.

### 9.2. Campos aceitos para código fiscal

Usar aliases estritos:

```python
ALIASES_CODIGO_PRODUTO_FISCAL = [
    "cod_item",    # C170/EFD
    "Cod_item",    # C170/EFD legado
    "COD_ITEM",    # C170/EFD variação
    "prod_cprod",  # NFe/NFCe
]
```

Não misturar GTIN/código de barras nesse componente. GTIN já tem score próprio.

### 9.3. Score composto

Adicionar ao score existente um componente `codigo_fiscal`, sem substituir os demais.

O resultado final deve expor:

- `score_total`;
- `score_descricao`;
- `score_ncm`;
- `score_cest`;
- `score_gtin`;
- `score_codigo_fiscal`;
- `motivos`;
- `risco_falso_positivo`.

Regras sugeridas:

- GTIN igual é evidência muito forte;
- código fiscal igual é evidência forte, mas não absoluta;
- NCM/CEST reforçam ou reduzem confiança;
- descrição muito diferente com código igual deve virar candidato de revisão, não agregação cega;
- código fiscal diferente não deve penalizar automaticamente, apenas deixar de bonificar.

### 9.4. Geração eficiente de candidatos

Evitar explosão quadrática. Usar buckets por:

- GTIN;
- código fiscal;
- NCM completo/NCM4 + token forte;
- CEST + token forte;
- tokens fortes da descrição;
- números relevantes da descrição.

Para código fiscal:

```python
for cod in row.codigo_produto_fiscal_partes:
    keys.add(f"CODFISCAL:{cod}")
    for ncm4 in row.ncm4_partes:
        keys.add(f"CODFISCAL:{cod}|NCM4:{ncm4}")
    for token in fortes[:3]:
        keys.add(f"CODFISCAL:{cod}|T:{token}")
```

Manter `max_bucket_size` e `top_k_per_row` para impedir explosão de pares.

### 9.5. Salvaguardas

Ignorar códigos fracos:

```python
CODIGOS_PRODUTO_FISCAIS_FRACOS = {
    "", "0", "00", "000", "0000", "1", "01", "001",
    "999", "9999", "SEM", "S/C", "NA", "N/A"
}
```

Auto-bloco por código fiscal só deve ocorrer se houver mais uma evidência:

- GTIN igual; ou
- NCM4/NCM6/NCM completo igual; ou
- CEST igual; ou
- descrição minimamente compatível; ou
- números/unidade compatíveis.

### 9.6. Testes obrigatórios

Criar/adaptar testes em `tests/test_descricao_similarity_service.py`:

1. `Cod_item` C170 igual a `prod_cprod` NFe, descrições diferentes, NCM igual → score alto;
2. `cod_item` C170 igual a `prod_cprod` NFCe, descrições diferentes, sem NCM/GTIN → candidato de revisão;
3. código fraco `001`, descrições diferentes → não agrupar sozinho;
4. código diferente, descrição igual → ainda sugerir por descrição;
5. mesmo GTIN e mesmo código fiscal → score muito alto;
6. mesmo código fiscal, NCM conflitante → score limitado e marcado como risco;
7. `cod_item`, `Cod_item` e `COD_ITEM` devem ser equivalentes.

### 9.7. Benchmark de similaridade

Atualizar `benchmarks/comparar_metodos_similaridade.py` para reportar:

- tempo total;
- pares candidatos;
- blocos gerados;
- tamanho máximo de bloco;
- pares com `CODIGO_FISCAL_IGUAL`;
- falsos positivos conhecidos;
- falsos negativos conhecidos.

Meta: melhorar recall sem explodir tempo ou memória.

---

## 10. Arquivos prioritários

### Desempenho/UI

- `src/interface_grafica/windows/main_window_navigation.py`
- `src/interface_grafica/windows/main_window_loading.py`
- `src/interface_grafica/windows/main_window_support.py`
- `src/interface_grafica/controllers/workers.py`
- `src/interface_grafica/controllers/sql_query_controller.py`

### Parquet/modelo/Oracle

- `src/interface_grafica/services/parquet_service.py`
- `src/interface_grafica/models/table_model.py`
- `src/interface_grafica/services/query_worker.py`

### Pipeline

- `src/transformacao/movimentacao_estoque_pkg/movimentacao_estoque.py`
- `src/transformacao/movimentacao_estoque_pkg/calculo_saldos.py`
- `src/transformacao/calculos_mensais_pkg/calculos_mensais.py`
- `src/transformacao/calculos_anuais_pkg/calculos_anuais.py`
- `src/transformacao/calculos_periodo_pkg/calculos_periodo.py`
- `src/interface_grafica/services/aggregation_service.py`

### Similaridade

- `src/interface_grafica/services/descricao_similarity_service.py`
- `src/interface_grafica/patches/similaridade_agregacao.py`
- `benchmarks/comparar_metodos_similaridade.py`
- `tests/test_descricao_similarity_service.py`

---

## 11. Ordem de execução recomendada

### Sprint 1 — impacto perceptível imediato

1. Baseline real.
2. Remover abertura automática de arquivo ao selecionar CNPJ.
3. `reload_table()` em worker.
4. Separar página e contagem total.
5. Desativar autoajuste de colunas em tabelas grandes.
6. Logs paginados.

### Sprint 2 — corte de I/O e memória

1. Projection pushdown.
2. APIs separadas de página/contagem/exportação.
3. Cache por orçamento de memória.
4. Valores únicos por coluna.
5. QueryWorker em batches/Parquet temporário.

### Sprint 3 — pipeline pesado

1. Medir `movimentacao_estoque` por etapa.
2. Reduzir colunas lidas.
3. Compactar joins.
4. Normalizar uma vez por fonte.
5. Avaliar saldo com Numba por offsets.
6. Recálculo parcial quando seguro.

### Sprint 4 — similaridade multiatributo eficiente

1. Integrar código fiscal ao score existente.
2. Expor scores por componente.
3. Gerar candidatos por buckets eficientes.
4. Adicionar salvaguardas.
5. Testes e benchmark.

### Sprint 5 — regressão e endurecimento

1. Benchmarks automatizados.
2. Testes de performance mínimos.
3. CI falhando se gargalo crítico piorar > 15%.
4. Documentar limites e configurações.

---

## 12. Definição de pronto

Este plano só deve ser considerado executado quando:

- há baseline antes/depois com CNPJ grande;
- a UI não congela nos fluxos principais;
- consultas e Parquet grandes não materializam tudo sem necessidade;
- o pipeline mais caro teve redução mensurável;
- o pico de memória caiu de forma mensurável;
- a similaridade existente compara descrição, NCM, CEST, GTIN e códigos fiscais do produto;
- a saída da similaridade apoia agregação manual com score e evidências por campo;
- existe benchmark para impedir regressão.
