# Plano abrangente de otimização e melhoria de desempenho — audit_pyside

> Objetivo: obter melhorias expressivas de desempenho na aplicação PySide6/Polars e evoluir a análise de similaridade **já existente**, agregando `Cod_item`/`cod_item` do C170 e `prod_cprod` de NFe/NFCe ao score composto que já usa descrição, números, NCM, CEST e GTIN.
>
> Este plano **não propõe criar uma segunda análise de similaridade**. A mudança correta é incorporar o código fiscal do produto ao serviço atual de similaridade, como mais uma evidência ponderada.

---

## 1. Resumo executivo

O repositório já tem uma base técnica boa: PySide6, Polars, workers Qt, cache de Parquet, instrumentação de performance e uma análise de similaridade em `src/interface_grafica/services/descricao_similarity_service.py`.

Mesmo assim, o desempenho pode ficar insatisfatório por quatro motivos principais:

1. **A interface ainda pode executar trabalho pesado na thread principal**, especialmente carregamento de páginas, contagens, filtros globais, refresh de logs e autoajuste de colunas.
2. **Há materialização excessiva de DataFrames completos**, em vez de leitura preguiçosa com `scan_parquet`, seleção de colunas e paginação real.
3. **O pipeline derivado é caro**, principalmente movimentação de estoque, cálculos mensais/anuais/períodos e recálculos encadeados após conversão/agregação.
4. **A similaridade já considera descrição, números, NCM, CEST e GTIN, mas ainda deve considerar o código fiscal do produto**, porque descrições diferentes podem representar o mesmo produto quando o código de origem coincide.

Estratégia: **medir com dados reais → tirar I/O pesado da UI → reduzir materializações → otimizar pipeline → agregar código fiscal à similaridade existente → travar regressões com benchmarks**.

---

## 2. Prioridades de alto impacto

### P0 — Ganhos imediatos e visíveis

- [ ] Criar baseline real com CNPJ grande.
- [ ] Fazer carregamento de página Parquet em worker, não na thread da UI.
- [ ] Impedir abertura automática do primeiro arquivo ao selecionar CNPJ.
- [ ] Mostrar primeira página antes da contagem total.
- [ ] Desativar `resizeColumnsToContents()` automático em tabelas grandes.
- [ ] Limitar refresh de logs às últimas N linhas.
- [ ] Agregar `Cod_item`/`cod_item` e `prod_cprod` ao score composto da similaridade existente.
- [ ] Criar testes e benchmark da similaridade com código fiscal.

### P1 — Redução forte de I/O e memória

- [ ] Trocar leituras completas por `scan_parquet().select(...)` onde for possível.
- [ ] Separar APIs de página, contagem, exportação e carregamento completo.
- [ ] Colocar orçamento de memória nos caches.
- [ ] Gravar consultas Oracle grandes em Parquet temporário ou processar em batches.
- [ ] Fazer filtro global de SQL/Parquet em worker.
- [ ] Ler valores únicos por coluna, não o arquivo inteiro.

### P2 — Pipeline e recálculo

- [ ] Otimizar `movimentacao_estoque` com projeção de colunas e normalização única.
- [ ] Reduzir joins e lookups repetidos.
- [ ] Reescrever cálculo de saldo para processar múltiplos grupos em uma chamada Numba, se benchmark confirmar ganho.
- [ ] Implementar recálculo parcial quando alterações de conversão/agregação afetarem poucos produtos.
- [ ] Criar benchmarks de regressão para pipeline.

---

## 3. Linha de base obrigatória

Antes de grandes alterações, gerar `docs/baseline_performance.md` com três execuções de cada fluxo:

- startup até janela visível
- seleção de CNPJ pequeno, médio e grande
- abertura da primeira página de Parquet grande
- troca de página
- aplicação de filtro textual
- troca para abas Estoque, Mensal, Anual e Períodos
- abertura de `mov_estoque`
- recálculo de conversão
- recálculo de agregação
- consulta Oracle representativa
- exportação Excel de dataset grande
- pipeline `movimentacao_estoque`
- pipeline mensal/anual/períodos
- análise de similaridade em dataset real

Métricas mínimas:

- duração média, mínima, máxima e p95
- linhas e colunas processadas
- CNPJ e arquivo
- cache hit/miss
- memória RSS antes/depois
- thread: `ui`, `worker`, `oracle`, `pipeline`
- status: `ok`, `error`, `cancelled`

Melhorar `scripts/resumir_performance.py` para gerar:

- top 20 eventos por tempo total
- top 20 eventos por pior caso
- resumo por fluxo
- resumo por CNPJ
- p50/p95/p99
- pico de memória
- saída Markdown

Critério de saída: conseguir apontar, com dados, os cinco maiores gargalos reais.

---

## 4. Interface PySide6: eliminar travamentos

### 4.1. Não abrir arquivo automaticamente ao selecionar CNPJ

A seleção de CNPJ deve apenas preencher a árvore e atualizar contexto. Não deve chamar `on_file_activated()` no primeiro item automaticamente.

Resultado esperado: seleção de CNPJ em menos de 300 ms, sem leitura pesada inesperada.

### 4.2. Carregar tabela em worker

Criar `TablePageWorker` para executar `ParquetService.get_page()` fora da UI.

Regras:

- cada solicitação recebe `request_id`
- resultado antigo é descartado se o usuário mudou filtro/página/arquivo
- status mostra carregamento
- paginação não dispara leituras concorrentes duplicadas
- erro volta para a UI de forma segura

Fluxo:

```text
UI solicita página
  -> cria request_id
  -> worker executa ParquetService.get_page
  -> worker retorna PageResult
  -> UI aplica resultado somente se request_id ainda for atual
```

### 4.3. Separar página e contagem total

A primeira página deve aparecer antes da contagem total exata.

Fluxo:

1. coletar página com `slice(offset, page_size)`
2. exibir tabela
3. contar total em worker separado
4. atualizar label quando terminar

Enquanto isso:

```text
Página 1 | contando total...
```

### 4.4. Autoajuste de colunas sob demanda

`resizeColumnsToContents()` deve ser aplicado somente se:

- linhas visíveis <= 200
- colunas <= 30

Caso contrário:

- aplicar larguras padrão
- adicionar botão “Ajustar colunas agora”
- registrar evento `ui.table_resize_to_contents`

### 4.5. Logs paginados

`refresh_logs()` deve ler apenas as últimas 500 ou 1000 linhas. Para logs grandes:

- botão “carregar mais”
- busca em worker
- evitar `setPlainText()` com arquivo inteiro

---

## 5. Modelo de tabela: reduzir custo por célula

O `PolarsTableModel` já é o caminho certo, mas ainda pode melhorar.

### 5.1. Cache de exibição da página

Ao trocar DataFrame, pré-computar os valores exibidos:

```python
self._display_columns = {
    col: [display_cell(v, col) for v in df.get_column(col).to_list()]
    for col in df.columns
}
```

`data()` passa a buscar valores prontos para `DisplayRole` e `ToolTipRole`.

### 5.2. Reduzir `df[row, col]`

Acesso célula a célula em Polars pode ficar caro durante pintura/rolagem. Para a página atual, listas por coluna são mais rápidas.

### 5.3. Estilo por linha, não por célula

Resolvers de foreground/background/font devem:

- cachear resultado por linha e role
- evitar `row_as_dict()` para cada célula
- preferir colunas auxiliares pré-calculadas

### 5.4. Edição por delta

Em `setData()`, evitar recriar uma coluna inteira a cada edição. Usar:

```python
pending_edits[(row_key, coluna)] = valor
```

Aplicar alterações em lote ao salvar, especialmente na aba Conversão.

---

## 6. Parquet/I/O

### 6.1. Projection pushdown

Toda leitura deve selecionar somente as colunas necessárias:

- colunas visíveis
- colunas usadas em filtros
- colunas usadas em ordenação
- colunas de chave/seleção

Evitar carregar `df_all_columns` quando a tela só precisa de `df_visible`.

### 6.2. APIs separadas

Criar métodos explícitos:

- `get_page_fast(...)`
- `count_rows(...)`
- `load_for_export(...)`
- `load_for_pipeline(...)`
- `unique_values_for_column(...)`

Evitar usar `load_dataset()` como atalho genérico em fluxo de UI.

### 6.3. Cache por orçamento de memória

Substituir limite por quantidade de entradas por orçamento aproximado em MB:

- page cache: 128 MB
- dataset cache: 512 MB configurável
- schema cache pode continuar maior
- limpar caches ao trocar CNPJ se RSS subir demais
- invalidar por `(path, mtime_ns, size)`

### 6.4. Valores únicos por coluna

Para filtros e combos:

```python
pl.scan_parquet(path)
  .select(pl.col(col).cast(pl.Utf8).drop_nulls().unique().sort())
  .collect()
```

Nunca ler o DataFrame completo apenas para montar lista de valores únicos.

---

## 7. Consulta Oracle

### 7.1. Não acumular `all_rows`

O `QueryWorker` deve parar de montar uma lista completa antes de criar o DataFrame.

Estratégia recomendada:

```text
Oracle fetchmany
  -> Polars DataFrame do batch
  -> append em Parquet temporário
  -> UI pagina o Parquet temporário
```

Benefícios:

- primeira página aparece antes
- pico de memória menor
- filtro/paginação reutiliza `ParquetService`
- cancelamento fica útil

### 7.2. Filtro SQL em worker

Filtro textual global em resultados SQL deve rodar fora da thread da UI e retornar página filtrada.

### 7.3. Cancelamento real

Adicionar botão Cancelar:

- `requestInterruption()`
- `conn.cancel()` quando disponível
- fechamento de cursor/conexão
- evento `status="cancelled"`

---

## 8. Pipeline: ganhos expressivos

### 8.1. `movimentacao_estoque`

Ações:

- trocar `read_parquet` por `scan_parquet().select(colunas_necessarias)` onde possível
- definir colunas necessárias a partir de `map_estoque.json` + joins + filtros
- normalizar `Cod_item`/`cod_item`, `prod_cprod`, descrição, unidade e tipo de operação uma única vez por fonte
- reduzir DataFrames de lookup antes dos joins
- garantir tipos coerentes antes dos joins
- medir separadamente: leitura, normalização, vínculo por código, vínculo por descrição, fator, eventos, saldo e escrita

### 8.2. Cálculo de saldo

O núcleo Numba já ajuda, mas pode haver ganho ao evitar `map_groups` para muitos grupos pequenos.

Plano:

1. ordenar por `id_agrupado`, ano/período, data e ordem fiscal
2. transformar colunas necessárias em arrays NumPy
3. gerar offsets de grupos
4. chamar uma função Numba única que percorre todos os grupos
5. reanexar arrays ao DataFrame

Só implementar se benchmark confirmar ganho real.

### 8.3. Recálculo parcial

Quando o usuário altera conversão/agregação de poucos produtos:

- identificar `id_agrupado` afetados
- recalcular só linhas desses produtos quando fiscalmente seguro
- atualizar mensal/anual/períodos afetados
- manter fallback “recalcular completo”
- registrar se o recálculo foi parcial ou completo

### 8.4. Agregação

- cache global com limite de memória
- assinatura por arquivo nos caches
- menos colunas carregadas
- logs com contagem de linhas antes/depois de joins
- benchmark para saber se o gargalo é cálculo, join, escrita ou UI

---

## 9. Similaridade: agregar código fiscal à análise existente

### 9.1. Regra central

A análise atual deve continuar combinando:

- descrição
- tokens/números extraídos da descrição
- NCM
- CEST
- GTIN

A melhoria é adicionar mais um componente:

- **código fiscal do produto**, vindo de `Cod_item`/`cod_item` no C170 e `prod_cprod` em NFe/NFCe

Portanto, não criar outro algoritmo paralelo. Alterar o score composto atual.

### 9.2. Campos aceitos

Usar aliases estritos:

```python
ALIASES_CODIGO_PRODUTO_FISCAL = [
    "cod_item",    # C170/EFD
    "Cod_item",    # C170/EFD legado
    "COD_ITEM",    # C170/EFD variação
    "prod_cprod",  # NFe/NFCe
]
```

Não incluir nesta etapa:

- `codigo`
- `codigo_produto`
- `codigo_fonte`
- `lista_codigos`
- GTIN/código de barras

Motivo: esses campos podem ter semântica diferente e aumentar falsos positivos.

### 9.3. Coluna canônica

Criar na preparação da similaridade:

```python
sim_codigo_produto_fiscal_norm
```

Ordem de resolução:

1. `prod_cprod`
2. `cod_item`
3. `Cod_item`
4. `COD_ITEM`
5. vazio

Essa coluna deve ser preenchida antes de criar `_RowSimilarityData`.

### 9.4. Normalização

```python
def _normalizar_codigo_produto_fiscal(valor: Any) -> str:
    texto = _normalizar_codigo(valor)
    texto = texto.strip().upper()
    texto = re.sub(r"[^A-Z0-9._-]", "", texto)
    return texto
```

Códigos fracos a ignorar:

```python
CODIGOS_PRODUTO_FISCAIS_FRACOS = {
    "", "0", "00", "000", "0000", "1", "01", "001",
    "999", "9999", "SEM", "S/C", "NA", "N/A"
}
```

### 9.5. Alterar `_RowSimilarityData`

Adicionar:

```python
codigo_produto_fiscal_norm: str
codigo_produto_fiscal_partes: frozenset[str]
```

### 9.6. Gerar candidatos por código fiscal

Atualizar `_candidate_keys()` para criar buckets adicionais:

```python
for cod in row.codigo_produto_fiscal_partes:
    keys.add(f"CODFISCAL:{cod}")
    for ncm4 in row.ncm4_partes:
        keys.add(f"CODFISCAL:{cod}|NCM4:{ncm4}")
    for token in fortes[:3]:
        keys.add(f"CODFISCAL:{cod}|T:{token}")
```

Isso melhora qualidade e desempenho porque reduz comparações aleatórias e cria pares candidatos mais relevantes.

### 9.7. Score composto integrado

Adicionar ao `SIM_CONFIG`:

```python
"codigo_fiscal_weight": 25,
"codigo_fiscal_scale": 2.0,
"codigo_fiscal_min_score": 88,
"codigo_fiscal_desc_min_score": 35,
"codigo_fiscal_ncm_min_score": 70,
```

Adicionar no `_score_composto`:

```python
score_codigo_fiscal = _codigo_score(
    a.codigo_produto_fiscal_partes,
    b.codigo_produto_fiscal_partes,
)
```

Incluir no `weight_map`:

```python
"codigo_fiscal": SIM_CONFIG["codigo_fiscal_weight"]
```

Adicionar aos componentes ponderados se `score_codigo_fiscal is not None`.

Regras sugeridas:

- código fiscal igual + GTIN igual: score mínimo 95
- código fiscal igual + NCM completo igual: score mínimo 90
- código fiscal igual + NCM6 igual: score mínimo 88
- código fiscal igual + NCM4 igual: score mínimo 84
- código fiscal igual + descrição média: score mínimo 88
- código fiscal igual + descrição muito diferente + NCM/CEST/GTIN ausentes: candidato forte para revisão, mas sem auto-agrupar sozinho
- código fiscal diferente: não penalizar, apenas não bonificar

Motivos novos:

- `CODIGO_FISCAL_IGUAL`
- `CODIGO_FISCAL_NCM_IGUAL`
- `CODIGO_FISCAL_DESC_DIVERGENTE`

### 9.8. Salvaguardas contra falso positivo

Auto-bloco por código fiscal só deve ocorrer se houver pelo menos mais uma evidência:

- GTIN igual, ou
- NCM4/NCM6/NCM completo igual, ou
- CEST igual, ou
- `score_desc >= 35`, ou
- números/unidade compatíveis.

Também manter:

- `max_bucket_size`
- descarte de códigos fracos
- marcação de risco quando código igual conflita com NCM/CEST/GTIN
- limite de score quando código é curto ou genérico

### 9.9. Saída/rastreabilidade

Adicionar nas sugestões:

- `score_codigo_fiscal`
- `codigo_fiscal_a`
- `codigo_fiscal_b`
- `motivos` com `CODIGO_FISCAL_IGUAL`
- `evidencias_fortes`
- `risco_falso_positivo`

### 9.10. Testes obrigatórios

Criar/adaptar testes em `tests/test_descricao_similarity_service.py`:

1. `Cod_item` C170 igual a `prod_cprod` NFe, descrições diferentes, NCM igual → score alto.
2. `cod_item` C170 igual a `prod_cprod` NFCe, descrições diferentes, sem NCM/GTIN → candidato de revisão, não auto-bloco sozinho.
3. código fraco `001`, descrições diferentes → não agrupar sozinho.
4. código diferente, descrição igual → ainda sugerir por descrição.
5. mesmo GTIN e mesmo código fiscal → score muito alto.
6. mesmo código fiscal, NCM conflitante → score limitado e marcado como risco.
7. `cod_item`, `Cod_item` e `COD_ITEM` devem ser equivalentes.

### 9.11. Benchmark

Atualizar `benchmarks/comparar_metodos_similaridade.py` para reportar:

- tempo total
- pares candidatos
- blocos gerados
- tamanho máximo de bloco
- pares com `CODIGO_FISCAL_IGUAL`
- falsos positivos conhecidos
- falsos negativos conhecidos

Meta: melhorar recall de produtos com descrição divergente sem explosão de pares candidatos.

---

## 10. Arquivos prioritários

### Interface e carregamento

- `src/interface_grafica/windows/main_window_navigation.py`
- `src/interface_grafica/windows/main_window_loading.py`
- `src/interface_grafica/windows/main_window_support.py`
- `src/interface_grafica/controllers/sql_query_controller.py`
- `src/interface_grafica/controllers/workers.py`

### Modelo e Parquet

- `src/interface_grafica/models/table_model.py`
- `src/interface_grafica/services/parquet_service.py`
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

## 11. Metas de sucesso

| Fluxo | Meta |
|---|---:|
| Seleção de CNPJ | UI livre em < 300 ms |
| Primeira página Parquet grande | pintura em < 2 s |
| Troca de página cacheada | < 500 ms |
| Filtro textual | primeira página em < 1,5 s |
| Consulta Oracle grande | primeira página sem aguardar todas as linhas |
| `movimentacao_estoque` | reduzir >= 40% contra baseline real |
| Pico de memória | reduzir >= 30% em CNPJ grande |
| Similaridade | maior recall para descrições divergentes com mesmo `Cod_item`/`prod_cprod` |
| Regressão | CI falha se benchmark crítico piorar > 15% |

---

## 12. Sequência recomendada

### Sprint 1 — Baseline e UI

1. Melhorar `scripts/resumir_performance.py`.
2. Criar `docs/baseline_performance.md`.
3. Impedir abertura automática de arquivo ao selecionar CNPJ.
4. Mover `reload_table()` para worker.
5. Adiar contagem total.
6. Desativar autoajuste de colunas em tabelas grandes.

### Sprint 2 — Similaridade integrada com código fiscal

1. Adicionar aliases estritos: `cod_item`, `Cod_item`, `COD_ITEM`, `prod_cprod`.
2. Criar `sim_codigo_produto_fiscal_norm`.
3. Adicionar campos fiscais em `_RowSimilarityData`.
4. Gerar candidatos por `CODFISCAL`.
5. Adicionar score/peso/floor no `_score_composto` existente.
6. Adicionar salvaguardas contra código fraco.
7. Atualizar testes e benchmark.

### Sprint 3 — I/O e Oracle

1. Separar APIs de página/contagem/exportação.
2. Aplicar projection pushdown.
3. Cache com orçamento de memória.
4. QueryWorker em batches ou Parquet temporário.
5. Filtro SQL assíncrono.

### Sprint 4 — Pipeline

1. Medir `movimentacao_estoque` por etapa.
2. Reduzir colunas lidas e normalizações repetidas.
3. Compactar joins.
4. Otimizar saldo com Numba por offsets de grupo.
5. Implementar recálculo parcial quando seguro.

### Sprint 5 — Regressão

1. Benchmarks automatizados.
2. Testes de performance básicos.
3. Limpeza de caches e memória.
4. Documentar limites e configurações.
5. Revisar UX de progresso/cancelamento.

---

## 13. Definição de pronto

Este plano só deve ser considerado executado quando:

- há baseline antes/depois com CNPJ grande
- a UI não congela nos fluxos principais
- consultas e Parquet grandes não materializam tudo sem necessidade
- o pipeline mais caro teve redução mensurável
- a similaridade existente incorpora `cod_item`/`Cod_item` do C170 e `prod_cprod` de NFe/NFCe no score composto
- os testes cobrem descrições divergentes com mesmo código fiscal
- existe benchmark para impedir regressão
