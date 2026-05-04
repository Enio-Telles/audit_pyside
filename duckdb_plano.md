# Plano validado: DuckDB para Parquets grandes, GUI paginada e MCP DuckDB

Status: proposta técnica validada por pesquisa em documentação oficial.

Branch sugerida: `docs/duckdb-plano`

## 1. Resumo executivo

O problema de travamento ao abrir Parquets grandes, especialmente arquivos acima de 2 GB, deve ser tratado como problema arquitetural da camada de interface e exploração de dados, não apenas como problema de thread.

A solução recomendada é híbrida:

```text
Polars  -> permanece no núcleo fiscal e nas transformações já testadas
DuckDB  -> entra como motor de consulta interativa da GUI e exportação grande
Parquet -> continua como formato canônico, com particionamento para tabelas grandes
MCP     -> entra como ferramenta local para agentes consultarem e diagnosticarem dados
```

A validação por pesquisa confirma que DuckDB é adequado para ler Parquet diretamente com projeção de colunas, pushdown de filtros, leitura de múltiplos arquivos, escrita via `COPY`, escrita particionada estilo Hive e processamento maior que a memória em certos cenários. Polars continua adequado para transformações lazy e escrita streaming via `sink_parquet`.

## 2. Fontes pesquisadas e conclusões

### 2.1. DuckDB lê Parquet diretamente e evita ler colunas desnecessárias

A documentação de DuckDB informa suporte a leitura e escrita eficiente de Parquet, incluindo `read_parquet`, leitura por glob e inferência automática para arquivos `.parquet`.

Também documenta `projection pushdown`: ao consultar Parquet, DuckDB lê somente as colunas necessárias para a query. Isso valida a proposta de a GUI selecionar apenas colunas visíveis e colunas usadas em filtros/sort, em vez de carregar o arquivo inteiro.

Conclusão para o projeto:

```text
A aba Consulta, mov_estoque, NFe entrada, mensal, anual, períodos e agregação devem consultar somente as colunas necessárias para renderizar a página atual.
```

Fonte: DuckDB — Reading and Writing Parquet Files.

### 2.2. DuckDB aplica filtros no scan de Parquet

A documentação de Parquet do DuckDB descreve `filter pushdown`: filtros aplicados a colunas lidas do Parquet são empurrados para o leitor e podem pular partes do arquivo quando há estatísticas/zonemaps.

Conclusão para o projeto:

```text
Filtros da GUI devem ser traduzidos para SQL DuckDB antes da leitura.
Não devem ser aplicados em DataFrame materializado inteiro.
```

Fonte: DuckDB — Reading and Writing Parquet Files.

### 2.3. DuckDB suporta datasets particionados Hive

A documentação de Hive Partitioning mostra leitura de diretórios particionados e pushdown de filtros em colunas de partição. Exemplo documentado:

```sql
SELECT *
FROM read_parquet('orders/*/*/*.parquet', hive_partitioning = true)
WHERE year = 2022
  AND month = 11;
```

DuckDB lê apenas os arquivos nas partições compatíveis.

Conclusão para o projeto:

```text
mov_estoque, nfe_agr, nfce_agr, c170_agr e nfe_entrada devem migrar para dataset particionado quando passarem de um limite de tamanho.
```

Partições candidatas:

```text
ano=<ano>/id_bucket=<000..127>/part-*.parquet
```

Fonte: DuckDB — Hive Partitioning.

### 2.4. DuckDB escreve Parquet particionado com COPY

A documentação de Partitioned Writes mostra `COPY ... TO ... (FORMAT parquet, PARTITION_BY (...))`, inclusive com opções como `OVERWRITE_OR_IGNORE`, `APPEND` e `FILENAME_PATTERN`.

Conclusão para o projeto:

```text
DuckDB pode ser usado para exportações grandes e também para criar datasets particionados quando a fonte for uma query SQL.
```

Fonte: DuckDB — Partitioned Writes.

### 2.5. Row groups e tamanho dos arquivos importam

O guia de performance de DuckDB informa que DuckDB paraleliza leitura de Parquet por row groups e recomenda row groups na faixa de 100 mil a 1 milhão de linhas. Também informa que arquivos individuais moderados são desejáveis e cita faixa ideal de 100 MB a 10 GB por arquivo individual.

Conclusão para o projeto:

```text
Ao salvar Parquet grande, configurar row_group_size entre 100_000 e 1_000_000 linhas.
Para datasets particionados, evitar milhares de arquivos minúsculos e também evitar arquivo único com row group gigante.
```

Fonte: DuckDB — Performance Guide: File Formats.

### 2.6. DuckDB suporta exportação grande via COPY

A documentação de Parquet do DuckDB mostra escrita de resultado para Parquet com `COPY`, incluindo compressão `zstd`.

Conclusão para o projeto:

```text
Exportações grandes da GUI não devem passar por DataFrame Python.
Devem usar DuckDB COPY para CSV ou Parquet.
```

Fonte: DuckDB — Reading and Writing Parquet Files.

### 2.7. DuckDB em Python precisa de conexão por thread

A documentação Python de DuckDB recomenda criar objetos de conexão em vez de usar a conexão global do módulo. Ela informa que `duckdb.sql()` e a conexão global não são thread-safe e que, para paralelismo, cada thread deve ter sua própria conexão.

Conclusão para o projeto:

```text
Cada worker Qt deve abrir sua própria conexão DuckDB.
Não compartilhar conexão global entre workers.
Não usar duckdb.sql() global no runtime da GUI.
```

Fonte: DuckDB — Python API.

### 2.8. Concorrência em DuckDB tem limites

A documentação de concorrência informa que um processo pode ler e escrever, e que múltiplos processos podem ler se o banco estiver em modo read-only. Escrita por múltiplos processos no mesmo arquivo DuckDB não é suportada automaticamente.

Conclusão para o projeto:

```text
A GUI deve consultar Parquet diretamente com conexões efêmeras.
O MCP pode usar arquivo .duckdb persistente, mas escrita deve ser tratada como operação controlada.
Evitar múltiplos processos escrevendo no mesmo .duckdb.
```

Fonte: DuckDB — Concurrency.

### 2.9. Polars continua válido para o pipeline fiscal

A documentação de Polars recomenda usar `scan_*` em vez de `read_*` quando se usa `LazyFrame`, pois isso permite ao otimizador pular colunas e linhas desnecessárias. A documentação também informa que `sink_parquet` permite executar a query em modo streaming e escrever para Parquet sem manter todo o resultado em RAM.

Conclusão para o projeto:

```text
No pipeline fiscal, manter Polars, mas trocar pontos de collect integral por scan/sink quando a saída for grande.
```

Fonte: Polars — Sources and Sinks; Polars LazyFrame API.

### 2.10. MCP Python SDK é uma base oficial viável

A documentação do Model Context Protocol lista o Python SDK como SDK oficial Tier 1. Isso valida a proposta de criar `tools/duckdb-mcp` com FastMCP.

Conclusão para o projeto:

```text
É adequado criar um MCP DuckDB local para diagnóstico, exploração e exportação assistida por agentes.
```

Fonte: Model Context Protocol — SDKs; MCP Python SDK.

## 3. Decisão técnica validada

### 3.1. O que fica em Polars

Manter Polars em:

- geração de produtos;
- descrição e normalização;
- fatores de conversão;
- movimentação de estoque;
- cálculos mensais;
- cálculos anuais;
- cálculos por períodos;
- invariantes fiscais;
- differential harness;
- transformações já testadas.

Racional:

```text
O núcleo fiscal já está em Polars e tem risco alto de regressão.
A primeira fase deve resolver travamento da interface sem reescrever regra fiscal.
```

### 3.2. O que passa para DuckDB

Usar DuckDB em:

- leitura paginada da aba Consulta;
- leitura paginada de `mov_estoque`;
- leitura paginada de `nfe_entrada`;
- leitura paginada das abas mensal/anual/períodos;
- tabelas de agregação superior e inferior;
- autocomplete/distinct sob demanda;
- exportação grande para CSV/Parquet;
- inspeção de Parquet;
- MCP para agentes.

Racional:

```text
DuckDB resolve o padrão interativo: SELECT poucas colunas + WHERE + LIMIT.
```

### 3.3. O que vira dataset particionado

Tabelas candidatas:

- `mov_estoque_<cnpj>`;
- `nfe_agr_<cnpj>`;
- `nfce_agr_<cnpj>`;
- `c170_agr_<cnpj>`;
- `nfe_entrada_<cnpj>`;
- eventualmente fontes auxiliares muito grandes.

Critério inicial:

```text
arquivo >= 2 GB -> salvar como dataset particionado
arquivo >= 512 MB -> ler na GUI somente via backend lazy/DuckDB
```

## 4. Arquitetura proposta

## 4.1. Camada nova: ParquetQueryService

Criar:

```text
src/interface_grafica/services/parquet_query_service.py
src/interface_grafica/services/duckdb_parquet_service.py
```

`ParquetQueryService` decide o backend:

```text
arquivo pequeno -> ParquetService/Polars atual, quando seguro
arquivo grande -> DuckDBParquetService
diretório particionado -> DuckDBParquetService
```

Interface comum:

```python
class ParquetQueryService:
    def get_schema(self, path): ...
    def get_page(self, path, conditions, visible_columns, page, page_size, sort_by=None, sort_desc=False): ...
    def get_count(self, path, conditions): ...
    def get_distinct_values(self, path, column, search="", limit=200): ...
    def export(self, path, conditions, columns, target, format): ...
```

## 4.2. Estado de tabela na GUI

Substituir DataFrames persistentes por estado de consulta:

```python
@dataclass
class TableQueryState:
    path: Path | None
    filters: list[FilterCondition]
    visible_columns: list[str]
    page: int
    page_size: int
    total_rows: int | None
    sort_by: str | None
    sort_desc: bool
```

Aplicar em:

- consulta principal;
- mov_estoque;
- nfe_entrada;
- mensal;
- anual;
- períodos;
- agregação superior;
- agregação inferior.

## 4.3. Modelo Qt

O `PolarsTableModel` continua recebendo `pl.DataFrame`, mas apenas da página atual.

Regras:

```text
modelo Qt nunca recebe arquivo inteiro grande
sort do modelo Qt só ordena a página, ou delega sort ao backend
seleção global usa chave, não índice da linha
```

## 4.4. Filtros

`FilterCondition` deve ser expandido para suportar:

```text
contains
ilike
equals
starts_with
ends_with
is_null
is_not_null
between_number
between_date
in_list
contains_any_visible_text
```

Operadores devem ser mapeados para SQL parametrizado.

## 4.5. Distinct/autocomplete

Não popular combos com `unique()` integral. Usar busca sob demanda:

```sql
SELECT DISTINCT "id_agrupado"
FROM read_parquet(?)
WHERE "id_agrupado" ILIKE ?
LIMIT 200;
```

## 5. Plano por PR

## PR 0.1 — Instrumentação

Branch:

```text
perf/gui-large-parquet-instrumentation
```

Entregas:

- medir tamanho de arquivo;
- medir memória RSS;
- medir tempo por operação;
- logar backend usado;
- exibir aviso na UI.

Critério de aceite:

```text
Logs indicam claramente quando uma operação tenta carregar arquivo grande.
```

## PR 0.2 — Guard rail anti-leitura integral

Branch:

```text
fix/gui-block-full-load-large-parquet
```

Entregas:

- `LARGE_PARQUET_THRESHOLD_MB = 512`;
- `load_dataset()` exige `allow_full_load=True` para arquivo grande;
- cache de DataFrame completo desabilitado para arquivo grande;
- mensagem de erro amigável.

Critério de aceite:

```text
Arquivo >512 MB não é carregado inteiro por acidente.
```

## PR 1.1 — DuckDBParquetService

Branch:

```text
feat/gui-duckdb-parquet-service
```

Entregas:

- adicionar `duckdb>=1.1.0`;
- criar `DuckDBParquetService`;
- implementar schema, page, count, distinct, export;
- testes unitários com Parquet sintético.

Critério de aceite:

```text
Serviço retorna página filtrada sem materializar arquivo inteiro.
```

## PR 1.2 — Serviço híbrido

Branch:

```text
perf/gui-auto-select-parquet-backend
```

Entregas:

- criar `ParquetQueryService`;
- rotear por tamanho e tipo de path;
- preservar API de paginação para a GUI.

Critério de aceite:

```text
A GUI chama apenas o serviço híbrido.
```

## PR 2.1 — Aba Consulta com DuckDB

Branch:

```text
perf/gui-query-large-parquet-with-duckdb
```

Entregas:

- migrar `reload_table()`;
- usar projeção de colunas;
- count opcional;
- primeira página antes do count total.

Critério de aceite:

```text
Parquet >2 GB abre primeira página sem travar.
```

## PR 3.1 — Remover loader integral da GUI

Branch:

```text
perf/gui-replace-full-parquet-async-loader
```

Entregas:

- substituir `_carregar_dados_parquet_async()`;
- criar loader paginado;
- criar distinct sob demanda.

Critério de aceite:

```text
Nenhuma nova renderização de tabela usa pl.read_parquet(path).
```

## PR 3.2 — Movimentação de estoque paginada

Branch:

```text
perf/gui-page-mov-estoque
```

Entregas:

- estado de consulta para `mov_estoque`;
- filtros traduzidos para backend;
- autocomplete de `id_agrupado` sob demanda;
- exportação respeitando filtros.

Critério de aceite:

```text
mov_estoque >2 GB abre, filtra e pagina sem guardar DataFrame completo.
```

## PR 3.3 — NFe entrada paginada

Branch:

```text
perf/gui-page-nfe-entrada
```

Entregas:

- aplicar mesmo padrão da `mov_estoque`;
- filtros de id, descrição, NCM, CO-SEFIN, data e texto.

Critério de aceite:

```text
NFe entrada grande funciona com página pequena e autocomplete sob demanda.
```

## PR 3.4 — Abas mensal/anual/períodos paginadas

Branch:

```text
perf/gui-page-stock-summary-tabs
```

Entregas:

- migrar `aba_mensal`;
- migrar `aba_anual`;
- migrar `aba_periodos`;
- manter filtros existentes.

Critério de aceite:

```text
Nenhuma dessas abas mantém DataFrame completo se arquivo for grande.
```

## PR 4.1 — Agregação paginada

Branch:

```text
perf/agregacao-page-produtos-agrupados
```

Entregas:

- tabela superior paginada;
- tabela inferior paginada;
- filtros rápidos no backend;
- filtro relacional via query.

Critério de aceite:

```text
Aba Agregação não usa load_dataset() para tabela inteira.
```

## PR 4.2 — Seleção por chave

Branch:

```text
perf/agregacao-selection-by-key
```

Entregas:

- seleção global por `id_agrupado`;
- seleção persiste ao trocar página/filtro;
- exibir contador de selecionados.

Critério de aceite:

```text
Usuário seleciona itens em páginas diferentes e consegue agregá-los.
```

## PR 4.3 — Agregar apenas IDs selecionados

Branch:

```text
perf/agregacao-load-only-selected-ids
```

Entregas:

- serviço consulta somente IDs selecionados;
- não materializa `produtos_agrupados` inteiro;
- testes com tabela sintética grande.

Critério de aceite:

```text
Agregação de poucos IDs não carrega tabela inteira.
```

## PR 4.4 — Delta manual de agregação

Branch:

```text
feat/agregacao-manual-delta-map
```

Entregas:

- salvar alterações manuais em delta pequeno;
- marcar derivados como defasados;
- reprocessar derivados sob demanda;
- manter log de agregações.

Critério de aceite:

```text
Agrupar poucos itens não dispara obrigatoriamente reprocessamento pesado imediato.
```

## PR 5.1 — Exportação grande via DuckDB COPY

Branch:

```text
perf/export-duckdb-copy-large-results
```

Entregas:

- exportar CSV via `COPY`;
- exportar Parquet via `COPY`;
- bloquear Excel para recorte grande;
- Word/HTML continuam limitados.

Critério de aceite:

```text
Exportação de milhões de linhas não passa por DataFrame Python.
```

## PR 6.1 — Leitura de arquivo ou dataset diretório

Branch:

```text
feat/parquet-query-file-or-dataset-dir
```

Entregas:

- árvore de arquivos reconhece dataset diretório;
- backend consulta `**/*.parquet`;
- manter compatibilidade com arquivo único.

Critério de aceite:

```text
GUI abre arquivo único e diretório particionado com o mesmo fluxo.
```

## PR 6.2 — Particionar mov_estoque

Branch:

```text
feat/parquet-partition-mov-estoque
```

Entregas:

- salvar `mov_estoque` por `ano` e `id_bucket`;
- configurar row group adequado;
- manter ponte de compatibilidade para consumidores antigos.

Critério de aceite:

```text
Filtro por ano/id reduz arquivos lidos.
```

## PR 6.3 — Particionar fontes grandes

Branch:

```text
feat/parquet-partition-large-source-tables
```

Entregas:

- particionar `nfe_agr`;
- particionar `nfce_agr`;
- particionar `c170_agr`;
- particionar `nfe_entrada`, se aplicável.

Critério de aceite:

```text
Tabelas acima de 2 GB são salvas como dataset particionado.
```

## PR 7.1 — Escrita streaming

Branch:

```text
perf/parquet-streaming-writes
```

Entregas:

- adicionar helper de escrita streaming;
- usar `sink_parquet` para LazyFrame grande;
- parametrizar `row_group_size`;
- preservar caminho atual para DataFrames pequenos.

Critério de aceite:

```text
LazyFrame grande salva sem collect integral.
```

## PR 8.1 — MCP DuckDB

Branch:

```text
feat/tools-duckdb-mcp
```

Entregas:

```text
tools/duckdb-mcp/server.py
tools/duckdb-mcp/pyproject.toml
tools/duckdb-mcp/.env.example
tools/duckdb-mcp/README.md
tools/duckdb-mcp/tests/test_server_tools.py
```

Dependências:

```toml
mcp>=1.7.0
duckdb>=1.1.0
pydantic>=2.0.0
python-dotenv>=1.0.0
```

Tools obrigatórias:

- `healthcheck`;
- `execute_sql`;
- `query_preview`;
- `explain_sql`;
- `list_tables`;
- `describe_table`;
- `create_table_from_file`;
- `export_query`;
- `run_maintenance`;
- `inspect_parquet`;
- `preview_parquet`.

Critério de aceite:

```text
MCP executa preview, explain, export e bloqueia paths fora de DUCKDB_ALLOWED_DIRS.
```

## 6. Implementação do MCP DuckDB

### 6.1. Estrutura

```text
tools/duckdb-mcp/
  server.py
  pyproject.toml
  .env.example
  README.md
  tests/
    test_server_tools.py
```

### 6.2. `.env.example`

```env
DUCKDB_PATH=/tmp/analytics.duckdb
DUCKDB_THREADS=4
DUCKDB_MEMORY_LIMIT=4GB
DUCKDB_ALLOWED_DIRS=/tmp,/data,/mnt/data
DUCKDB_DEFAULT_LIMIT=1000
DUCKDB_MAX_PREVIEW_ROWS=10000
```

### 6.3. Regras de segurança

Mesmo com escrita liberada:

```text
DUCKDB_ALLOWED_DIRS é obrigatório em ambientes reais.
Qualquer path fora dos diretórios permitidos deve falhar.
Tools especializadas devem validar path antes de criar, ler ou exportar arquivo.
```

### 6.4. Observação sobre `execute_sql`

`execute_sql` é propositalmente poderoso. Ele permite escrita livre, incluindo `CREATE`, `INSERT`, `UPDATE`, `DELETE`, `COPY`, `ATTACH`, `DROP`, `VACUUM` e `CHECKPOINT`.

Como SQL livre pode embutir caminhos de arquivo, a validação perfeita de path é difícil. Por isso:

```text
- usar execute_sql apenas em contexto confiável;
- preferir export_query e create_table_from_file para operações com arquivo;
- documentar risco no README;
- manter DUCKDB_ALLOWED_DIRS configurado.
```

## 7. Benchmarks e testes grandes

Criar:

```text
scripts/generate_large_parquet_fixture.py
bench/test_gui_large_parquet_query.py
```

Tamanhos de teste:

```text
256 MB
1 GB
2 GB
```

Métricas:

- tempo para primeira página;
- memória máxima RSS;
- tempo para filtro por `id_agrupado`;
- tempo para filtro textual;
- tempo para troca de página;
- tempo para `distinct` limitado;
- tempo para exportação CSV/Parquet.

Critérios mínimos:

```text
primeira página < 5s em SSD local
RAM adicional < 1 GB para arquivo de 2 GB
troca de página < 2s sem sort global
filtro por id < 5s
nenhum cache de DataFrame completo para arquivo grande
```

## 8. Ordem recomendada final

```text
1. Instrumentação
2. Guard rail anti-leitura integral
3. DuckDBParquetService
4. Serviço híbrido
5. Aba Consulta
6. Count assíncrono
7. mov_estoque
8. nfe_entrada
9. mensal/anual/períodos
10. agregação paginada
11. seleção por chave
12. agregação por IDs
13. exportação via COPY
14. MCP DuckDB
15. leitura de diretório particionado
16. particionar mov_estoque
17. particionar fontes grandes
18. escrita streaming
19. benchmarks grandes
```

## 9. Regra permanente para novas PRs

```text
Não usar pl.read_parquet(path) em arquivo de usuário dentro da GUI.
Não usar load_dataset() para arquivo grande.
Não guardar DataFrame completo em modelo Qt.
Não popular combo com unique() integral.
Não exportar resultado gigante via DataFrame em memória.
Não aplicar filtros grandes depois da materialização.
```

Caminho padrão:

```text
DuckDB/scan -> filtros -> projeção de colunas -> página pequena -> modelo Qt
```

## 10. Referências pesquisadas

1. DuckDB — Reading and Writing Parquet Files: https://duckdb.org/docs/lts/data/parquet/overview.html
2. DuckDB — Hive Partitioning: https://duckdb.org/docs/current/data/partitioning/hive_partitioning.html
3. DuckDB — Partitioned Writes: https://duckdb.org/docs/stable/data/partitioning/partitioned_writes
4. DuckDB — Performance Guide: File Formats: https://duckdb.org/docs/current/guides/performance/file_formats.html
5. DuckDB — Python API: https://duckdb.org/docs/stable/clients/python/overview
6. DuckDB — Multiple Python Threads: https://duckdb.org/docs/current/guides/python/multiple_threads.html
7. DuckDB — Concurrency: https://duckdb.org/docs/current/connect/concurrency.html
8. Polars — Sources and Sinks: https://docs.pola.rs/user-guide/lazy/sources_sinks/
9. Polars — LazyFrame API: https://docs.pola.rs/api/python/stable/reference/lazyframe/
10. Model Context Protocol — SDKs: https://modelcontextprotocol.io/docs/sdk
11. MCP Python SDK: https://github.com/modelcontextprotocol/python-sdk
