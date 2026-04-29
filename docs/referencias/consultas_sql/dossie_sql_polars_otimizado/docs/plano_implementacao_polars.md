# Plano de implementação da visualização otimizada do dossiê em Python Polars com integração Parquet

## 1. Objetivo

Construir uma visualização otimizada do dossiê fiscal do contribuinte com:
- extração modular via SQL Oracle;
- persistência em Parquet por domínio;
- integração com outra base de dados em Parquet;
- consolidação final em **Polars LazyFrame**;
- geração de um dossiê consultável por abas/paineis temáticos.

## 2. Princípios de arquitetura

### 2.1. Separação em camadas
1. **Camada de extração SQL**
   - Executa as consultas singulares do pacote.
   - Salva cada resultado como parquet bruto ou bronze.

2. **Camada de enriquecimento tabular**
   - Padroniza tipos, datas, chaves e nomes.
   - Resolve deduplicação e normalização.

3. **Camada de integração**
   - Faz joins com a base externa em Parquet.
   - Cria indicadores derivados para a visualização.

4. **Camada de apresentação**
   - Produz tabelas resumo, timelines, alertas, painéis e drill-downs.

### 2.2. Chaves canônicas
Padronizar desde o início:
- `co_cnpj_cpf`
- `co_cad_icms`
- `chave_acesso`
- `nu_acao_fiscal`
- `it_nu_diligencia`
- `cpf_cnpj_socio`
- `ano`
- `periodo_ym` no formato `YYYY-MM`

## 3. Estrutura de diretórios sugerida

```text
projeto_dossie/
├─ conf/
│  ├─ fontes.yaml
│  ├─ schemas.yaml
│  └─ consultas.yaml
├─ sql/
│  └─ ...consultas deste pacote...
├─ data/
│  ├─ bronze/
│  │  ├─ oracle/
│  │  └─ parquet_externo/
│  ├─ silver/
│  │  ├─ contribuinte/
│  │  ├─ cadastro/
│  │  ├─ societario/
│  │  ├─ documentos/
│  │  ├─ arrecadacao/
│  │  └─ fiscalizacao/
│  └─ gold/
│     ├─ dossie_resumo/
│     ├─ dossie_timeline/
│     └─ dossie_alertas/
├─ src/
│  ├─ extract/
│  ├─ transform/
│  ├─ integrate/
│  ├─ marts/
│  └─ app/
└─ notebooks/
```

## 4. Estratégia de extração SQL

## 4.1. Extração por domínio
Executar cada SQL singular separadamente e salvar como parquet:
- `00_base/*` -> `silver/contribuinte/`
- `10_cadastro/*` -> `silver/cadastro/`
- `20_societario/*` -> `silver/societario/`
- `30_documentos_fiscais/*` -> `silver/documentos/`
- `40_arrecadacao_regularidade/*` -> `silver/arrecadacao/`
- `50_fiscalizacao_conformidade/*` -> `silver/fiscalizacao/`

## 4.2. Granularidade ideal
Persistir os datasets em dois níveis:
- **fato detalhado**: um parquet por consulta singular;
- **agregado reutilizável**: um parquet por consulta agregada.

Exemplo:
- `30_nfe_movimento_item.parquet`
- `33_vaf_anual.parquet`

## 4.3. Particionamento recomendado
Se o volume for grande, particionar por:
- `co_cnpj_cpf`
- `ano`
- `origem_doc` quando aplicável
- `dominio`

Exemplo de path:
```text
data/silver/documentos/30_nfe_movimento_item/co_cnpj_cpf=12345678000199/ano=2025/part-000.parquet
```

## 5. Integração com outra base de dados Parquet

## 5.1. Tipos de integração esperados
A base externa em parquet pode trazer:
- scoring de risco;
- grafos societários;
- informações bancárias ou de meios de pagamento;
- georreferenciamento;
- malhas analíticas;
- séries históricas já tratadas.

## 5.2. Regras de integração
1. Nunca juntar direto na extração Oracle.
2. Persistir primeiro os dados Oracle.
3. Integrar em Polars por chaves padronizadas.
4. Manter a origem dos campos em colunas de rastreabilidade.

## 5.3. Colunas de rastreabilidade
Toda tabela integrada deve incluir:
- `origem_sistema`
- `origem_arquivo`
- `data_extracao`
- `data_processamento`
- `versao_regra`
- `hash_linha` quando necessário

## 6. Modelagem analítica sugerida

## 6.1. Entidades centrais
- `dim_contribuinte`
- `fato_documento_fiscal`
- `fato_arrecadacao`
- `fato_evento_fiscalizacao`
- `fato_evento_cadastral`
- `fato_relacionamento_societario`

## 6.2. Mart final do dossiê
Produzir pelo menos 4 visões ouro:

### a) `gold_dossie_resumo`
Uma linha por contribuinte contendo:
- identificação;
- situação atual;
- regime atual;
- total de entrada e saída;
- VAF por ano;
- inadimplência total;
- quantidade de ações fiscais;
- quantidade de autos;
- quantidade de notificações;
- quantidade de sócios atuais;
- flags de risco.

### b) `gold_dossie_timeline`
Eventos históricos com colunas:
- `tipo_evento`
- `subtipo_evento`
- `data_evento`
- `descricao_curta`
- `valor_evento`
- `chave_evento`
- `co_cnpj_cpf`

### c) `gold_dossie_societario`
- sócios atuais e antigos;
- empresas dos sócios;
- inadimplência cruzada;
- sinais de rede.

### d) `gold_dossie_documental`
- notas de entrada/saída;
- MDF-e relacionado;
- IP de transmissão;
- comparativo DIMP x documentos fiscais.

## 7. Pipeline em Polars

## 7.1. Padrão de leitura
Usar `scan_parquet` em vez de `read_parquet` sempre que possível.

```python
import polars as pl

dim_contribuinte = pl.scan_parquet("data/silver/contribuinte/**/*.parquet")
docs_nfe = pl.scan_parquet("data/silver/documentos/**/*.parquet")
base_externa = pl.scan_parquet("data/bronze/parquet_externo/**/*.parquet")
```

## 7.2. Padronização de tipos
Centralizar funções de:
- limpeza de CNPJ/CPF;
- parse de datas;
- cast numérico;
- colunas booleanas de regra.

```python
import polars as pl

def normalizar_cnpj(expr: pl.Expr) -> pl.Expr:
    return (
        expr.cast(pl.Utf8)
        .str.replace_all(r"\D+", "")
        .str.zfill(14)
    )

def to_periodo_ym(expr: pl.Expr) -> pl.Expr:
    return expr.dt.strftime("%Y-%m")
```

## 7.3. Exemplo de integração com base externa
```python
docs = (
    pl.scan_parquet("data/silver/documentos/30_nfe_movimento_item/**/*.parquet")
    .with_columns([
        normalizar_cnpj(pl.col("co_emitente")).alias("co_emitente"),
        normalizar_cnpj(pl.col("co_destinatario")).alias("co_destinatario"),
    ])
)

score_externo = (
    pl.scan_parquet("data/bronze/parquet_externo/score_risco/**/*.parquet")
    .with_columns(normalizar_cnpj(pl.col("co_cnpj_cpf")).alias("co_cnpj_cpf"))
)

resumo_docs = (
    docs.group_by("co_emitente")
        .agg([
            pl.sum("valor_item").alias("valor_total_emitido"),
            pl.n_unique("chave_acesso").alias("qtd_notas_emitidas"),
        ])
        .rename({"co_emitente": "co_cnpj_cpf"})
)

resumo_integrado = (
    resumo_docs
    .join(score_externo, on="co_cnpj_cpf", how="left")
)
```

## 8. Estratégia de visualização

## 8.1. Camadas de visualização
Separar a UI em 6 abas:

1. **Resumo executivo**
2. **Cadastro e histórico**
3. **Documentos fiscais**
4. **Arrecadação e regularidade**
5. **Fiscalização e contencioso**
6. **Rede societária e vínculos**

## 8.2. Componentes visuais sugeridos
- cartões KPI;
- tabelas drill-down;
- timeline de eventos;
- heatmap por ano/período;
- grafo societário;
- matriz de risco;
- tabela comparativa DIMP x NF;
- mapa de endereços/IP/UF de emissão.

## 8.3. Frameworks possíveis
- **Streamlit** para velocidade de entrega;
- **FastAPI + React** para produto mais robusto;
- **Panel** ou **Dash** se já houver ecossistema analítico.

## 8.4. Recomendação objetiva
Para o primeiro ciclo:
- backend de transformação: **Polars**
- camada de cache: **Parquet**
- visualização: **Streamlit**
- orquestração: **Prefect** ou `cron + scripts` se o ambiente for simples

## 9. Exemplo de geração do resumo ouro

```python
import polars as pl

contrib = pl.scan_parquet("data/silver/contribuinte/**/*.parquet")
cadastro = pl.scan_parquet("data/silver/cadastro/10_cadastro_principal/**/*.parquet")
vaf = pl.scan_parquet("data/silver/documentos/33_vaf_anual/**/*.parquet")
conta = pl.scan_parquet("data/silver/arrecadacao/41_conta_corrente_agregado/**/*.parquet")
acoes = pl.scan_parquet("data/silver/fiscalizacao/56_acoes_fiscais_agregado/**/*.parquet")
autos = pl.scan_parquet("data/silver/fiscalizacao/58_autos_infracao/**/*.parquet")

vaf_resumo = (
    vaf.group_by("co_cnpj_cpf")
       .agg([
           pl.sum("entrada").alias("entrada_total"),
           pl.sum("saida").alias("saida_total"),
           pl.max("ano").alias("ultimo_ano_vaf"),
       ])
)

conta_resumo = (
    conta.group_by("co_cnpj_cpf")
         .agg(pl.sum("valor_total").alias("saldo_total_conta_corrente"))
)

acoes_resumo = (
    acoes.group_by("co_cnpj_cpf")
         .agg(pl.n_unique("acao_fiscal").alias("qtd_acoes_fiscais"))
)

autos_resumo = (
    autos.group_by("co_cnpj_cpf")
         .agg(pl.sum("total").alias("valor_total_autos"))
)

gold_dossie_resumo = (
    contrib
    .join(cadastro, on="co_cnpj_cpf", how="left")
    .join(vaf_resumo, on="co_cnpj_cpf", how="left")
    .join(conta_resumo, on="co_cnpj_cpf", how="left")
    .join(acoes_resumo, on="co_cnpj_cpf", how="left")
    .join(autos_resumo, on="co_cnpj_cpf", how="left")
)

gold_dossie_resumo.sink_parquet("data/gold/dossie_resumo/dossie_resumo.parquet")
```

## 10. Estratégia de performance

## 10.1. SQL
- evitar `LIKE '%...%'` em produção quando houver alternativa indexável;
- extrair fatos brutos uma vez;
- reutilizar parquet intermediário;
- não recalcular históricos em toda navegação.

## 10.2. Polars
- privilegiar `LazyFrame`;
- aplicar filtros cedo;
- selecionar somente colunas necessárias;
- evitar `.collect()` prematuro;
- persistir marts ouro.

## 10.3. Cache
Criar cache por:
- contribuinte;
- período;
- domínio;
- versão da regra.

## 11. Regras de governança

- Toda tabela precisa de dicionário de colunas.
- Toda regra derivada precisa de nome e versão.
- Toda integração parquet externa precisa de contrato de schema.
- Toda transformação crítica precisa de teste automatizado.

## 12. Testes mínimos

### 12.1. Testes de schema
- tipos esperados;
- colunas obrigatórias;
- não nulidade de chave principal.

### 12.2. Testes de negócio
- um contribuinte sem IE não pode quebrar a pipeline;
- uma NF autorizada deve entrar no dataset documental;
- um contribuinte sem ação fiscal deve permanecer no resumo;
- totais agregados devem fechar com as bases singulares.

### 12.3. Testes de reconciliação
- comparar total por consulta original x total por consulta atomizada;
- validar contagem de registros antes/depois;
- validar amostra manual por contribuinte.

## 13. Roadmap recomendado

### Fase 1 — Fundacional
- implementar `00_base`
- implementar `10_cadastro`
- persistir em parquet
- montar resumo executivo simples

### Fase 2 — Documental e arrecadação
- implementar `30_documentos_fiscais`
- implementar `40_arrecadacao_regularidade`
- criar comparativos anuais e mensais

### Fase 3 — Fiscalização e societário
- implementar `20_societario`
- implementar `50_fiscalizacao_conformidade`
- criar timeline consolidada e alertas de risco

### Fase 4 — Integração externa
- incorporar a base parquet adicional
- definir chaves de vínculo
- adicionar score/grafo/indicadores externos

## 14. Entrega mínima viável

A primeira versão útil do dossiê deve conter:
- identificação do contribuinte;
- situação cadastral;
- histórico de situação;
- VAF anual;
- entradas e saídas;
- conta corrente agregada;
- ações fiscais e autos;
- notificações;
- sócios atuais;
- empresas relacionadas dos sócios;
- visualização em 6 abas com drill-down.

## 15. Conclusão

O melhor desenho para esse dossiê é:
- SQL modular para extração;
- Parquet como camada persistente intermediária;
- Polars para integração, agregação e performance;
- UI desacoplada da lógica de negócio.

Esse modelo reduz acoplamento, melhora auditoria, simplifica manutenção e facilita a integração com outras bases analíticas já materializadas em Parquet.
