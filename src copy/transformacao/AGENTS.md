# AGENT – Pipeline (src/transformacao)

Este agente se aplica aos scripts e módulos de **pipeline** em `src/transformacao`. Seu papel é garantir que as transformações de dados sejam idempotentes, rastreáveis e alinhadas com as políticas fiscais.

## Responsabilidades

- **Extrair** dados das fontes (Oracle, CSV, Parquet) apenas quando necessário, seguindo a política cache-first.  
- **Normalizar** tipos e nomes, mantendo as chaves `id_agrupado`, `id_agregado` e `__qtd_decl_final_audit__` em todas as linhas.  
- **Agregar** e derivar métricas utilizando **Polars**, sem repetir lógica existente em outras camadas.  
- **Materializar** datasets em Parquet com schema estável e versionado, documentando origem, filtros e período.  
- **Orquestrar** a sequência **raw → base → curated → marts/views** de forma clara, com um script responsável por cada etapa.

## Convenções

- **Separação de etapas**: crie módulos separados para extração (`raw`), normalização (`base`) e agregação (`curated`). Evite scripts monolíticos que misturam etapas.  
- **Reuso de funções**: utilize utilitários comuns de transformação. Se uma função existe (p. ex. para normalizar campos NCM), não a reimplemente.  
- **Polars LazyFrame**: trabalhe com `pl.scan_parquet` e `pl.scan_csv` para operações lazy, e chame `collect()` apenas nos pontos de checkpoint definidos.  
- **Sem SQL inline**: queries de extração devem residir no diretório `sql/` e ser chamadas via funções apropriadas.  
- **Logging e lineage**: registre, no início de cada pipeline, o dataset de origem, o período (ano-mês) e o CNPJ processado. No final, registre o nome do dataset gerado e seu schema.  
- **Tipos consistentes**: padronize colunas de data, monetária e quantidade; use `Decimal` ou `Float64` de forma explícita.  
- **Chaves invariantes**: assegure que `id_agrupado` e `id_agregado` não sejam quebrados ou redefinidos. Para novos agrupamentos, crie chaves derivadas separadas.

## Formato A–E

Ao planejar uma nova pipeline ou modificar uma existente, responda no formato A–E.  
Especifique claramente qual camada será impactada, quais datasets podem ser reaproveitados e quais validações (testes de reconciliação, checagem de schema) serão necessárias.

## Anti‑padrões específicos

- **Misturar extração e agregação** no mesmo script sem dividir em etapas.  
- **Usar pandas** para grandes volumes quando **Polars** seria mais eficiente.  
- **Criar Parquet sem registrar schema**, dificultando a compatibilidade futura.  
- **Aplicar lógica fiscal** (cálculo de impostos, conformidade) no pipeline sem documentação ou validação.