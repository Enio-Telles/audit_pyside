# Runbook: Rollback do DuckDB no Pipeline GUI

Este runbook documenta os procedimentos operacionais para desabilitar o backend DuckDB do `ParquetQueryService` em caso de regressões severas (OOM, crashes C++, lentidão extrema em produção) sem a necessidade de re-deploy de código.

## Cenários de Ativação

- **Crashes não tratados (Segfault / C++ Exception)** reportados na aba Consulta ou Relatórios devido a chamadas ao DuckDB (especialmente no Windows/PyInstaller).
- **Consumo de Memória Extremo (OOM)** em queries que deveriam rodar via out-of-core.
- **Lentidão Severa (P95 > 10s)** na abertura ou filtragem de parquets > 512 MB, impossibilitando a operação.

## Procedimento de Rollback de Emergência

O roteamento entre Polars e DuckDB no `ParquetQueryService` é automático e baseado em limiares e diretórios, mas **pode ser bypassado completamente em tempo de execução via variável de ambiente**.

1. Feche o aplicativo `audit_pyside` em todas as instâncias afetadas.
2. Configure a variável de ambiente de sistema:
   ```cmd
   setx AUDIT_DISABLE_DUCKDB "1"
   ```
   *No Linux/macOS:*
   ```bash
   export AUDIT_DISABLE_DUCKDB=1
   ```
3. Reinicie o aplicativo `audit_pyside`.

### Confirmação do Bypass

Ao rodar com `AUDIT_DISABLE_DUCKDB=1`:
- O `ParquetQueryService` ignorará o `DuckDBParquetService` e roteará *tudo* para o `ParquetService` (Polars nativo).
- O log inicial do aplicativo registrará `[WARN] DuckDB desabilitado via variavel de ambiente AUDIT_DISABLE_DUCKDB`.

## Impactos do Rollback

- **Consumo de RAM (RSS):** Arquivos maiores que 512 MB serão lidos e oxigenados no Polars String Cache. Espera-se um aumento dramático no pico de consumo de RAM (pode passar de 8 GB para arquivos de 2 GB).
- **Risco de OOM real:** Máquinas com pouca memória (<= 8GB RAM) poderão falhar ("Out of Memory") ao abrir parquets massivos.
- **Corretude:** Os dados não sofrerão corrupção, pois tanto Polars quanto DuckDB realizam apenas leituras em parquets finais. **Nenhum reprocessamento de dados da pipeline é necessário**.

## Estratégia de Remoção Completa (Code-Level)

Se a regressão for insolúvel e a decisão de negócio for remover o DuckDB permanentemente (referência: PR #218):

1. Excluir `src/interface_grafica/services/duckdb_parquet_service.py`.
2. Remover a dependência de roteamento em `ParquetQueryService` (limpar condicional `usa_duckdb`).
3. Remover a dependência de build em `requirements.txt` / `pyproject.toml` (`duckdb`).
4. Mergear as alterações após validação de smoke tests na infraestrutura.

## Monitoramento Pós-Rollback

Após aplicar a mitigação, observe e monitore:
- **TTFP (Time to First Page)** na abertura de parquets grandes (>512 MB). O Polars pode se mostrar mais rápido, porém com um custo de RAM muito maior.
- Coletar logs de crashes. Se os crashes pararem, a hipótese de falha nativa do DuckDB é confirmada.
