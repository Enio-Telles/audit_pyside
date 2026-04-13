# SEFIN Classification Padding & GitHub Sync

## Goal 1: Fix SEFIN Padding & Source

The user reported two specific improvements to the logic generating `it_pc_interna` through `it_in_reducao_credito` fields.

**Issue 1**: Instead of generating a [co_sefin_final](file:///c:/funcoes%20-%20Copia/src/transformacao/co_sefin_class.py#22-74) strictly from `ncm_padrao` and `cest_padrao` natively inside [co_sefin_class.py](file:///c:/funcoes%20-%20Copia/src/transformacao/co_sefin_class.py), we should map and use `co_sefin_padrao` directly from the `produtos_agrupados_<cnpj>.parquet` file as the lookup key. 

**Fix 1**: We will modify the signature of [enriquecer_co_sefin_class(df_mov, cnpj: str = None)](file:///c:/funcoes%20-%20Copia/src/transformacao/co_sefin_class.py#75-194) so it can locate and load `produtos_agrupados_<cnpj>.parquet`. It will join this table to get `co_sefin_padrao` on [id_agrupado](file:///c:/funcoes%20-%20Copia/src/transformacao/04_produtos_final.py#42-44), using it as the anchor for classification. If [cnpj](file:///c:/funcoes%20-%20Copia/src/interface_grafica/services/parquet_service.py#57-59) is not passed, it will gracefully fall back to the old generation method.

**Issue 2**: The matching logic between `dt_referencia` and `da_inicio` / `da_final` fails when `da_final` is null in the reference dataset. A [null](file:///c:/funcoes%20-%20Copia/src/transformacao/c170_xml.py#143-147) upper limit currently evaluates the `dt_referencia <= da_final` expression to [null](file:///c:/funcoes%20-%20Copia/src/transformacao/c170_xml.py#143-147), meaning valid classifications that are active indefinitely are getting prematurely filtered out.

**Fix 2**: We will correct the date validity comparison operator:
```python
    cond_dentro_do_prazo = (
        (pl.col("da_inicio").is_null() | (pl.col("dt_referencia") >= pl.col("da_inicio"))) & 
        (pl.col("da_final").is_null() | (pl.col("dt_referencia") <= pl.col("da_final")))
    )
```

Additionally, as before, if a row remains unmatched after adjusting the dates, we can still fall back to the most recent Sefin parameter (using `it_da_inicio`) for that [co_sefin](file:///c:/funcoes%20-%20Copia/src/transformacao/co_sefin_class.py#22-74) to guarantee complete coverage.

### [MODIFY] [c:\Sistema_pysisde\src\transformacao\co_sefin_class.py](file:///funcoes%20-%20Copia/src/transformacao/co_sefin_class.py)
We will rewrite [enriquecer_co_sefin_class](file:///c:/funcoes%20-%20Copia/src/transformacao/co_sefin_class.py#75-194) and its imports to support these two fixes (loading [produtos_agrupados](file:///c:/funcoes%20-%20Copia/src/transformacao/04_produtos_final.py#112-246) and fixing the date condition).

### [MODIFY] [c:\Sistema_pysisde\src\transformacao\c170_xml.py](file:///funcoes%20-%20Copia/src/transformacao/c170_xml.py)
### [MODIFY] [c:\Sistema_pysisde\src\transformacao\movimentacao_estoque.py](file:///funcoes%20-%20Copia/src/transformacao/movimentacao_estoque.py)
Pass the [cnpj](file:///c:/funcoes%20-%20Copia/src/interface_grafica/services/parquet_service.py#57-59) variable when calling [enriquecer_co_sefin_class(df_mov, cnpj)](file:///c:/funcoes%20-%20Copia/src/transformacao/co_sefin_class.py#75-194).

## Goal 2: GitHub Synchronization
We need to sync our ongoing local changes with the remote [main](file:///c:/funcoes%20-%20Copia/src/extracao/extrair_dados_cnpj.py#177-201) branch of `https://github.com/Enio-Telles/sefin_audit_5`.

**Proposed Steps**:
1. Run `git status` and `git add .`
2. Run `git commit -m "Fix preco_item padding and SEFIN classification gaps"`
3. Run `git pull origin main --rebase` to integrate any remote changes.
4. Run `git push origin main`.

## Verification
- We will process `37671507000187` and assert that no row with a valid [co_sefin_final](file:///c:/funcoes%20-%20Copia/src/transformacao/co_sefin_class.py#22-74) has an empty `it_pc_interna`.

