# Cálculos do Relatório Mensal

Este documento explica os cálculos e as colunas do relatório mensal (aba mensal) gerado pelo sistema.

## Dicionário de Colunas (Aba Mensal)

A tabela abaixo descreve detalhadamente cada campo apresentado no relatório mensal, seguindo a ordem real de geração:

| Coluna | Descrição Técnica | Precisão |
| :--- | :--- | :--- |
| **ano** | Ano civil da movimentação. | Inteiro |
| **mes** | Mês do calendário (1 a 12). | Inteiro |
| **id_agregado** | Identificador único do produto (agrupado). | Texto |
| **descr_padrao** | Descrição padronizada do produto agrupado. | Texto |
| **unids_mes** | Lista de unidades de medida encontradas nas NFes do mês. | Lista |
| **unids_ref_mes** | Unidades de medida de referência utilizadas para os cálculos. | Lista |
| **it_in_st** | Identifica situação de ST ('S' ou 'N') no último registro do mês. | Texto |
| **valor_entradas** | Valor financeiro total das entradas no mês (compras/transferências). | 2 decimais |
| **qtd_entradas** | Quantidade física total das entradas (convertida). | 4 decimais |
| **pme_mes** | **Preço Médio de Entrada**: Média ponderada das aquisições válidas. | 4 decimais |
| **valor_saidas** | Valor financeiro total das saídas no mês (vendas/baixas). | 2 decimais |
| **qtd_saidas** | Quantidade física total das saídas (convertida). | 4 decimais |
| **pms_mes** | **Preço Médio de Saída**: Média ponderada das vendas/saídas válidas. | 4 decimais |
| **entradas_desacob** | **Omissão de Entrada**: Quantidade física de entrada sem nota detectada pelo saldo. | 4 decimais |
| **ICMS_entr_desacob** | Valor estimado de ICMS sobre as entradas omitidas (apenas para itens ST). | 2 decimais |
| **saldo_mes** | Saldo final de estoque disponível no encerramento do mês. | 4 decimais |
| **custo_medio_mes** | Custo médio unitário do produto ao final do mês. | 4 decimais |
| **valor_estoque** | Avaliação financeira do estoque final (`saldo_mes` × `custo_medio_mes`). | 2 decimais |

## Detalhamento dos Cálculos

### 1. Preços Médios (PME e PMS)

Os preços médios mensais são calculados apenas sobre registros "válidos", desconsiderando:
*   Devoluções (`dev_simples`, `dev_venda`, `dev_compra`, `dev_ent_simples` ou `finnfe == 4`).
*   Registros marcados para exclusão (`excluir_estoque`).

**Fórmulas:**
*   **PME_mes**: $\frac{\sum \text{Valor Entradas Válidas}}{\sum \text{Qtd Entradas Válidas}}$
*   **PMS_mes**: $\frac{\sum |\text{Valor Saídas Válidas}|}{\sum |\text{Qtd Saídas Válidas}|}$

### 2. Omissão de Entrada e ICMS Devido (`ICMS_entr_desacob`)

O sistema calcula um valor estimado de ICMS para compensar entradas que não foram registradas (identificadas quando o saldo físico resultaria em negativo).

> [!IMPORTANT]
> Este cálculo é aplicado exclusivamente quando:
> 1. O item é sujeito a Substituição Tributária (**it_in_st == 'S'**).
> 2. Foi detectada omissão de entrada (**entradas_desacob > 0**).

**Fórmula:**
$$ICMS\_entr\_desacob = \text{entradas\_desacob} \times \text{Base\_Calculo} \times \left(\frac{\text{aliq\_mes}}{100}\right)$$

**Definição da Base de Cálculo:**
1.  **Prioridade 1**: Se houve saídas no mês (`pms_mes > 0`), utiliza-se o **pms_mes**.
2.  **Prioridade 2 (MVA)**: Caso não haja saídas válidas, utiliza-se o **pme_mes** ajustado pelo MVA:
    *   $\text{Base} = \text{pme\_mes} \times \text{Fator\_MVA}$
    *   $\text{Fator\_MVA} = 1 + \left(\frac{\text{mva\_pct\_mes}}{100}\right)$

**Parâmetros de Alíquota e MVA:**
*   **aliq_mes**: Alíquota interna obtida da última entrada do mês (ou do último registro se não houver entrada). Origem: `Aliq_icms` ou `it_pc_interna`.
*   **mva_pct_mes**: Percentual de MVA (`it_pc_mva`) do último registro do mês.

---

## Fontes de Dados

*   **Script de Transformação**: `src/transformacao/calculos_mensais.py`
*   **Origem dos Dados**: `analises/produtos/mov_estoque_<cnpj>.parquet`
*   **Arquivo de Saída**: `analises/produtos/aba_mensal_<cnpj>.parquet`
