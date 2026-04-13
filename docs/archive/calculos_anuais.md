# Relatório Anual - Metodologia e Composição

O Relatório Anual (Aba Anual) fornece uma visão consolidada por produto ao longo de cada ano civil. Ele é a principal ferramenta para auditoria de omissões de entrada e saída, comparando o fluxo de mercadorias declarado com o saldo físico calculado.

## 1. Origem dos Dados e Diferenças de Cálculo

Existem dois motores de cálculo no sistema: o **Cálculo por Período** e o **Cálculo Anual**.

* **Cálculo por Período (Cronológico)**: Considera todos os inventários registrados (intermediários). Se houver um inventário em Março, o saldo é "ajustado" para aquele valor, ocultando possíveis omissões ocorridas entre Janeiro e Fevereiro.
* **Cálculo Anual (Auditoria)**:

  * O saldo é **reiniciado (zerado)** no primeiro dia de cada ano (01/01).
  * **Inventários Intermediários são ignorados**: Registros de estoque inicial (Tipo 0) que não sejam 01/01 e estoque final (Tipo 3) que não sejam 31/12 têm sua quantidade zerada no cálculo anual.
  * **Objetivo**: Identificar se as Entradas e Saídas declaradas ao longo de *todo o ano* são consistentes com as fotos de estoque de abertura e fechamento anual.

---

## 2. Composição das Colunas da Tabela

Abaixo, a explicação de cada coluna presente na Aba Anual:

### Dados Identificadores

* **ano**: O ano civil da auditoria.
* **codigo_produto_ajustado**: O código único do produto (após aplicação de De/Para).
* **unids / unids_ref**: Unidades de medida encontradas nas notas e a unidade de referência adotada.
* **cods_trib**: Códigos de tributação (CRT/CST) associados ao produto no ano.

### Fluxo de Quantidades (Físico)

* **estoque_inicial**: Quantidade declarada no inventário em 01/01 (Tipo 0).
* **entradas**: Soma das quantidades de todas as notas fiscais de entrada (Tipo 1) do ano.
* **estoque_final**: Quantidade declarada no inventário em 31/12 (Tipo 3).
* **saidas**: Soma (em valor absoluto) das quantidades das notas fiscais de saída (Tipo 2).
* **entradas_desacobertadas**: Quantidade total de omissões de entrada detectadas pelo cálculo cronológico anual (quando o saldo fica negativo).
* **saldo_final**: O saldo físico calculado pelo sistema ao final do ano, considerando `Est_Ini + Entradas + Omissões - Saídas`.

### Indicadores de Auditoria

* **saidas_calculadas**: Representa quanto o contribuinte *deveria* ter vendido.

  * `Fórmula: Estoque_Inicial + Entradas + Entradas_Desacobertadas - Estoque_Final`
* **saidas_desacobertadas**: Ocorre quando as saídas calculadas são maiores que as saídas efetivamente declaradas nas notas fiscais.

  * `Fórmula: max(0, Saídas_Calculadas - Saídas_Declaradas)`
* **estoque_final_desacoberto**: Ocorre quando o contribuinte declara um estoque final (31/12) maior do que o sistema calculou como disponível.

  * `Fórmula: max(0, Estoque_Final_Declarado - Saldo_Final_Calculado)`

### Valores e ICMS

* **pme (Preço Médio de Entrada)**: Média ponderada dos preços de compra no ano (exclui devoluções).
* **pms (Preço Médio de Saída)**: Média ponderada dos preços de venda no ano.
* **ICMS_saidas_desacob**: Estimativa de imposto devido sobre as vendas não declaradas.

  * `Fórmula: Alíquota * Saídas_Desacobertadas * PMS`
* **ICMS_est_final_desacob**: Estimativa de imposto sobre a falta de comprovação de entrada para o estoque final declarado.

  * `Fórmula: Alíquota * Estoque_Final_Desacoberto * PMS`

> [!NOTE]

> A alíquota de ICMS utlizada é de **17.5%** para anos até 2023 e **19.5%** a partir de 2024.
>
