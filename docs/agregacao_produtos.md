# Agregação de Produtos

Este documento consolida a regra de rastreabilidade e agrupamento de produtos do projeto, baseada no conceito de “fio de ouro” entre a linha original extraída e as tabelas analíticas.

## Objetivo

Garantir que qualquer linha de NFe, NFCe, C170 ou Bloco H possa ser:

- agrupada em um produto mestre para análise;
- desagrupada ou auditada até sua origem exata;
- enriquecida sem perder a identidade da linha original.

## Chaves centrais

### `id_linha_origem`

Chave física da linha original do documento. Exemplos usuais:

- NFe e NFCe: `chave_acesso + prod_nitem`
- C170: `reg_0000_id + num_doc + num_item`
- Bloco H: chave física do inventário da extração

### `codigo_fonte`

Identifica o produto antes do agrupamento:

```text
CNPJ_Emitente + "|" + codigo_produto_original
```

Essa chave evita misturar produtos de emissores diferentes antes da etapa de MDM.

### `id_agrupado`

Chave mestra que representa o produto consolidado no pipeline analítico.

## Golden Thread

O “fio de ouro” do projeto é:

```text
linha original -> id_linha_origem -> codigo_fonte -> id_agrupado -> tabelas analíticas
```

Esse encadeamento é o que permite auditar totais de estoque, preço médio, ST e ICMS até o registro original.

## Agregação automática

O agrupamento automático considera principalmente duas trilhas:

1. GTIN comum entre produtos.
2. `descricao_normalizada` igual com interseção de NCM.

Fallback tolerado:

- se ambos não tiverem NCM, descrições idênticas ainda podem formar grupo.

Regras de cuidado:

- `cest` não é equivalente a `gtin`;
- código de barras não deve ser tratado como classificação fiscal.

## Tabela mestre e tabela ponte

O modelo usa duas estruturas complementares.

Tabela mestre:

- contém o registro consolidado do produto;
- elege atributos como `descr_padrao`, `ncm_padrao`, `cest_padrao`, `co_sefin_padrao` e `co_sefin_agr`.

Tabela ponte:

- relaciona cada `codigo_fonte` ao respectivo `id_agrupado`;
- preserva a capacidade de voltar da análise ao item bruto.

Na prática, a tabela ponte é a peça central da agregação e da desagregação.

## Agregação e desagregação manual

Quando a heurística automática não é suficiente, a interface permite intervenção manual.

Agregação manual:

- vários grupos mestre são fundidos em um novo `id_agrupado`;
- os vínculos da tabela ponte passam a apontar para o novo grupo.

Desagregação:

- o grupo consolidado é particionado;
- a tabela ponte restaura a associação autônoma dos itens de origem.

Essas operações devem preservar a rastreabilidade, nunca substituir a linha original.

## Enriquecimento das fontes

As fontes fiscais são enriquecidas por `LEFT JOIN`:

1. a linha original entra com `codigo_fonte`;
2. a tabela ponte injeta `id_agrupado`;
3. a tabela mestre injeta atributos padronizados;
4. a tabela de fatores injeta `unid_ref` e `fator`.

Com isso, as fontes enriquecidas mantêm simultaneamente:

- a identidade fiscal original;
- a classificação analítica do produto;
- a unidade padronizada para cálculos posteriores.

## Relação com as tabelas analíticas

A `mov_estoque` é a principal camada de enriquecimento operacional. Ela recebe:

- `id_agrupado`
- atributos padronizados do produto
- parâmetros fiscais da SEFIN
- fatores de conversão

É a partir dela que nascem a tabela mensal e a tabela anual.
