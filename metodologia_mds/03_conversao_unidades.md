# Conversão de unidades

Este documento consolida as regras de normalização de unidades de medida e cálculo do **fator de conversão** para os produtos agrupados. As nomenclaturas foram atualizadas para esclarecer a finalidade de cada campo e para alinhar o processo às boas práticas de auditoria e à legislação vigente.

## Objetivo

Garantir que todas as quantidades e valores de um mesmo produto agregado sejam expressos em uma **unidade de referência** (`unidade_referencia`) comum, preservando fatores de conversão manuais e evitando distorções provocadas por embalagens ou unidades comerciais diferentes.

## Fontes de entrada

O cálculo baseia‑se principalmente nos Parquets:

* `item_unidades_<cnpj>.parquet` – contêm as quantidades originais (`quantidade_convertida`) e a unidade comercial/tributável de cada item;
* `produtos_final_<cnpj>.parquet` – catálogo de produtos com códigos, NCM e unidades cadastradas;
* `descricao_produtos_<cnpj>.parquet` – lista de descrições e unidades utilizadas para cada produto;
* `map_produto_agrupado_<cnpj>.parquet` – mapeamento entre `id_produto_origem` e `id_produto_agrupado`.

O vínculo preferencial é `descricao_produtos` → `map_produto_agrupado`. Somente quando a correspondência for impossível utiliza‑se `produtos_final` como fallback.

## Escolha da unidade de referência

Definida por prioridade:

1. **`unidade_referencia_override`** – unidade definida manualmente pelo auditor para o grupo.
2. **`unidade_referencia_sugerida`** – unidade recomendada pela camada de agrupamento (por exemplo, a unidade predominante nos dados).
3. **`unidade_referencia_auto`** – unidade escolhida automaticamente com base no maior volume movimentado, respeitando a coerência física (p.ex. litros vs. mililitros).

O campo final **`unidade_referencia`** recebe a primeira opção não nula nessa ordem.

## Cálculo do fator de conversão

O fator de conversão expressa quantas unidades originais correspondem a uma unidade de referência. Ao contrário da versão anterior, **o fator deve ser derivado de uma equivalência física** e não de comparações de preço médio. Use os seguintes critérios:

1. **Equivalência unitária declarada**: quando um mesmo produto é comercializado em diferentes embalagens (ex.: caixa com 12 unidades, pacote de 500 g), derive o fator comparando as quantidades físicas. Se uma nota informa que 2 pacotes de 500 g somam 1 kg, então `fator_conversao` para o pacote de 500 g em relação ao kg é `0.5`.
2. **Informação do fabricante**: utilize especificações técnicas do produto (ficha técnica) ou informações de catálogo para determinar quantidades equivalentes.
3. **Override manual** (`fator_conversao_override`): o auditor pode inserir o valor correto quando a equivalência física não for clara ou quando houver divergência nos documentos.
4. **Fallback por preço médio**: somente utilize uma relação baseada em preço médio (`preco_medio_base / preco_unidade_referencia`) quando as opções acima não forem possíveis e desde que haja evidências de proporcionalidade (ex.: preços proporcionais ao volume). Essa origem deve ser registrada no campo `fator_conversao_origem` como `preco`.

Se nenhuma informação estiver disponível, use `1.0` como último fallback e registre `fator_conversao_origem = 'fallback_sem_dados'`.

### Campos de saída

| Campo                         | Descrição |
|------------------------------|-----------|
| `unidade_referencia`         | Unidade de medida final adotada para o produto agregado. |
| `fator_conversao`            | Quantidade original multiplicada por este fator resulta na quantidade na unidade de referência. |
| `unidade_referencia_override`| Unidade definida manualmente (não nula em caso de override). |
| `fator_conversao_override`   | Fator de conversão definido manualmente. |
| `fator_conversao_origem`     | Origem do fator: `manual`, `fisico`, `preco`, `fallback_sem_dados`. |

### Exemplo de cálculo físico

Suponha que o produto “Óleo de Motor” seja vendido em frascos de 1 litro e em frascos de 500 ml. Se o grupo eleger a **unidade de referência** `litro` e a equivalência física declarar que 500 ml = 0,5 litro, então:

* Para linhas com `unidade_medida = '500 ML'`, `fator_conversao = 0.5` e `fator_conversao_origem = 'fisico'`.
* Para linhas com `unidade_medida = '1 L'`, `fator_conversao = 1.0`.

O preço médio não entra no cálculo do fator; ele será utilizado apenas nas etapas de apuração de ICMS.

## Reconciliação e reprocessamento

Em reprocessamentos:

* **Preserve overrides manuais**: os campos `unidade_referencia_override` e `fator_conversao_override` nunca devem ser sobrescritos automaticamente.
* **Verifique correspondência única**: antes de migrar fatores entre versões de agrupamento, confirme que o `id_produto_agrupado` é o mesmo e que a unidade permanece consistente. Caso contrário, descarte o fator e registre log de auditoria.
* **Registre a origem**: sempre preencha `fator_conversao_origem` para justificar o valor aplicado.

## Utilização do fator

O fator de conversão é consumido em diversas etapas do pipeline, notadamente:

* **movimentacao_estoque**: cálculo de `quantidade_convertida` a partir de `quantidade_original * fator_conversao`.
* **c170_xml** e **c176_xml**: normalização de quantidades dos itens de SPED.
* **agregações mensais e anuais**: harmonização de unidades para médias de preço e cálculo de divergências.

Ao seguir estas diretrizes, a conversão de unidades torna‑se coerente com a equivalência física declarada e evita distorções decorrentes de variações de preço.