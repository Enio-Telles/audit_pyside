# Agregação de produtos
<a id="mds-02-agregacao-produtos"></a>

Este documento define a regra canônica de rastreabilidade, agrupamento e enriquecimento de produtos consumindo as buscas SQL (Bloco H, C170, NF‑e, NFC‑e). As nomenclaturas foram atualizadas para reforçar a ligação com os dados de origem e eliminar ambiguidades identificadas na versão anterior.

## Objetivo

Agrupar itens fiscalmente idênticos (mesmo NCM/CEST, mesma unidade de medida e mesmo código de origem) em um **produto agregado** para fins de auditoria e cálculo de estoque, mantendo a possibilidade de desagregação até a linha original. O agrupamento deve ser determinístico, auditável e coerente com as regras de classificação fiscal.

## Cadeia de rastreabilidade

O pipeline garante que qualquer linha original possa ser rastreada até o produto agregado e vice‑versa. A cadeia de chaves recomendada é:

```text
linha original (SQL) -> id_linha_origem -> id_produto_origem -> id_produto_agrupado_base -> id_produto_agrupado -> tabelas analíticas
```

### Chaves principais

| Campo                   | Descrição |
|------------------------|-----------|
| `id_linha_origem`      | Identificador físico da linha na fonte (ID da tabela no banco de origem: C170, NF‑e, NFC‑e ou Bloco H). |
| `id_produto_origem`    | Chave de produto antes do agrupamento. Recomenda‑se formar esta chave concatenando o CNPJ do emitente com o código do item da origem, conforme implementado nas buscas SQL (`co_emitente || '|' || cod_item` ou `co_emitente || '|' || prod_cprod`). |
| `id_produto_agrupado_base` | Chave gerada automaticamente e de forma determinística a partir da descrição normalizada (ver seção de agregação automática). Preservada para auditoria. |
| `id_produto_agrupado`  | Chave mestra que representa o produto consolidado no pipeline. Pode ser alterada manualmente via mapa de agregação. |
| `versao_agrupamento`   | Inteiro sequencial incrementado sempre que há alteração manual nos grupos. |

### Recomendações de nomenclatura

- Substitua `codigo_fonte` por **`id_produto_origem`** para tornar explícita a ligação com a linha de origem.
- Substitua `id_agrupado` por **`id_produto_agrupado`** e `id_agrupado_base` por **`id_produto_agrupado_base`**.
- Não utilize `descricao_normalizada` ou outras heurísticas textuais como chave após a geração de `id_produto_agrupado_base`.

## Critérios de agrupamento

1. **NCM e CEST coincidentes**: somente itens que compartilham o mesmo NCM (8 dígitos) e o mesmo CEST podem pertencer ao mesmo grupo. Divergências nesses códigos indicam tratamentos tributários distintos.
2. **Unidade de medida compatível**: itens com unidade de medida diferente (p.ex. kg vs. litro) somente podem ser agrupados se existir fator de conversão físico claro e unívoco. Caso contrário, o agrupamento deve ser impedido ou demandar intervenção manual.
3. **Descrição normalizada**: usada apenas para gerar `id_produto_agrupado_base` de forma determinística (passo automático). Após a geração, não deve ser usada para joinear ou enriquecer dados.
4. **Agregação automática**: items com a mesma `descricao_normalizada`, NCM/CEST e unidade compatível recebem o mesmo `id_produto_agrupado_base`. Esse identificador é independente de CNPJ e serve como agrupamento inicial.
5. **Agregação manual**: discrepâncias ou agrupamentos específicos podem ser corrigidos via `mapa_agrupamento_manual_<cnpj>.parquet`. As regras de precedência são:
   1. manual por `id_linha_origem`;
   2. manual por `descricao_normalizada` (quando unívoca);
   3. automático por `id_produto_agrupado_base`.

## Estruturas principais

### Tabela mestre de produtos agregados

Esta tabela consolida atributos padrão de cada `id_produto_agrupado`. Deve incluir, no mínimo:

* `id_produto_agrupado`
* `descricao_padrao` (string padronizada do grupo)
* `ncm` e `cest`
* `unidade_referencia` (sugerida pela conversão de unidades)
* `criterio_agrupamento` (automático vs. manual)
* `origem_agrupamento` (base, manual por id, manual por descrição)
* `qtd_descricoes_grupo` (número de descrições distintas agrupadas)

### Tabela ponte de mapeamento

O arquivo `map_produto_agrupado_<cnpj>.parquet` vincula cada `id_produto_origem` ao `id_produto_agrupado`. A tabela deve conter:

* `id_produto_origem`
* `id_produto_agrupado`
* `id_produto_agrupado_base`
* `descricao_normalizada`

Entradas que não encontrarem correspondência devem ser exportadas para auditoria com motivo explícito (`id_produto_origem_sem_mapeamento`, `descricao_normalizada_ambigua`, etc.).

## Auditoria e integridade

Para garantir a integridade:

* **Jamais quebre o vínculo** entre a linha original (`id_linha_origem`) e o `id_produto_agrupado`. Todos os enriquecimentos devem percorrer a cadeia descrita no início.
* **Nunca resolva ambiguidades silenciosamente**. Se um `id_produto_origem` corresponder a múltiplas descrições ou vice‑versa, registre o caso para auditoria e exija intervenção manual.
* **Documente as justificativas** de agrupamentos manuais. O campo `versao_agrupamento` existe justamente para rastrear alterações e permitir replicabilidade.

## Conexão com as buscas SQL

As buscas SQL (C170, NF‑e, NFC‑e e Bloco H) devem preencher os campos necessários para o agrupamento:

* A coluna **`id_produto_origem`** deve ser criada na consulta SQL concatenando o CNPJ do emitente (ou do participante de inventário) ao código do produto da linha. É a chave física do item.
* O **NCM, CEST, unidade de medida e descrição do produto** devem ser extraídos diretamente dos registros 0200 (Sped) ou do bloco de produto das NF‑e/NFC‑e.
* Em linhas de inventário, o campo `quantidade` e a unidade de medida devem ser preservados para permitir cálculo de estoque final.

Ao cumprir estes requisitos e seguir as nomenclaturas propostas, o processo de agregação torna‑se determinístico, auditável e alinhado à legislação fiscal.