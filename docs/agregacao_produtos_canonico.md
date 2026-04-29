# Agregação de Produtos

Este documento define a regra canônica de rastreabilidade, agrupamento e enriquecimento de produtos do projeto.

## Objetivo

Garantir que qualquer linha de NFe, NFCe, C170 ou Bloco H possa ser:

- agrupada em um produto mestre para análise;
- desagrupada ou auditada até sua origem exata;
- enriquecida sem perder a identidade fiscal da linha original.

## Princípio de rastreabilidade

O "fio de ouro" do projeto é:

```text
linha original -> id_linha_origem -> codigo_fonte -> id_agrupado -> tabelas analíticas
```

> **⚠ REGRA DE FOGO:** É terminantemente proibido realizar joins por `descricao_normalizada`
> (ou qualquer outra heurística textual) após a etapa 02 do pipeline. Todo enriquecimento
> posterior à geração de `descricao_produtos` deve usar exclusivamente `codigo_fonte`,
> `id_descricao` ou `id_agrupado` como chaves de vínculo.
> Joins por descrição são aceitos **apenas** como fallback explícito e logado quando a
> chave física não estiver disponível na entrada.

## Chaves centrais

### `id_linha_origem`

Chave física da linha original do documento.

### `codigo_fonte`

Identifica o produto antes do agrupamento:

```text
CNPJ_Emitente + "|" + codigo_produto_original
```

### `id_agrupado`

Chave mestra que representa o produto consolidado no pipeline analítico.

### `id_agrupado_base`

Chave automática e determinística gerada a partir de `descricao_normalizada`. Ela representa o agrupamento automático de base e é preservada para auditoria mesmo quando houver merge manual.

### `versao_agrupamento`

Inteiro sequencial incrementado a cada operação manual de merge.

## Estruturas principais

### Tabela mestre

Contém o registro consolidado do produto e elege atributos padronizados.

Campos adicionais relevantes após este patch:

- `criterio_agrupamento`
- `origem_agrupamento`
- `qtd_descricoes_grupo`

### Tabela ponte

A tabela ponte (`map_produto_agrupado_{cnpj}.parquet`) continua sendo a peça central da agregação e da desagregação.

Campos relevantes:

- `chave_produto`
- `id_agrupado`
- `codigo_fonte`
- `descricao_normalizada`

## Regra de normalização textual

A `descricao_normalizada` deve aplicar apenas estas transformações:

1. remover espaços no início e no fim;
2. reduzir espaços internos consecutivos para um único espaço;
3. remover acentos;
4. converter para maiúsculas.

Pontuação e demais caracteres textuais são preservados. Assim, `"PROD. A"` e `"PROD A"` continuam sendo descrições diferentes.

## Agregação automática

A agregação automática segue a sistemática abaixo:

1. **Critério**: produtos com a mesma `descricao_normalizada` recebem o mesmo `id_agrupado_base`.
2. **Determinismo**: o `id_agrupado_base` é gerado de forma determinística a partir da própria `descricao_normalizada`, reduzindo instabilidade entre reprocessamentos.
3. **Preservação semântica**: a identidade fiscal original do item permanece preservada nas tabelas de rastreabilidade.
4. **Associações não determinísticas**: o pipeline não deve resolver ambiguidades silenciosamente.

## Persistência de agrupamentos manuais

Para evitar perda de trabalho após reprocessamentos, o sistema utiliza o arquivo:

- `mapa_agrupamento_manual_<cnpj>.parquet`

O arquivo pode trazer:

- `id_descricao` + `id_agrupado`
- `descricao_normalizada` + `id_agrupado`
- ou ambos

A precedência é:

1. manual por `id_descricao`
2. manual por `descricao_normalizada`
3. automático por `descricao_normalizada`

## Colunas de descrição

As descrições principais e os complementos devem permanecer separados:

- `lista_descricoes`: descrições principais
- `lista_desc_compl`: complementos
- `lista_itens_agrupados`: descrições-base vigentes no grupo
- `ids_origem_agrupamento`: lista dos `id_agrupado_base` que deram origem ao grupo atual

## Auditoria de inconsistências

### Mapa manual sem correspondência

Entradas do `mapa_agrupamento_manual_<cnpj>.parquet` que não encontrarem correspondência na base atual devem ser exportadas para arquivo de auditoria específico e não podem ser aplicadas silenciosamente.

### Fontes sem `id_agrupado`

As rotinas de enriquecimento por fonte devem:

1. tentar `codigo_fonte`;
2. usar fallback por `descricao_normalizada` apenas quando a correspondência for unívoca;
3. exportar linhas sem correspondência para auditoria com motivo explícito.

Motivos mínimos esperados:

- `codigo_fonte_sem_mapeamento`
- `descricao_normalizada_ambigua`
- `descricao_normalizada_sem_match`
- `sem_codigo_fonte_sem_descricao`

## Regra de ouro

Nenhuma operação de agrupamento, desagrupamento, enriquecimento ou auditoria pode romper o encadeamento entre a linha original e o `id_agrupado` vigente.
