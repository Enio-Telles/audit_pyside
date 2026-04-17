# Agregação de Produtos

Este documento define a regra canônica de rastreabilidade, agrupamento e enriquecimento de produtos do projeto. Ele consolida as regras funcionais, operacionais e de auditoria em uma única referência.

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

Esse encadeamento é a base para auditoria de estoque, preço médio, ST, ICMS e demais análises até o registro original.

## Chaves centrais

### `id_linha_origem`

Chave física da linha original do documento.

Exemplos usuais:

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

### `versao_agrupamento`

Inteiro sequencial incrementado a cada operação manual de merge. Pode ser usado por tabelas derivadas para validação de consistência.

## Colunas de rastreabilidade

| Coluna               | Origem                                                                                                      | Preservada em                   |
|----------------------|-------------------------------------------------------------------------------------------------------------|---------------------------------|
| `id_linha_origem`    | Extração SQL (NFe: `chave_acesso|prod_nitem`, C170: `num_doc|num_item`, Bloco H: `num_inventario|dt_inv`) | `*_agr`, ponte                  |
| `codigo_fonte`       | Extração SQL (`CNPJ|codigo_produto`)                                                                        | `*_agr`, ponte, `item_unidades` |
| `id_agrupado`        | Regra de agrupamento                                                                                        | Todas as camadas analíticas     |
| `versao_agrupamento` | Controle transacional do agrupamento                                                                        | Tabela mestre                   |

## Estruturas principais

### Tabela mestre

Contém o registro consolidado do produto e elege atributos padronizados, como:

- `descr_padrao`
- `ncm_padrao`
- `cest_padrao`
- `co_sefin_padrao`
- `co_sefin_agr`

### Tabela ponte

A tabela ponte (`map_produto_agrupado_{cnpj}.parquet`) é a peça central da agregação e da desagregação. Ela relaciona cada `codigo_fonte` ao respectivo `id_agrupado` e preserva a capacidade de voltar da análise ao item bruto.

Campos relevantes:

- `chave_produto` — ID interno da descrição
- `id_agrupado` — grupo consolidado
- `codigo_fonte` — vínculo com a fonte bruta
- `descricao_normalizada` — chave de matching automático

## Agregação automática

A agregação automática segue uma sistemática simplificada:

1. **Critério de agrupamento automático**: produtos com a mesma `descricao_normalizada` (após remoção de acentos, conversão para maiúsculas e limpeza de espaços) são agrupados automaticamente no mesmo `id_agrupado`.
2. **Preservação da identidade fiscal**: os atributos `codigo`, `descricao`, `descricao_complementar`, `tipo_item`, `ncm`, `cest` e `gtin` permanecem preservados para rastreabilidade, auditoria e análise manual.
3. **Restrição de associação automática**: qualquer associação entre produtos com nomes diferentes, ainda que compartilhem GTIN, NCM ou outros identificadores fiscais, deve ser feita explicitamente de forma manual.

Em outras palavras: o agrupamento automático usa `descricao_normalizada` como chave de consolidação, enquanto a identidade fiscal continua registrada para evitar perda semântica do item original.

## Persistência de agrupamentos manuais

Para evitar perda de trabalho após reprocessamentos, o sistema utiliza o arquivo:

- `mapa_agrupamento_manual_<cnpj>.parquet`

Esse arquivo armazena os DE-PARA manuais realizados na interface. O pipeline prioriza esse mapeamento antes da aplicação da sistemática automática.

## Operações manuais

Quando a heurística automática não é suficiente, a interface permite intervenção manual.

### Merge manual

No merge manual:

- um grupo de destino é preservado como `id_agrupado` final da operação;
- os grupos de origem passam a apontar para esse `id_agrupado` de destino;
- o merge não renumera todos os grupos existentes;
- a operação incrementa `versao_agrupamento`.

### Unmerge / reversão

Na reversão:

- o sistema restaura os grupos de origem a partir do snapshot registrado no momento do merge;
- a restauração reaplica os vínculos originais da tabela ponte;
- a reversão utiliza o histórico do merge manual, preservando a rastreabilidade completa da operação.

### Log e trilha de auditoria

Cada agrupamento manual deve registrar em log:

- grupo de destino;
- grupos de origem;
- itens envolvidos;
- versão do agrupamento;
- snapshot necessário para reversão.

Essas operações devem preservar a rastreabilidade e nunca substituir a linha original.

## Colunas de descrição

Na camada de agregação, descrições principais e complementos devem permanecer semanticamente separadas.

- `lista_descricoes`: contém apenas descrições principais do produto ou grupo.
- `lista_desc_compl`: contém apenas descrições complementares provenientes de `descr_compl`.
- `lista_itens_agrupados`: mostra as descrições-base dos itens atualmente vinculados ao grupo.
- `ids_origem_agrupamento`: registra quais `id_agrupado` deram origem ao grupo atual.

### Regras semânticas

- Complementos não devem ser incorporados em `lista_descricoes`.
- Rotinas de reconciliação por descrição podem utilizar `lista_descricoes` e `lista_desc_compl`, mas os dois campos permanecem semanticamente distintos.

### Regra de interface e busca

- Os filtros textuais da aba `Agregacao` podem considerar simultaneamente `lista_descricoes` e `lista_desc_compl` para preservar rastreabilidade e facilitar localização, sem alterar a separação semântica entre os campos.

## Identificação do CO_SEFIN (SITAFE)

A identificação do `co_sefin_item` ocorre na etapa `item_unidades` e é baseada em match de códigos fiscais contra as tabelas oficiais do SITAFE, armazenadas em `dados/referencias/CO_SEFIN/`.

A ordem de precedência é:

1. **CEST + NCM**: match exato nos dois campos (`sitafe_cest_ncm.parquet`)
2. **Somente CEST**: match pelo código CEST (`sitafe_cest.parquet`)
3. **Somente NCM**: match pelo código NCM (`sitafe_ncm.parquet`)

Esse código orienta a determinação de carga tributária, alíquotas de ST e reduções de base de cálculo.

### Ciclo do CO_SEFIN no pipeline

1. A fonte fiscal gera o item bruto.
2. A etapa `item_unidades` identifica `co_sefin_item` conforme a precedência do SITAFE.
3. A camada de agregação consolida os atributos padronizados do grupo, incluindo `co_sefin_padrao` e `co_sefin_agr`.
4. As tabelas analíticas recebem os atributos fiscais consolidados via enriquecimento.

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

## Auditoria e consistência de `id_agrupado`

Para garantir rastreabilidade e integridade fiscal, todas as linhas das tabelas `c170`, `nfe`, `nfce` e `bloco_h` devem possuir o campo `id_agrupado` preenchido.

A rotina `src/transformacao/rastreabilidade_produtos/fontes_produtos.py` centraliza a geração dos arquivos:

- `c170_agr_<cnpj>.parquet`
- `bloco_h_agr_<cnpj>.parquet`
- `nfe_agr_<cnpj>.parquet`
- `nfce_agr_<cnpj>.parquet`

Durante a geração, qualquer linha sem `id_agrupado` deve ser:

1. exportada para um arquivo de auditoria, por exemplo `c170_agr_sem_id_agrupado_<cnpj>.parquet`;
2. excluída da saída final principal;
3. registrada em log com aviso e caminho do arquivo de auditoria.

Todos os módulos do pipeline que consomem essas tabelas devem utilizar exclusivamente os arquivos `*_agr_<cnpj>.parquet`, garantindo que apenas linhas válidas, com `id_agrupado`, sejam processadas.

> Caso existam linhas sem `id_agrupado`, o pipeline não precisa falhar. Ele deve gerar o arquivo de auditoria, excluir essas linhas da saída principal e permitir análise posterior.

## Relação com as tabelas analíticas

A `mov_estoque` é a principal camada de enriquecimento operacional. Ela recebe:

- `id_agrupado`
- atributos padronizados do produto
- parâmetros fiscais da SEFIN
- fatores de conversão

A partir dela derivam as tabelas mensal e anual.

## API de agregação

| Método | Rota                                       | Ação                                   |
|--------|--------------------------------------------|----------------------------------------|
| GET    | `/aggregation/{cnpj}/tabela_agrupada`      | Lista grupos paginados                 |
| POST   | `/aggregation/merge`                       | Merge manual de grupos                 |
| POST   | `/aggregation/unmerge`                     | Reverte último merge de um grupo       |
| GET    | `/aggregation/{cnpj}/historico_agregacoes` | Histórico completo de merges/reversões |

## Glossário mínimo

| Termo canônico            | Alias / uso relacionado        | Significado |
|---------------------------|--------------------------------|-------------|
| `descricao_complementar`  | `descr_compl`                  | Descrição complementar no nível do item |
| `lista_desc_compl`        | agregação de `descr_compl`     | Lista consolidada de complementos no nível do grupo |
| `co_sefin_item`           | —                              | Código SEFIN identificado no nível do item em `item_unidades` |
| `co_sefin_padrao`         | —                              | Código SEFIN padronizado no nível do grupo mestre |
| `co_sefin_agr`            | —                              | Atributo fiscal consolidado associado ao agrupamento |
| reversão                  | `unmerge`, desagregação manual | Restauração dos vínculos anteriores a partir do snapshot do merge |

## Regra de ouro

Nenhuma operação de agrupamento, desagrupamento, enriquecimento ou auditoria pode romper o encadeamento entre a linha original e o `id_agrupado` vigente.
