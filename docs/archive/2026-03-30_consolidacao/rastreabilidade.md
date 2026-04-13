# Rastreabilidade e Agregação (Fio de Ouro)

Este documento descreve como garantimos a integridade do dado desde o XML original até o relatório final.

## 1. O Conceito de "Fio de Ouro" (Golden Thread)

Nenhuma transformação no sistema é destrutiva. Qualquer métrica agregada pode ser auditada de volta ao seu registro original irrefutável (XML ou TXT).

## 2. Chave de Unicidade (`codigo_fonte`)

Para evitar fusão acidental de produtos, criamos o identicador absoluto de catálogo:
- **Fórmula:** `CNPJ_Emitente + '|' + Codigo_Produto_Local`
- Este identificador é preservado em todas as tabelas derivadas para garantir a volta à origem.

## 3. Master Data Management (MDM) e Agrupamento

O sistema agrupa produtos automaticamente sob um **`id_agrupado`** baseado em:
1.  **Regra de GTIN/EAN:** Match exato pelo código de barras universal.
2.  **Regra de Semântica:** Match de `descricao_normalizada` + interseção de `NCM`.

### Estrutura Relacional
- **Tabela Mestre (`produtos_agrupados.parquet`):** Contém os metadados eleitos (eleição frequencista).
- **Tabela Ponte (`map_produto_agrupado.parquet`):** Relaciona cada `chave_produto` (bruta) ao seu `id_agrupado` (mestre).

## 4. Intervenção Manual (Agregação/Desagregação)

O sistema permite que o usuário corrija agrupamentos via interface gráfica:
- **Agregação Manual:** Une múltiplos grupos mestre em um novo ID, remapeando as chaves brutas na Tabela Ponte.
- **Desagregação:** Isola componentes de um grupo, restaurando a autonomia original.

## 5. Enriquecimento de Fontes

A integração de dados ocorre via **Left Joins** dinâmicos:
1. A fonte bruta (ex: NFCe) é lida.
2. É cruzada com a **Tabela Ponte** para obter o `id_agrupado`.
3. É cruzada com a **Tabela Mestre** para injetar as descrições padronizadas e parâmetros fiscais (NCM, CEST, SEFIN).

Desta forma, a nota fiscal original permanece intacta em nosso banco de dados, mas "espetada" com toda a inteligência analítica do sistema.
