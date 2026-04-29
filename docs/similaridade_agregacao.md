# Estratégia de similaridade na aba Agregação

Este documento descreve a estratégia usada para ordenar visualmente itens semelhantes na aba **Agregação**.

A funcionalidade **não executa agrupamento automático**. Ela apenas reorganiza a tabela para colocar itens candidatos próximos uns dos outros, facilitando a revisão humana e a seleção manual das linhas antes do clique em **Agregar Descricoes**.

---

## Objetivo

A ordenação por similaridade busca responder:

```text
Quais linhas parecem boas candidatas para serem analisadas juntas?
```

Ela não tenta responder automaticamente:

```text
Quais linhas devem ser agrupadas definitivamente?
```

A decisão final continua manual para evitar agrupamentos fiscais incorretos.

---

## Resumo da abordagem

A estratégia atual usa:

```text
normalização de descrição
+ extração de sinais fiscais/comerciais
+ geração de candidatos por múltiplas chaves
+ cálculo de score composto
+ formação de blocos por grafo/union-find
+ ordenação interna por proximidade
```

Essa abordagem substitui a lógica mais simples de comparar apenas vizinhos depois de uma ordenação inicial. A vantagem é que descrições parecidas não dependem mais de ficarem próximas por acaso na ordenação alfabética.

---

## 1. Normalização da descrição

Antes de comparar, a descrição é normalizada pela função canônica `normalize_desc()` e, em DataFrames Polars, por `expr_normalizar_descricao()`.

Regras:

```text
- converter para maiúsculas;
- remover acentos;
- reduzir espaços excedentes;
- preservar letras, números, espaços e a pontuação: -%$#@!.,}{][/\;
- substituir outros caracteres por espaço;
- não remover stop words.
```

Exemplo:

```text
Entrada:
"  Água   mineral 500ml - %$#@!.,}{][/\;  "

Saída:
"AGUA MINERAL 500ML - %$#@!.,}{][/\;"
```

As stop words não são removidas em descrição fiscal porque podem ser relevantes:

```text
OLEO DE SOJA
LEITE EM PO
PANO DE CHAO
```

---

## 2. Campos considerados

A similaridade usa os seguintes sinais:

```text
Descrição normalizada
Tokens fortes da descrição
Números presentes na descrição
NCM
CEST
GTIN
```

A função tenta localizar colunas por aliases. Exemplos:

```text
Descrição:
descr_padrao, descricao_normalizada, descricao, descricao_final, lista_descricoes, lista_itens_agrupados

NCM:
ncm_padrao, NCM_padrao, lista_ncm, ncm_final, ncm

CEST:
cest_padrao, CEST_padrao, lista_cest, cest_final, cest

GTIN:
gtin_padrao, GTIN_padrao, lista_gtin, gtin, cod_barra, cod_barras
```

---

## 3. Geração de candidatos

A implementação evita comparar todas as linhas contra todas as outras, porque isso teria custo alto em bases grandes.

Em vez disso, gera pares candidatos por múltiplas chaves.

Exemplos de chaves:

```text
GTIN igual
NCM completo + token forte
NCM com 4 primeiros dígitos + token forte
CEST + token forte
duas palavras fortes em comum
número + palavra forte
```

Exemplo de descrição:

```text
CERVEJA HEINEKEN LATA 350ML
```

Pode gerar chaves como:

```text
GTIN:789000000001
NCM:22030000|T:CERVEJA
NCM4:2203|T:HEINEKEN
CEST:0302100|T:CERVEJA
TOK2:CERVEJA|HEINEKEN
NUM:350|T:HEINEKEN
```

Isso permite encontrar descrições parecidas mesmo quando a ordem das palavras muda:

```text
CERVEJA HEINEKEN LATA 350ML
HEINEKEN CERVEJA 350 ML LT
CERV HEINEKEN 350ML LATA
```

---

## 4. Fallback por vizinhança

Além dos candidatos por chaves, ainda existe um fallback simples por vizinhança textual.

A descrição é ordenada por uma chave textual leve, e cada linha compara com algumas linhas próximas.

Esse fallback ajuda em casos nos quais duas descrições não compartilham chaves fortes suficientes, mas ainda ficaram próximas por texto.

---

## 5. Score textual da descrição

O score da descrição combina dois critérios:

```text
similaridade por trigramas de caracteres
+ similaridade por tokens fortes
```

### Trigramas

Trigramas são pedaços de 3 caracteres.

Exemplo:

```text
CERVEJA
```

vira:

```text
CER
ERV
RVE
VEJ
EJA
```

Isso ajuda a comparar abreviações e pequenas variações.

### Tokens fortes

Tokens fortes são palavras relevantes da descrição, ignorando termos muito genéricos para chaveamento, como:

```text
DE, DA, DO, COM, PARA, EM, UN, UND
```

Eles não são removidos da descrição normalizada. Apenas deixam de ser usados como palavra forte na criação de chaves.

---

## 6. Score de números

Quando duas descrições compartilham números, isso reforça a similaridade.

Exemplo:

```text
CERVEJA HEINEKEN LATA 350ML
HEINEKEN CERVEJA 350 ML LT
```

Ambas têm `350`, então:

```text
sim_score_numeros = 100
```

Se uma das descrições não tiver número, o campo fica `null` e não entra no cálculo composto.

---

## 7. Score de NCM

O NCM tem três níveis:

```text
100  = NCM completo igual
70   = NCM diferente, mas os 4 primeiros dígitos são iguais
0    = NCM diferente e sem coincidência nos 4 primeiros dígitos
null = falta NCM em uma das linhas
```

Exemplo:

```text
19053100
19059090
```

Os códigos completos são diferentes, mas ambos começam com `1905`.

Resultado:

```text
sim_score_ncm = 70
```

Isso ajuda a aproximar produtos da mesma família fiscal sem tratar como equivalência total.

---

## 8. Score de CEST

O CEST é avaliado de forma direta:

```text
100  = CEST igual
0    = CEST diferente
null = falta CEST em uma das linhas
```

---

## 9. Score de GTIN

O GTIN também é avaliado de forma direta:

```text
100  = GTIN igual
0    = GTIN diferente
null = falta GTIN em uma das linhas
```

GTIN igual é um sinal forte. Por isso, quando o GTIN coincide e a descrição tem relação mínima, o score final é elevado para tratar o par como candidato forte.

---

## 10. Score composto

O `sim_score` final combina os sinais disponíveis.

Pesos usados:

```text
Descrição textual: 60%
Números:           15%
NCM:               10%
CEST:               5%
GTIN:              10%
```

Campos ausentes não entram no cálculo. Por exemplo, se não houver CEST nem GTIN, o score é calculado apenas com os componentes disponíveis.

---

## 11. Formação de blocos por grafo

Após calcular os pares candidatos, a implementação cria um grafo:

```text
linha A parecida com linha B
linha B parecida com linha C
linha D parecida com linha E
```

Cada ligação acima do limite mínimo cria uma conexão.

A estrutura `union-find` une as linhas conectadas em blocos visuais.

Exemplo:

```text
A parecido com B
B parecido com C
A não tão parecido diretamente com C
```

Mesmo assim, A, B e C podem ficar no mesmo bloco porque há uma ponte de similaridade entre eles.

Isso melhora a ordenação em relação ao método antigo, que dependia muito do vizinho imediato.

---

## 12. Ordenação interna do bloco

Depois que os blocos são formados, as linhas dentro de cada bloco são ordenadas por proximidade.

A lógica é:

```text
1. escolher uma linha representativa do bloco;
2. colocar depois dela a linha mais próxima;
3. repetir com a próxima linha mais parecida;
4. continuar até acabar o bloco.
```

Isso coloca descrições parecidas uma abaixo da outra.

---

## 13. Colunas geradas

A ordenação adiciona colunas auxiliares à tabela:

```text
sim_bloco
sim_score
sim_score_desc
sim_score_tokens
sim_score_numeros
sim_score_ncm
sim_score_cest
sim_score_gtin
sim_nivel
sim_motivos
sim_desc_norm
sim_chave_ordem
sim_desc_referencia
```

### `sim_bloco`

Número do bloco visual de similaridade.

Linhas no mesmo bloco são candidatas para revisão conjunta, mas não são agrupadas automaticamente.

### `sim_score`

Score final composto.

### `sim_score_desc`

Score textual da descrição, combinando trigramas e tokens fortes.

### `sim_score_tokens`

Similaridade por palavras fortes em comum.

### `sim_score_numeros`

Indica se há números relevantes em comum.

### `sim_score_ncm`

Similaridade fiscal por NCM completo ou pelos 4 primeiros dígitos.

### `sim_score_cest`

Similaridade por CEST.

### `sim_score_gtin`

Similaridade por GTIN.

### `sim_nivel`

Classificação textual do score:

```text
EXATO
MUITO PARECIDO
PARECIDO
FRACO
```

### `sim_motivos`

Explica por que as linhas foram aproximadas.

Exemplos:

```text
DESC_ALTA; TOKENS; NUMEROS_IGUAIS; NCM4_IGUAL
GTIN_IGUAL; DESC_MEDIA
NCM_IGUAL; CEST_IGUAL
```

### `sim_desc_referencia`

Mostra a descrição normalizada da linha mais parecida encontrada para aquela linha.

---

## 14. Checkbox "Priorizar NCM/CEST"

Quando marcado, NCM e CEST ajudam também na geração de candidatos.

Quando desmarcado, a comparação continua usando descrição, tokens, números e GTIN, mas NCM/CEST deixam de guiar a aproximação inicial.

Mesmo desmarcado, NCM/CEST ainda podem aparecer como informação de score quando o par candidato for encontrado por outros sinais.

---

## 15. Segurança fiscal

Esta funcionalidade não altera dados fiscais por conta própria.

Ela não deve:

```text
salvar Parquet automaticamente;
chamar agregar_linhas();
alterar id_agrupado;
remover linhas;
executar agrupamento automático.
```

O fluxo correto é:

```text
1. abrir tabela agrupada;
2. clicar em Ordenar por similaridade;
3. revisar os blocos visuais;
4. marcar manualmente as linhas corretas;
5. clicar em Agregar Descricoes.
```

---

## 16. Testes

Rodar os testes específicos:

```bash
uv run pytest tests/test_descricao_similarity_service.py -q
uv run pytest tests/test_text_normalizacao_descricao.py -q
```

Rodar a suíte completa:

```bash
uv run pytest -q
```

Teste manual:

```bash
uv run python app.py
```

Depois:

```text
1. selecionar um CNPJ;
2. abrir a aba Agregação;
3. clicar em Abrir tabela agrupada;
4. clicar em Ordenar por similaridade;
5. verificar os blocos e as colunas sim_*;
6. confirmar que nenhum agrupamento automático ocorreu.
```

---

## 17. Limitações conhecidas

A estratégia evita comparação completa `n x n`, mas ainda depende da qualidade das chaves candidatas.

Casos muito abreviados, marcas ausentes ou descrições extremamente genéricas podem continuar difíceis.

Exemplos de descrições difíceis:

```text
PRODUTO DIVERSO
ITEM SORTIDO
MERCADORIA VARIADA
UNIDADE 1
```

Nesses casos, a revisão humana continua essencial.

---

## 18. Possíveis melhorias futuras

Ideias futuras:

```text
adicionar seletor Conservador / Equilibrado / Amplo;
permitir ajuste do limite mínimo de bloco;
destacar visualmente sim_motivos fortes;
tratar unidades e medidas com regras específicas;
limitar tamanho máximo de bloco visual;
gerar relatório de candidatos de agrupamento sem aplicar alterações.
```
