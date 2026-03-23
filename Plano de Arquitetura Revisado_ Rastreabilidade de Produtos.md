# **Plano de Arquitetura Revisado: Processamento de Dados Fiscais com Rastreabilidade Total**

> **Nota Oficial de Documentação:**  
> A documentação detalhada sobre a teoria, regras de heurística e os métodos exatos de como produtos são associados (agregação automática, intervenção manual e JOIN dinâmico de chaves nas fontes) foi consolidada no diretório oficial de documentação:
> 👉 **[Consulte a Metodologia Completa: `docs/metodo_agregacao_rastreabilidade.md`](docs/metodo_agregacao_rastreabilidade.md)**

Este documento aprimora a arquitetura original, introduzindo o conceito de **Chaves Surrogadas (Surrogate Keys)** e **Tabelas de Ligação (Bridge Tables)** para garantir que qualquer cálculo de fator de conversão ou agrupamento manual possa ser rastreado e aplicado diretamente às linhas individuais das fontes originais (NFe, NFCe, C170, Bloco\_H) armazenadas em Parquet.

## **O Conceito da "Linha Dourada" (Golden Thread)**

Para rastrear o dado, não podemos alterar as tabelas originais, nem perder o nível de granularidade da linha do documento fiscal.

A solução é:

1. Criar uma **Chave Única de Linha (id\_linha\_origem)** na extração dos dados (SQL).  
2. Propagar essa chave (ou a chave do produto original \+ CNPJ) até o Módulo de Agrupamento.  
3. Criar um **Módulo 5 (Camada Enriquecida)** que faz o JOIN das tabelas originais com os de/para gerados.

## **0\. Mapeamento de Colunas e Chaves de Origem (Pré-Requisito)**

Antes do Módulo 1, ao gerar os arquivos Parquet via SQL, você **deve** garantir chaves compostas inquebráveis.

* **NFe / NFCe:** A chave única da linha é chave\_acesso \+ prod\_nitem.  
* **Bloco H:** A chave única é reg\_h010\_id (como já está no seu SQL).  
* **C170 (EFD):** A chave única é reg\_0000\_id \+ num\_doc \+ num\_item (ou o ID sequencial da tabela do banco).

**Nova Chave de Produto Fonte (codigo\_fonte):**

Para não misturar o código "001" do Fornecedor A com o código "001" da sua própria empresa, a chave base de qualquer produto antes do agrupamento deve ser:

CNPJ\_Emitente \+ | \+ codigo\_produto\_original.

## **1\. Módulo: produtos\_unidades.py (Ajustado)**

**Objetivo:** Gerar a tabela base de movimentações por unidade, mantendo a linhagem.

* **Campos:** cnpj\_emitente, codigo\_produto\_original, codigo\_fonte *(novo)*, descricao, tipo\_item, ncm, cest, gtin, unid, compras, vendas.  
* **Lógica de Agrupamento:** Em vez de agregar apenas por descrição, agregue por codigo\_fonte (CNPJ \+ Código).  
  * *Por que?* Porque um fornecedor não muda a unidade de medida do mesmo código de produto de forma aleatória. Isso garante que a relação Código Original \<-\> Unidade seja precisa.  
* *Restante da lógica (Cálculo de preço, identificação C170/NFe) permanece igual.*

## **2\. Módulo: produtos.py (Ajustado para Rastreabilidade)**

**Objetivo:** Gerar a dimensão de produtos únicos reportados pelas fontes.

* **Identificador:** chave\_produto (Gerado a partir do codigo\_fonte único).  
* **Regra de Normalização:** A descricao\_normalizada serve **apenas para sugerir agrupamentos**, não para agrupar os dados fisicamente nesta etapa. Se você agrupar por descrição aqui, perderá a rastreabilidade se duas coisas diferentes tiverem a mesma descrição normalizada.  
* **Saída:** Uma tabela contendo uma linha para cada codigo\_fonte distinto, com suas listas de atributos (NCM, CEST, Unidades) que vieram atreladas a esse código específico.

## **3\. Módulo: produtos\_agrupados.py (Tabela de De/Para)**

**Objetivo:** Onde a mágica do MDM (Master Data Management) acontece. Permite a união de múltiplos chave\_produto (do módulo 2\) em um único id\_agrupado.

* **Função:** O sistema tenta agrupar automaticamente (baseado em GTIN igual ou descricao\_normalizada igual \+ NCM igual). O usuário revisa e consolida.  
* **Campos da Tabela produtos\_agrupados (O Produto Mestre):**  
  * id\_agrupado (SK gerada, ex: PROD\_MSTR\_001)  
  * descr\_padrao, ncm\_padrao, cest\_padrao, gtin\_padrao (Lógica de atributos padrão baseada na moda estatística, como no seu plano original).  
* **A Tabela Crucial (A Ponte): map\_produto\_agrupado**  
  * Esta tabela relaciona N chave\_produto (Módulo 2\) para 1 id\_agrupado (Módulo 3).  
  * **Campos:** chave\_produto (CNPJ+Codigo), id\_agrupado.  
  * *É esta tabela que permitirá voltar para as NFs e Blocos H.*

## **4\. Módulo: fatores\_conversao.py**

**Objetivo:** Calcular relações entre unidades dentro do mesmo id\_agrupado.

* **Entrada:** Join da tabela base de movimentações (Módulo 1\) com o id\_agrupado (via tabela ponte do Módulo 3).  
* **Campos:** id\_agrupado, unid\_origem, unid\_ref (padrão), fator.  
* **Cálculo:** Permanece exatamente como planejado (Preço Médio de Compra da Unidade Origem / Preço Médio de Compra da Unid Ref).  
* **Log:** Importante manter o log de produtos sem preço de compra, forçando a digitação manual do fator (ex: Fator \= 12 para Caixa \-\> Unidade).

## **5\. NOVO MÓDULO: enriquecimento\_fontes.py (A Rastreabilidade na Prática)**

**Objetivo:** Materializar as regras de agrupamento e conversão nas tabelas originais sem destruir os dados brutos. Este módulo lê os Parquets base e gera "Parquets Enriquecidos" (Camada Trusted/Gold).

**Lógica de Implementação (Exemplo para NFe/NFCe):**

1. Carregar nfe\_base.parquet.  
2. Criar a coluna temporária codigo\_fonte \= co\_emitente \+ | \+ prod\_cprod.  
3. Fazer um LEFT JOIN com a tabela ponte map\_produto\_agrupado usando codigo\_fonte para trazer o id\_agrupado.  
4. Trazer as informações padrão: LEFT JOIN com produtos\_agrupados usando id\_agrupado (traz a descrição padronizada, NCM padronizado, etc).  
5. Aplicar a Conversão: LEFT JOIN com fatores\_conversao usando id\_agrupado E prod\_ucom (a unidade comercial da NF).  
   * Condição: Se o fator for encontrado, calcular: qtd\_padronizada \= prod\_qcom \* fator.  
   * Se não for encontrado (ou a unidade for a de referência), qtd\_padronizada \= prod\_qcom.  
   * Calcular valor unitário padronizado: vuncom\_padronizado \= prod\_vuncom / fator.

**Colunas Finais do Parquet Enriquecido (Ex: nfe\_enriquecida.parquet):**

* *Todas as colunas originais (chave\_acesso, prod\_nitem, prod\_cprod, prod\_qcom, prod\_ucom...)*  
* id\_agrupado (A chave do seu produto mestre)  
* descr\_padrao, ncm\_padrao (Dados saneados)  
* unid\_ref (A unidade de medida padrão definida no módulo 4\)  
* fator\_conversao\_aplicado (O fator utilizado nesta linha, ex: 12\)  
* qtd\_padronizada (A quantidade na unidade de referência)  
* vuncom\_padronizado (O valor unitário na unidade de referência)

## **Resumo das Vantagens do Plano Revisado**

1. **Zero Perda de Informação:** Como não agrupamos prematuramente por "descrição" no Módulo 2, não corremos o risco de unir um produto do fornecedor A com o fornecedor B erroneamente sem chance de desfazer.  
2. **Desacoplamento:** Se um usuário cometer um erro no Módulo 3 (Agrupamento Manual), basta atualizar a tabela map\_produto\_agrupado e re-rodar o Módulo 5\. O Parquet será recriado instantaneamente com os novos id\_agrupado e quantidades padronizadas recalculadas.  
3. **Auditoria Fiscal Fácil:** Se o fisco perguntar "Como você chegou neste total de estoque do id\_agrupado X?", você filtra id\_agrupado \= X no nfe\_enriquecida.parquet e terá a lista exata de todas as chaves de acesso de NF-e, linhas (prod\_nitem) e o fator exato aplicado linha a linha.