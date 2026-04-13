# **Plano de Melhorias: Refatoração Arquitetural (Orientada a Abas)**

**Data:** Março de 2026

**Objetivo:** Transitar o projeto "Fiscal Parquet Analyzer" para uma arquitetura modular de "1 Tabela \= 1 Função \= 1 Arquivo/Pasta", eliminando acoplamentos, melhorando a rastreabilidade e espelhando a estrutura de abas da interface de utilizador (UI).

## **Fase 1: Preparação do Ecossistema e Remoção de Anti-Patterns**

*Foco: Transformar as pastas de scripts soltos em pacotes Python reais, permitindo importações limpas e seguras.*

* \[ \] **1.1. Criar ficheiros \_\_init\_\_.py:** Adicionar um ficheiro \_\_init\_\_.py vazio na raiz das pastas src/transformacao, src/extracao e src/utilitarios.  
* \[ \] **1.2. Limpar orquestrador\_pipeline.py:** Remover completamente o bloco de código que faz sys.path.insert(0, str(path)).  
* \[ \] **1.3. Atualizar Importações Globais:** Em todos os ficheiros afetados, substituir importações relativas ou locais por importações absolutas a partir da raiz src (ex: from src.utilitarios.text import normalizar\_texto).

## **Fase 2: Reestruturação de Diretórios em src/transformacao/**

*Foco: Mover e renomear os ficheiros atuais para refletirem exatamente as abas do sistema. Os nomes devem ser descritivos e em português.*

* \[ \] **2.1. Criar as novas pastas de domínio:**  
  * src/transformacao/tabelas\_base/  
  * src/transformacao/rastreabilidade\_produtos/  
  * src/transformacao/movimentacao\_estoque/  
  * src/transformacao/calculos\_mensais/  
  * src/transformacao/calculos\_anuais/  
* \[ \] **2.2. Migrar e renomear ficheiros base:**  
  * Mover tabela\_documentos.py para tabelas\_base/  
  * Mover 02\_itens.py para tabelas\_base/tabela\_itens.py  
  * Mover c170\_xml.py e c176\_xml.py para as respetivas pastas de verificação (ou criar uma pasta auditoria\_sped/).  
* \[ \] **2.3. Migrar e renomear ficheiros de Rastreabilidade (Aba Produtos):**  
  * Mover 01\_item\_unidades.py para rastreabilidade\_produtos/item\_unidades.py  
  * Mover 03\_descricao\_produtos.py para rastreabilidade\_produtos/descricao\_produtos.py  
  * Mover produtos\_agrupados.py para rastreabilidade\_produtos/produtos\_agrupados.py  
  * Mover 04\_produtos\_final.py (ou produtos\_final\_v2.py) para rastreabilidade\_produtos/produtos\_final.py  
  * Mover fatores\_conversao.py para rastreabilidade\_produtos/fatores\_conversao.py  
* \[ \] **2.4. Modularizar movimentacao\_estoque (Transformar em Pacote):**  
  * Mover o código de movimentacao\_estoque.py para a pasta src/transformacao/movimentacao\_estoque/.  
  * Dividir a lógica pesada em calculo\_saldos.py e entradas\_saidas.py.  
  * Criar um \_\_init\_\_.py que expõe apenas a função principal para o exterior.

## **Fase 3: Padronização de Contratos (Assinaturas de Funções)**

*Foco: Garantir que o orquestrador consiga executar qualquer passo do pipeline cegamente, confiando que todas as funções falam a mesma língua.*

* \[ \] **3.1. Refatorar funções principais:** Editar cada ficheiro movido na Fase 2 para garantir que a função de entrada principal segue o formato estrito:  
  def gerar\_nome\_da\_tabela(cnpj: str, \*\*kwargs) \-\> bool:  
      \# Lógica interna de Polars (try/except)  
      \# Retorna True em caso de sucesso, False se falhar

* \[ \] **3.2. Isolamento Total:** Garantir que NENHUMA destas funções importe ficheiros de src/interface\_grafica/ ou manipule ficheiros de estado da UI.

## **Fase 4: Refatoração Dinâmica do Orquestrador**

*Foco: Tornar o orquestrador\_pipeline.py dinâmico e inteligente (Padrão Registry).*

* \[ \] **4.1. Criar o Dicionário de Registo (Registry):** No topo do orquestrador, criar um mapeamento das strings (IDs das tabelas) para as respetivas funções importadas.  
  REGISTO\_TABELAS \= {  
      "tb\_documentos": gerar\_tabela\_documentos,  
      "item\_unidades": gerar\_item\_unidades,  
      "movimentacao\_estoque": gerar\_movimentacao\_estoque,  
      \# ...  
  }

* \[ \] **4.2. Eliminar "Hardcodes":** Remover o *array* ordem \= \[...\] e as validações do tipo if tab\_id in \["tb\_documentos"\]: break. Substituir por uma lógica que verifica dependências (ex: através de um pequeno grafo de execução NetworkX ou mapeamento de dependências críticas no próprio dicionário).

## **Fase 5: Otimização Direcionada (Polars & Assincronismo)**

*Foco: Atacar os problemas mapeados no diagnostico\_performance\_atual.md após a casa estar arrumada.*

* \[ \] **5.1. Otimizar movimentacao\_estoque/calculo\_saldos.py:** Substituir as iterações ou cálculos estáticos por funções nativas de janela (window functions) em C do Polars (.rolling(), .cumulative\_eval(), ou .group\_by\_dynamic()).  
* \[ \] **5.2. Assegurar Assincronismo na UI:** Confirmar que no src/interface\_grafica/services/pipeline\_service.py ou query\_worker.py, o chamamento ao executar\_pipeline\_completo do orquestrador é sempre feito através de uma QThread ou QRunnable, impedindo bloqueios visuais na aplicação PySide6.  
* \[ \] **5.3. Logs de Fallback:** Garantir que o ficheiro produtos\_final.py implementa a regra de negócio exigida: fallbacks de preços de venda devem gerar de forma obrigatória log\_sem\_preco\_medio\_compra\_\<cnpj\>.json e .parquet.