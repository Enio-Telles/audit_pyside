# Analise Tecnica Profunda - Metodologia MDS no runtime atual

**Data:** 20/04/2026  
**Escopo:** agregacao de produtos, conversao de unidades, movimentacao de estoque e simplificacao da implementacao/auditabilidade  
**Principio orientador:** entre alternativas equivalentes, preferir sempre a solucao mais simples que preserve o contrato atual do pipeline

Este relatorio foi montado a partir do codigo realmente executado pelo pipeline e da cobertura de testes vigente. Para o snapshot atual, ele deve ser tratado como referencia mais atual que `docs/analise_audit_pyside.md`, cujo conteudo ficou parcialmente superado pelo codigo recente.

## 1. Resumo Executivo

### Veredicto final

`metodo consistente mas parcialmente aplicado`

### Leitura curta

- o fluxo principal do pipeline esta coerente e hoje passa por proxies simples ate implementacoes canonicas claras;
- a nomenclatura ainda e fragmentada entre nomes internos, aliases de apresentacao e nomes conceituais da metodologia;
- a regra mais simples desejada para produtos ficou consolidada assim: grupo-base automatico apenas por igualdade estrita da descricao principal normalizada, com descricao complementar e demais atributos listados para decisao manual;
- a maior parte das divergencias atuais esta na documentacao auxiliar e na redundancia de implementacao, nao no nucleo fiscal;
- a melhor linha de acao e enxuta: consolidar a nomenclatura documental, corrigir dois contratos documentais incorretos e simplificar trechos redundantes do runtime sem reescrever o pipeline.

### Principais riscos atuais

- nomes diferentes para o mesmo conceito (`id_agrupado`, `id_agregado`, `id_produto_agrupado`) ainda aumentam custo de manutencao e erro de leitura;
- chamar o grupo-base automatico de "agregacao final" induz a leitura errada de que semelhanca semantica estaria sendo decidida pelo codigo, quando a regra desejada e manual;
- `docs/tabelas/fatores_conversao.md` e `docs/tabelas/produtos_final.md` tinham pontos objetivos fora do contrato real;
- `movimentacao_estoque.py` ainda repete a mesma derivacao central de quantidades em dois blocos consecutivos;
- medias de mensal, anual e periodos nao usam exatamente o mesmo filtro semantico, o que precisa ficar explicitado como diferenca operacional e nao como regra universal.

### Acao ja executada nesta rodada

- consolidacao metodologica em `docs/metodologia_mds_plan.md` e `metodologia_mds/02_agregacao_produtos.md`;
- consolidacao documental de nomenclatura em `docs/tabelas/fatores_conversao.md`;
- consolidacao documental de contrato em `docs/tabelas/produtos_final.md`;
- producao deste relatorio tecnico no formato solicitado.

## 2. Fluxo real do pipeline e nomenclatura consolidada

### 2.1 Fluxo real executado

Fluxo confirmado a partir de `src/orquestrador_pipeline.py`:

```text
orquestrador_pipeline.py
  -> transformacao.produtos_final_v2:gerar_produtos_final
    -> transformacao.rastreabilidade_produtos._produtos_final_impl

  -> transformacao.fontes_produtos:gerar_fontes_produtos
    -> transformacao.rastreabilidade_produtos.fontes_produtos

  -> transformacao.fatores_conversao:calcular_fatores_conversao
    -> transformacao.rastreabilidade_produtos.fatores_conversao

  -> transformacao.movimentacao_estoque:gerar_movimentacao_estoque
    -> transformacao.movimentacao_estoque_pkg.movimentacao_estoque

  -> transformacao.calculos_mensais:gerar_calculos_mensais
    -> transformacao.calculos_mensais_pkg.calculos_mensais

  -> transformacao.calculos_anuais:gerar_calculos_anuais
    -> transformacao.calculos_anuais_pkg.calculos_anuais

  -> transformacao.calculos_periodo_pkg:gerar_calculos_periodos
    -> transformacao.calculos_periodo_pkg.calculos_periodo
```

### 2.2 Nomenclatura consolidada

Regra de consolidacao adotada neste relatorio:

- nome canonico interno do pipeline vence quando houver conflito;
- alias de apresentacao fica documentado como alias, nao como chave interna;
- nomes conceituais da metodologia ficam mapeados explicitamente, nunca assumidos como contrato literal.

| Conceito | Canonico interno | Alias / apresentacao | Observacao |
| --- | --- | --- | --- |
| produto agregado | `id_agrupado` | `id_agregado` | `id_agregado` aparece nas abas finais |
| origem do produto | `codigo_fonte` | `id_produto_origem` | nome conceitual, nao coluna canonica |
| chave detalhada da descricao | `id_descricao` | `chave_produto` | depende da camada |
| quantidade observada | `q_conv` | `quantidade_convertida` | alias conceitual em camadas auxiliares |
| quantidade fisica | `q_conv_fisica` | `quantidade_fisica` | estoque final fica zero aqui |
| quantidade sinalizada | `__q_conv_sinal__` | `quantidade_fisica_sinalizada` | usada no saldo sequencial |
| estoque final declarado | `__qtd_decl_final_audit__` | `estoque_final_declarado` | valor de auditoria |
| unidade de referencia | `unid_ref` | `unidade_referencia` | `unidade_referencia` e nome conceitual |
| tabela de periodos | `aba_periodos_<cnpj>.parquet` | `tabela_periodos` | nome conceitual |
| tabela mensal | `aba_mensal_<cnpj>.parquet` | `tabela_mensal` | nome conceitual |
| tabela anual | `aba_anual_<cnpj>.parquet` | `tabela_anual` | nome conceitual |

## 3. Diagnostico da agregacao de produtos

### Regra metodologica consolidada

A regra desejada mais simples para agregacao ficou consolidada assim:

1. unir automaticamente apenas descricoes principais iguais apos normalizacao;
2. a normalizacao da descricao principal e: maiusculas, trim nas pontas, remocao de acentos e colapso de espacos internos duplicados;
3. descricao complementar nao entra na chave automatica;
4. NCM, CEST, GTIN, CO_SEFIN, unidade e demais atributos tambem nao entram na chave automatica;
5. descricao complementar e demais atributos devem ser listados para auditoria;
6. qualquer agregacao alem da igualdade estrita da descricao principal normalizada e manual.

### Implementacao real

- `03_descricao_produtos.py` ja normaliza apenas a coluna `descricao` e agrupa por `descricao_normalizada`;
- essa camada ja lista `lista_desc_compl`, `lista_ncm`, `lista_cest`, `lista_gtin`, `lista_co_sefin`, `lista_unid`, `lista_codigos`, `fontes` e `lista_codigo_fonte`;
- `_produtos_final_impl.py` gera `id_agrupado_base` deterministicamente a partir de `descricao_normalizada`;
- `id_agrupado` nasce igual ao grupo-base e pode receber override manual por `id_descricao` e, em seguida, por `descricao_normalizada`;
- `produtos_agrupados_<cnpj>.parquet` e a tabela mestre do agrupamento vigente;
- `produtos_final_<cnpj>.parquet` e a camada detalhada por `id_descricao`;
- `map_produto_agrupado_<cnpj>.parquet` preserva `codigo_fonte` e `descricao_normalizada` para vinculo posterior.

### Divergencias

1. A principal divergencia estava na linguagem: a documentacao vinha chamando de "agrupamento automatico" aquilo que precisa ser lido como grupo-base automatico por descricao principal normalizada, seguido de agregacao manual.
2. `docs/tabelas/produtos_final.md` chamava `produtos_final` de tabela mestre e listava `codigo_fonte` como se fosse coluna canonica da saida.
3. O fio de ouro depende de `id_linha_origem`, mas `fontes_produtos.py` so preserva essa coluna quando ela ja vem da extracao; o runtime nao a torna obrigatoria.

### Riscos

- chamar a etapa automatica de agregacao final abre margem para misturar semelhanca textual com decisao de negocio;
- confundir `produtos_final` com a tabela mestre leva a diagnosticos errados sobre onde o agrupamento e consolidado;
- confundir `codigo_fonte` como coluna de `produtos_final` embaralha o papel da tabela ponte;
- ausencia de `id_linha_origem` nao quebra o pipeline, mas reduz rastreabilidade ponta a ponta.

### Melhorias propostas

- manter `produtos_agrupados` como fonte de verdade do grupo vigente e `produtos_final` como camada detalhada;
- fixar documentalmente que descricao complementar e demais atributos sao listas de apoio para revisao manual;
- tratar `id_agrupado_base` como grupo-base automatico minimo e `id_agrupado` como agrupamento vigente;
- documentar `id_linha_origem` como requisito desejavel de extracao, nao como garantia universal do runtime;
- preservar o fluxo atual sem abrir nova camada ou renomeacao estrutural ampla.

## 4. Diagnostico da conversao de unidades

### Metodo documentado

- `docs/conversao_unidades.md` e a metodologia MDS atual ja reconhecem a realidade do runtime: escolha de `unid_ref` por precedencia e calculo de `fator` ainda apoiado em preco medio;
- `docs/tabelas/fatores_conversao.md` estava parcialmente desalinhado ao resumir `fator_origem` de forma excessivamente simplificada e ao listar `fator_conversao_origem` como se fosse coluna canonica.

### Implementacao real

- `fatores_conversao.py` tenta primeiro vinculo fisico `id_item_unid -> id_agrupado`, quando o artefato existe;
- sem esse artefato, cai para vinculo por `descricao_normalizada`;
- `unid_ref` e resolvida por `unid_ref_override -> unid_ref_sugerida -> unid_ref_auto`;
- `fator` e resolvido por `fator_override -> fator_calc`;
- `fator_origem` hoje pode assumir `manual`, `preco`, `fallback_sem_preco` e `fallback_sem_preco_ref`.

### Divergencias

1. A doc auxiliar de fatores resumia `fator_origem` como `manual`, `fallback` e `preco`, mas o codigo expõe duas categorias de fallback.
2. A mesma doc citava `fator_conversao_origem` como se fosse coluna canonica da tabela, quando isso e apenas alias conceitual em camadas auxiliares.

### Riscos

- leitura documental imprecisa pode gerar filtros errados em auditoria de fatores;
- consolidar ambos os fallbacks em um rotulo generico perde informacao operacional importante.

### Melhorias propostas

- manter `fator_origem` como contrato real da tabela canonica;
- reservar `fator_conversao_origem` para documentacao conceitual do service MDS;
- evitar qualquer tentativa de renomear a tabela agora; a solucao mais simples e documentar corretamente.

## 5. Diagnostico da movimentacao de estoque e das tabelas finais

### Metodo documentado

- `docs/mov_estoque.md` e a pasta `metodologia_mds/` estao alinhados quanto ao papel de `q_conv`, `q_conv_fisica`, `__q_conv_sinal__` e `__qtd_decl_final_audit__`;
- `docs/tabela_periodo.md` documenta periodos com `q_conv`;
- mensal e anual trabalham com `q_conv_fisica` quando disponivel.

### Implementacao real

- `movimentacao_estoque.py` materializa `q_conv`, `q_conv_fisica`, `__q_conv_sinal__`, `periodo_inventario`, deltas declarados e colunas conceituais via `MovimentacaoService`;
- `calculos_mensais.py` usa `q_conv_fisica` e um conjunto ampliado de flags de devolucao para medias;
- `calculos_anuais.py` usa `q_conv_fisica`, mas com filtro de medias mais restrito;
- `calculos_periodo.py` usa `q_conv` e tambem um filtro mais restrito.

### Divergencias

1. Mensal, anual e periodos nao usam exatamente o mesmo criterio de exclusao de linhas para medias.
2. `movimentacao_estoque.py` repete a mesma derivacao central de `q_conv`, `q_conv_fisica`, `__q_conv_sinal__` e `__qtd_decl_final_audit__` em dois blocos consecutivos.

### Riscos

- a assimetria de filtros entre mensal, anual e periodos pode gerar leitura equivocada de que todas as medias significam exatamente a mesma coisa;
- a duplicacao da derivacao central aumenta risco de regressao futura por manutencao parcial.

### Melhorias propostas

- manter a semantica atual, mas explicitar no contrato quando as medias nao sao simetricas entre camadas;
- extrair a derivacao repetida em `movimentacao_estoque.py` para um unico bloco ou helper interno, sem redesenhar o pipeline.

## 6. Diagnostico da otimizacao, simplicidade e auditabilidade

### Metodo documentado

A preferencia validada pelo usuario e pela manutencao do repo e: fechar o escopo atual com a menor mudanca segura, evitando reescritas amplas.

### Implementacao real

O codigo atual ja avancou na direcao correta em dois pontos importantes:

- proxies simples substituíram boa parte da indirecao mais fragil;
- logs de auditoria relevantes foram adicionados em fatores e fontes.

### Problemas atuais de simplicidade

1. `movimentacao_estoque.py` ainda tem derivacao duplicada da mesma regra central.
2. `fontes_produtos.py` tem um `return` morto em `_normalizar_descricao_expr`, sinal claro de resquicio de refatoracao.
3. `docs/analise_audit_pyside.md` ficou historicamente util, mas nao representa mais o snapshot atual.

### Melhorias propostas

- remover codigo morto e blocos duplicados antes de abrir qualquer refactor maior;
- padronizar relatórios vivos e relatorios historicos para nao competir pelo mesmo status de verdade;
- adicionar testes de contrato de schema/nomenclatura nas saidas finais para impedir regressao documental.

## 7. Tabela de inconsistencias metodo x implementacao x testes

| Tema | Severidade | Metodo / documentacao | Comportamento real do codigo | Cobertura de testes | Risco | Correcao recomendada |
| --- | --- | --- | --- | --- | --- | --- |
| etapa automatica tratada como agregacao final | alta | a metodologia anterior nao separava claramente grupo-base automatico de agregacao manual | `descricao_produtos` ja forma buckets por descricao principal normalizada e lista `lista_desc_compl` e demais atributos; a decisao adicional de negocio deve ser manual | ha cobertura de agregacao e agrupamento manual, mas nao um teste contratual dessa leitura | misturar semelhanca textual com decisao manual | corrigido nesta rodada na metodologia e no relatorio |
| `produtos_final` tratado como tabela mestre | media | `docs/tabelas/produtos_final.md` apresentava `produtos_final` como mestre | a tabela mestre real e `produtos_agrupados`; `produtos_final` e a camada detalhada por `id_descricao` | ha cobertura para agrupamento e manual, mas nao para esse contrato documental | leitura errada do fio de ouro | corrigido nesta rodada na doc |
| `codigo_fonte` listado como coluna de `produtos_final` | media | doc auxiliar listava `codigo_fonte` como coluna principal | `codigo_fonte` pertence a `map_produto_agrupado`, nao a `produtos_final` | nao ha teste de contrato documental | erro de leitura do papel da tabela ponte | corrigido nesta rodada na doc |
| `fator_origem` simplificado demais | media | doc auxiliar reduzia `fator_origem` a `manual/fallback/preco` | runtime usa `manual`, `preco`, `fallback_sem_preco`, `fallback_sem_preco_ref` | testes cobrem `manual`, `preco`, `fallback_sem_preco`, mas nao toda a doc | perda de granularidade operacional | corrigido nesta rodada na doc |
| `fator_conversao_origem` como coluna canonica | baixa | doc auxiliar listava coluna que nao pertence ao parquet canonico | esse nome aparece como alias conceitual no service MDS, nao na tabela de fatores | sem teste contratual | nomenclatura confusa | corrigido nesta rodada na doc |
| filtros de medias nao uniformes entre camadas | media | leitura metodologica pode sugerir regra unica | mensal usa conjunto ampliado de flags; anual e periodos usam filtro mais restrito | cobertura existe por camada, nao como contrato cruzado | comparacoes cruzadas podem ser mal interpretadas | documentar explicitamente a assimetria e testar contrato cruzado |
| derivacao central duplicada em `movimentacao_estoque.py` | media | metodo pressupoe uma regra unica e auditavel | implementacao repete o mesmo bloco de derivacao | testes cobrem comportamento, nao estrutura | manutencao propensa a drift futuro | extrair para helper unico sem mudar semantica |
| `id_linha_origem` nao e garantido de ponta a ponta | media | o fio de ouro ideal o trata como elo forte | runtime so preserva a coluna quando a fonte a entrega | ha testes de presenca e ausencia local, nao de pipeline completo | rastreabilidade parcial em algumas extrações | documentar como requisito de extracao e criar smoke test de pipeline |
| `docs/analise_audit_pyside.md` superado | baixa | parecer antigo ainda circula como analise viva | snapshot atual ja corrigiu varias premissas antigas | sem teste, por ser artefato textual | equipe consultar diagnostico obsoleto | manter este relatorio como referencia viva do snapshot atual |

## 8. Tabela de otimizacoes propostas

| Tema | Problema atual | Proposta | Impacto esperado | Risco | Prioridade | Arquivos afetados |
| --- | --- | --- | --- | --- | --- | --- |
| terminologia da agregacao | "agrupamento automatico" estava sendo lido como agregacao final | separar documentalmente `grupo-base automatico` de `agregacao manual` | menor ambiguidade metodologica e menos retrabalho operacional | baixo | alta | `docs/metodologia_mds_plan.md`, `metodologia_mds/02_agregacao_produtos.md`, relatorio |
| consolidacao de nomenclatura | termos misturados entre metodo, runtime e apresentacao | manter tabela canonica de nomes e usar sempre o nome interno primeiro | menor ambiguidade em auditoria e manutencao | baixo | alta | `docs/analise_metodologia_mds_runtime_2026-04-20.md`, docs auxiliares |
| documentacao de `produtos_final` | papel da tabela estava superestimado | tratar `produtos_agrupados` como mestre e `produtos_final` como detalhamento | clareza arquitetural | baixo | alta | `docs/tabelas/produtos_final.md` |
| documentacao de fatores | contrato documental incompleto de `fator_origem` | listar valores reais e separar alias conceitual de coluna canonica | auditoria mais precisa | baixo | alta | `docs/tabelas/fatores_conversao.md` |
| derivacao duplicada no mov_estoque | mesmo bloco de calculo aparece duas vezes | extrair helper interno unico | menor risco de drift e mais legibilidade | baixo | media | `src/transformacao/movimentacao_estoque_pkg/movimentacao_estoque.py` |
| return morto em `fontes_produtos` | sobra de refatoracao | remover return inalcançavel | limpeza e menor ruído | baixo | media | `src/transformacao/rastreabilidade_produtos/fontes_produtos.py` |
| teste de contrato de saidas | hoje os testes cobrem comportamento por modulo, mas pouco o contrato cruzado de nomes | criar smoke tests de schema para `produtos_final`, `fatores_conversao`, `aba_*` | regressao documental mais dificil | baixo | media | `tests/` |
| relatorio tecnico vivo | diagnostico antigo ficou superado | manter relatorio por snapshot em vez de reusar um unico arquivo historico | menos conflituo entre contexto velho e atual | baixo | media | `docs/` |

## 9. Melhorias especificas propostas

### 9.1 Melhorias de metodo

- fixar que o nome canonico interno e sempre o primeiro nome citado na documentacao;
- fixar que a etapa automatica de produtos e apenas o grupo-base por descricao principal normalizada;
- fixar que descricao complementar e demais atributos entram como listas de apoio, nunca como chave automatica;
- fixar que agregacao alem da igualdade estrita da descricao principal normalizada e manual;
- tratar `id_agregado`, `quantidade_convertida`, `quantidade_fisica` e `unidade_referencia` como aliases de leitura, nunca como substitutos automaticos do contrato atual;
- documentar explicitamente quando uma camada usa semantica diferente de outra para medias.

### 9.2 Melhorias de implementacao

- remover duplicacao da derivacao central de quantidades em `movimentacao_estoque.py`;
- remover codigo morto em `_normalizar_descricao_expr`;
- nao reabrir refatoracao estrutural grande; a menor mudanca segura e suficiente aqui.

### 9.3 Melhorias de testes

- adicionar testes de contrato de schema/nomenclatura para `produtos_final`, `fatores_conversao`, `aba_periodos`, `aba_mensal` e `aba_anual`;
- adicionar teste cruzado que compare a regra de exclusao de medias entre mensal, anual e periodos, deixando explicita a diferenca esperada;
- adicionar smoke test do `REGISTO_TABELAS` cobrindo o caminho principal de produtos -> fontes -> fatores -> mov_estoque -> abas.

### 9.4 Melhorias de observabilidade e auditoria

- registrar de forma resumida quantas linhas chegaram com `id_linha_origem` por fonte;
- registrar quantos vinculos de fatores usaram chave fisica versus fallback por descricao;
- manter a safa de auditoria atual em fatores e fontes, sem abrir nova infra.

### 9.5 Melhorias de performance e manutenibilidade

- simplificar blocos redundantes antes de qualquer otimizacao agressiva;
- manter proxies simples atuais, que ja estao mais saudaveis que a indirecao historica;
- evitar novas camadas de alias persistido no parquet enquanto os consumidores ainda dependem dos nomes legados.

## 10. Lista priorizada de correcoes

1. Fixar documentalmente a regra simples de produtos: grupo-base automatico por descricao principal normalizada e agregacao final manual.
2. Corrigir e manter os contratos documentais de `produtos_final` e `fatores_conversao`.
3. Tratar este relatorio como referencia viva do snapshot atual e o diagnostico antigo como historico.
4. Extrair a derivacao duplicada de quantidades em `movimentacao_estoque.py`.
5. Remover o `return` morto em `fontes_produtos.py`.
6. Criar smoke tests de contrato de schema/nomenclatura.
7. Criar teste cruzado explicito para as diferencas de medias entre mensal, anual e periodos.

## 11. Sugestao de patches por arquivo

### Patch 1 - documental, sem risco funcional

- `docs/tabelas/produtos_final.md`
  - corrigir o papel da tabela;
  - remover `codigo_fonte` como coluna canonica;
  - reescrever o fio de ouro com a tabela ponte no lugar correto.

- `docs/tabelas/fatores_conversao.md`
  - listar os valores reais de `fator_origem`;
  - remover `fator_conversao_origem` como coluna canonica;
  - explicitar que ele e alias conceitual.

### Patch 2 - simplificacao local futura

- `src/transformacao/movimentacao_estoque_pkg/movimentacao_estoque.py`
  - extrair a derivacao repetida de `q_conv`, `q_conv_fisica`, `__q_conv_sinal__` e `__qtd_decl_final_audit__` para helper unico.

- `src/transformacao/rastreabilidade_produtos/fontes_produtos.py`
  - remover `return` inalcançavel em `_normalizar_descricao_expr`.

## 12. Gaps de testes e testes a criar

### Cenarios nao cobertos ou cobertos de forma insuficiente

- contrato documental das colunas expostas por `produtos_final_<cnpj>.parquet`;
- contrato documental das colunas expostas por `fatores_conversao_<cnpj>.parquet`;
- diferenca semantica entre medias de mensal, anual e periodos;
- caminho completo do pipeline a partir do `REGISTO_TABELAS` para o trilho de produtos/estoque;
- contagem resumida de `id_linha_origem` por fonte em um run real.

### Testes de regressao necessarios

- teste de schema de `produtos_final` garantindo ausencia de `codigo_fonte` como coluna canonica;
- teste de schema de `fatores_conversao` garantindo presenca de `fator_origem` e ausencia de `fator_conversao_origem`;
- teste estrutural simples assegurando que a derivacao central de quantidades em `movimentacao_estoque.py` permaneceu unificada depois da simplificacao.

### Testes para reprocessamento

- manter e expandir a suite de reconciliacao de overrides em fatores;
- adicionar caso que combine remapeamento de agrupamento com `unid_ref_override` e `fator_override` simultaneamente.

### Testes para overrides manuais e auditabilidade

- caso com `mapa_agrupamento_manual` por `id_descricao`;
- caso com `mapa_agrupamento_manual` por `descricao_normalizada`;
- caso com `lista_codigo_fonte` ausente na ponte e aviso emitido.

### Testes para devolucoes e entradas desacobertadas

- contrato cruzado entre mensal e anual mostrando que a camada mensal usa conjunto mais amplo de flags de devolucao;
- caso com `entradas_desacob` e ST ativo versus inativo, ja coberto parcialmente, mas faltando comparacao cruzada com periodos.

### Testes para auditoria de estoque final

- confirmar que `q_conv` pode permanecer preenchido para inventario sem afetar saldo;
- confirmar que `q_conv_fisica = 0` e `__q_conv_sinal__ = 0` em estoque final;
- confirmar que `__qtd_decl_final_audit__` e o unico campo usado para o estoque declarado nas abas.

## 13. Conclusao final

### Classificacao obrigatoria

`metodo consistente mas parcialmente aplicado`

### Justificativa

- o metodo central de agrupamento, fatores, movimentacao e abas esta operacionalmente coerente no codigo executado;
- no caso de produtos, o runtime ja implementa a parte simples desejada: normaliza a descricao principal, forma grupos-base por igualdade estrita e lista descricao complementar e demais atributos para revisao;
- os testes sustentam os principais comportamentos fiscais e de rastreabilidade local;
- as maiores deficiencias restantes estao em padronizacao de nomes, em separar corretamente grupo-base automatico de agregacao manual, em contratos documentais auxiliares e em redundancias de implementacao;
- a melhor resposta tecnica, coerente com a preferencia pela solucao mais simples, e corrigir contratos documentais e remover redundancias locais antes de qualquer refatoracao estrutural.

### Fecho objetivo

Hoje o problema principal nao e "metodo errado", e sim "metodo simples parcialmente bem implementado, mas ainda descrito com nomes e camadas que induzem leitura errada". A correcao mais eficiente e pequena: consolidar nomes, registrar que descricao complementar e demais atributos sao listas de apoio, fixar que a agregacao final e manual, ajustar as docs auxiliares e simplificar trechos redundantes do runtime.
