# Plano de Reorganização para Centralizar Todas as Consultas no Projeto

## Resumo
Adotar `corte direto` e tornar o repositório a única fonte de verdade para SQLs usadas pelo sistema. A reorganização vai consolidar todas as consultas ativas em uma árvore única dentro de `sql/`, remover dependências runtime de `c:\funcoes - Copia\sql` e `c:\funcoes - Copia\consultas_fonte`, e alinhar backend FastAPI, frontend React e desktop PySide ao mesmo catálogo de consultas.

Estrutura-alvo das SQLs:
```text
sql/
  fiscal/
    efd/
    documentos/
    fronteira/
    validacao/
  fisconforme/
    cadastro/
    malhas/
  apoio/
    dicionarios/
    verificacoes/
  archive/
```

Critério de organização escolhido: `por domínio`.
Estratégia escolhida: `corte direto`, sem fallback runtime para pastas externas após a migração.

## Mudanças de Implementação
### 1. Consolidar e canonizar o acervo SQL
- Inventariar todas as consultas realmente usadas hoje por:
  - descoberta automática do pipeline,
  - referências diretas do Fisconforme,
  - estado salvo em `workspace/app_state/selections.json`,
  - endpoint de listagem SQL da API.
- Mover/copiar para dentro de `sql/` todas as consultas externas ainda ativas, preservando conteúdo e nome funcional, mas reposicionando-as por domínio.
- Tornar `sql/` a árvore canônica; `src/sql/` deixa de ser catálogo paralelo. A consulta hoje em `src/sql/c176_verificacao.sql` deve ser absorvida para `sql/fiscal/validacao/c176_verificacao.sql`.
- Consultas históricas ou não usadas em runtime ficam em `sql/archive/` e não entram na descoberta automática.

### 2. Padronizar descoberta e identificação de consultas
- Substituir o modelo atual baseado em caminhos absolutos por identificadores relativos ao repositório.
- Interface pública nova para seleção/listagem de SQL: cada consulta passa a ser identificada por um path relativo a `sql/`, por exemplo `fisconforme/malhas/FISCOFORME_MALHAS_ID_10120.sql`.
- Remover da configuração e dos serviços:
  - `EXTRA_SQL_DIRS`
  - dependência de `FUNCOES_ROOT` para SQL
  - qualquer busca em `consultas_fonte` externa
- A descoberta automática do pipeline passa a escanear apenas `sql/`, com exclusão explícita de `sql/archive/`.

### 3. Reorganizar a camada de paths/configuração
- Criar um ponto único de resolução de caminhos do projeto para runtime ativo, com constantes equivalentes a:
  - `PROJECT_ROOT`
  - `SQL_ROOT = PROJECT_ROOT / "sql"`
  - `DATA_ROOT = PROJECT_ROOT / "dados"`
  - `CNPJ_ROOT = DATA_ROOT / "CNPJ"`
- Todos os consumidores ativos devem usar esse resolver único:
  - extração Oracle,
  - serviços de SQL,
  - pipeline,
  - Fisconforme,
  - routers do backend.
- Qualquer referência runtime restante a `c:\funcoes - Copia` ou `c:\funcoes` deve ser tratada como bug de migração.

### 4. Migrar chamadas e estado persistido
- Atualizar o estado salvo de seleções para armazenar IDs relativos de SQL, não caminhos absolutos legados.
- Regra de migração do estado:
  - ao carregar uma seleção antiga com caminho absoluto, converter para o path relativo novo quando houver correspondência;
  - se não houver correspondência, descartar a entrada e registrar aviso no log.
- Atualizar backend e desktop para retornarem e aceitarem paths relativos no catálogo SQL.
- Atualizar qualquer tela React ou PySide que exiba/consuma consulta para trabalhar com o novo identificador canônico.

### 5. Reorganizar a estrutura do projeto sem quebrar runtime
- Manter como runtime oficial:
  - `backend/` para API FastAPI,
  - `frontend/` para React,
  - `src/` para extração, transformação e desktop.
- Não misturar SQL ativa em `src/`; `src/` deve conter código, não catálogo operacional de consultas.
- `server/python/` não entra nesta migração estrutural de SQL e deve ser tratado como legado não canônico até uma auditoria separada.
- Atualizar README e documentação operacional para apontar apenas para a nova árvore `sql/` como fonte de verdade.

## Interfaces e Contratos que Mudam
- Catálogo SQL:
  - antes: caminhos absolutos e múltiplas raízes
  - depois: um único catálogo em `sql/`, com IDs relativos
- Configuração:
  - antes: combinação de `SQL_DIR` + `EXTRA_SQL_DIRS` + raízes externas
  - depois: `SQL_ROOT` único no projeto
- Estado persistido:
  - antes: `selections.json` com paths como `c:\funcoes - Copia\sql\...`
  - depois: paths relativos como `fiscal/efd/c170.sql`

## Testes e Critérios de Aceite
- Busca textual no repositório não pode retornar referências runtime restantes a:
  - `c:\funcoes - Copia\sql`
  - `c:\funcoes - Copia\consultas_fonte`
  - `c:\funcoes\sql`
- O endpoint de listagem SQL deve retornar apenas itens sob `sql/`.
- O pipeline deve conseguir descobrir e executar consultas usando apenas a árvore local do projeto.
- O Fisconforme deve continuar localizando:
  - `dados_cadastrais.sql`
  - `Fisconforme_malha_cnpj.sql`
  - malhas `10061`, `10080`, `10100`, `10120`
  a partir da nova estrutura local.
- `selections.json` antigo deve ser carregado sem quebrar a UI, com migração ou descarte seguro de entradas legadas.
- Smoke test obrigatório:
  - extração React/FastAPI para um CNPJ real,
  - consulta Fisconforme com malha `10120`,
  - listagem SQL na API,
  - abertura da seleção no desktop PySide.

## Assunções e Defaults
- A reorganização será feita em uma única entrega lógica, sem compatibilidade runtime com diretórios externos após merge.
- A árvore `sql/` será a única fonte de verdade para consultas ativas.
- Consultas históricas serão preservadas em `sql/archive/`, mas fora da descoberta automática.
- `server/python/` fica fora desta reorganização operacional e não deve dirigir decisões de layout desta migração.
