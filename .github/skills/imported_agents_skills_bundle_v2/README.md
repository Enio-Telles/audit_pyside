# Agents & Skills Bundle v2

Pacote reorganizado com nomenclatura e estrutura mais limpas.

## Estrutura

- `agents/core/` — agentes principais recomendados
- `agents/specialized/` — agentes focados por função
- `agents/legacy/` — versões antigas mantidas por compatibilidade
- `skills/github/` — skills de GitHub e engenharia
- `skills/notion/` — skills de Notion e gestão do projeto
- `skills/local/` — skills operacionais locais

## Agentes recomendados

1. `github-orchestrator`  
   Agente geral para GitHub + Notion.
2. `notion-plan-executor`  
   Executor orientado pelo Notion.
3. `sistema-ro-plan-executor`  
   Executor específico para o projeto `sistema_ro`.

## Agentes especializados

- `github-maintainer`
- `github-reviewer`
- `github-ci-agent`
- `github-triage-agent`

## Agente legado

- `githubagent` — mantido por compatibilidade, mas com sobreposição funcional em relação ao `github-orchestrator`.

## Skills incluídas

### GitHub
- `gitbranches`
- `gitconflicts`
- `pullrequests`
- `issuetriage`
- `githubactions`
- `releases-changelog`
- `repository-health`
- `commitmessages`
- `monorepo-structure`
- `dependency-updates`
- `dependency-prs`
- `repo-docs-maintenance`
- `security-review`

### Notion
- `notion-workspace`
- `notion-project-management`

### Local
- `desktop-commander`

## Recomendação de uso

- Para uso geral, comece por `agents/core/github-orchestrator.yaml`.
- Para execução guiada por plano do Notion, use `agents/core/notion-plan-executor.yaml`.
- Para `sistema_ro`, prefira `agents/core/sistema-ro-plan-executor.yaml`.
- Use os agentes especializados apenas quando quiser separar claramente manutenção, revisão, CI ou triagem.

## Observações

- Os arquivos mantêm o conteúdo original, mas foram reorganizados para reduzir redundância na navegação.
- O bundle preserva todos os agentes e skills já gerados.
