# Como usar este setup com Codex

## O que o Codex lê
O Codex lê arquivos `AGENTS.md` antes de começar a trabalhar.
A regra oficial é por escopo de diretório:
- um `AGENTS.md` na raiz vale para o repositório inteiro
- um `AGENTS.md` dentro de uma subpasta vale para aquela árvore específica
- arquivos mais específicos complementam os mais gerais

## Estrutura deste setup
- `AGENTS.md` → instruções globais do repositório
- `src/transformacao/AGENTS.md` → regras do pipeline e Polars/Parquet
- `src/interface_grafica/AGENTS.md` → regras da GUI PySide6
- `tests/AGENTS.md` → regras para testes
- `docs/AGENTS.md` → regras para documentação

## Como aplicar
1. copie esses arquivos para a raiz do repositório
2. commit e push
3. abra o projeto no Codex CLI, app ou ambiente integrado
4. rode uma tarefa como:
   - Planeje uma alteração em `src/transformacao` preservando rastreabilidade e reprocessamento.
   - Revise uma mudança na GUI PySide6 considerando performance, clareza e separação de responsabilidades.

## Dica prática
No Codex CLI, o comando `/init` pode gerar um AGENTS.md inicial, mas aqui você já tem uma versão adaptada ao projeto. Ajuste os comandos de teste locais se quiser deixar o agente ainda mais obediente.
