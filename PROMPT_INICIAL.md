# Prompt inicial para o Claude Code

> Cole o conteudo entre as linhas como primeira mensagem ao Claude Code.
> Antes de colar:
>
> 1. Coloque os arquivos `CLAUDE.md`, `PLANO_CLAUDE_CODE.md` e os
>    quatro `.patch` na pasta de trabalho onde voce vai abrir o
>    Claude Code.
> 2. Inicie o Claude Code nessa pasta: `claude`

---

Voce vai implementar uma metodologia nova de similaridade no projeto
**audit_pyside** (Python/Polars/PySide6). Ja tenho 4 commits prontos
de melhorias incrementais ao metodo atual e quero adicionar um
metodo alternativo baseado em particionamento por chaves fiscais.

**Arquivos disponiveis nesta pasta:**

- `PLANO_CLAUDE_CODE.md` — plano completo, leia integralmente antes
  de comecar
- `CLAUDE.md` — convencoes do projeto (copie para a raiz do repo
  apos clonar)
- `0001-*.patch` ate `0004-*.patch` — 4 commits ja prontos para
  aplicar como ponto de partida

**O que vou pedir:**

1. Clone o repo `https://github.com/Enio-Telles/audit_pyside.git`,
   crie a branch `feat/similaridade-particionamento` a partir de
   `main`, copie o `CLAUDE.md` para a raiz do repo e aplique os 4
   patches com `git am`.
2. Rode a suite de testes para validar o ponto de partida.
3. Leia o `PLANO_CLAUDE_CODE.md`. Quando terminar, me apresente um
   resumo das fases em 5-8 linhas para eu confirmar antes de comecar.
4. Implemente as 5 fases do plano em ordem, **uma fase por commit**,
   rodando testes apos cada uma. Use `TodoWrite` para acompanhar
   progresso.
5. Ao final de cada fase, me mostre o `git log --oneline` da branch
   e me pergunte se pode seguir para a proxima.

**Comece agora pelo passo 1.** Antes de qualquer outra coisa,
configure `git config core.autocrlf false` na sua copia local — o
arquivo principal usa CRLF e queremos preservar.
