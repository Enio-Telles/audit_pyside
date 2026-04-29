# CLAUDE.md — convenções para trabalhos no audit_pyside

> Este arquivo deve ser copiado para a raiz do repo `audit_pyside`.
> O Claude Code o le automaticamente em toda sessao.

## Idioma e estilo

- Codigo, comentarios, docstrings, mensagens de commit e nomes de
  variaveis: **portugues sem acentos**. Mantenha essa convencao
  mesmo quando soar estranho. O projeto inteiro segue isso.
- **Sem emojis** em codigo, comentarios ou commits.
- Mensagens de commit no formato Conventional Commits:
  `feat(escopo): descricao curta`, `test(escopo): ...`,
  `refactor(escopo): ...`, `docs(escopo): ...`. Corpo em bullets curtos.

## Estrutura do projeto

- Codigo de producao em `src/`.
- Testes em `tests/`.
- O `pyproject.toml` declara `pythonpath = ["src"]`, entao imports
  usam `from interface_grafica.services...`, **sem prefixo `src.`**.
- Documentacao em `docs/`.

## Stack

- Python >= 3.11
- `polars >= 1.0`, `PySide6 >= 6.5`, `pytest`, `numba`, `structlog`
- **Nenhuma dependencia nova** sem confirmar com o usuario via
  pergunta explicita. Bibliotecas opcionais (datasketch, scipy,
  sentence-transformers) entram via `try/except ImportError` com
  fallback transparente.

## Como rodar testes

```bash
# Suite focada nos modulos de similaridade
PYTHONPATH=src python3 -m pytest \
    tests/test_descricao_similarity_service.py \
    tests/test_unidades_descricao.py \
    tests/test_descricao_similarity_idf.py \
    tests/test_text_normalizacao_descricao.py \
    tests/test_particionamento_fiscal.py \
    tests/test_inverted_index_descricao.py \
    -q

# Suite completa do projeto
PYTHONPATH=src python3 -m pytest -q
```

## Estilo Python

- Type hints em todas as funcoes publicas e privadas novas.
- Dataclasses `frozen=True` quando representam valor imutavel.
- Sem `from __future__ import annotations` em arquivos novos a
  menos que o arquivo ja use (manter consistencia local).
- **Linha de fim CRLF** no arquivo
  `descricao_similarity_service.py` ja existente. Editores que
  convertem para LF criam diff inteiro - configurar `git config
  core.autocrlf false` antes de editar.
- `print()` apenas para CLI/scripts. Codigo de producao usa
  `structlog`:
  ```python
  import structlog
  _LOG = structlog.get_logger(__name__)
  _LOG.info("evento_estruturado", chave=valor, ...)
  ```

## Filosofia "ordenar != agrupar"

Toda funcionalidade de similaridade neste codigo apenas
**reordena** o DataFrame e adiciona colunas indicadoras. **Nunca**:

- altera `id_agrupado`,
- chama `agregar_linhas()`,
- salva parquet automaticamente,
- remove linhas.

A decisao de agrupar e sempre humana, no clique em "Agregar
Descricoes" da UI.

## Polars: padroes preferidos

- Operacoes vetorizadas via `with_columns`, `group_by`, `join`.
- Loops Python sobre `.iter_rows()` apenas quando estritamente
  necessario (ex.: calculos n-a-n com estado).
- `with_row_index("idx")` para preservar ordem original quando
  for preciso reordenar.
- `pl.UInt32` em colunas de indice (resultado de `with_row_index`).

## Comportamento default retrocompativel

Toda nova feature entra como **opt-in**: nova funcao publica, novo
kwarg com default que reproduz comportamento atual, ou nova flag em
config dict. **Nunca** mude o comportamento default sem teste
explicito que valide a nova expectativa.

## Quando estiver em duvida

- Sobre paths/imports: olhe um arquivo vizinho na mesma pasta.
- Sobre estilo de teste: olhe `tests/test_descricao_similarity_service.py`
  como referencia.
- Sobre estilo de commit: `git log --oneline -20` mostra o padrao.
- Sobre arquitetura/decisao de design: pergunte ao usuario antes
  de inventar.

## O que **nao** fazer sem pedir

- Adicionar dependencia ao `pyproject.toml`.
- Renomear arquivos ou funcoes publicas existentes.
- Alterar a UI grande (`src/interface_grafica/ui/main_window_impl.py`
  e gerada por Qt Designer; mudancas vao via patches em
  `src/interface_grafica/patches/`).
- Mexer em `id_agrupado`, agregacao automatica ou parquets fiscais.
