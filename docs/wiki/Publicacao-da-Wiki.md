# Como publicar esta Wiki no GitHub

As paginas desta wiki estao versionadas em `docs/wiki/` no repositorio principal.

A sincronizacao e feita de forma **automatica** via GitHub Actions sempre que houver um push na `main` que altere a pasta `docs/wiki/`.

## Por que duas copias?

| Local | Finalidade |
|---|---|
| `docs/wiki/*.md` no repositorio | Revisao por PR, historico versionado, rastreabilidade |
| `audit_pyside.wiki.git` | Exibicao na aba Wiki do GitHub |

As duas copias devem estar sincronizadas. Atualize sempre as duas ao fazer mudancas.

## Passos para a primeira publicacao

### 1. Inicializar o wiki.git (so na primeira vez)

O repositorio wiki do GitHub precisa ser inicializado pela interface web.

1. Acesse https://github.com/Enio-Telles/audit_pyside/wiki
2. Clique em **"Create the first page"**
3. Coloque o titulo `Home` e qualquer texto temporario
4. Clique em **Save Page**

Isso cria o repositorio `audit_pyside.wiki.git` no GitHub.

### 2. Clonar o wiki.git localmente

```bash
git clone https://github.com/Enio-Telles/audit_pyside.wiki.git wiki-local
cd wiki-local
```

### 3. Copiar os arquivos de docs/wiki/

No repositorio principal (`audit_pyside`), apos o merge do PR:

```bash
cp docs/wiki/*.md ../wiki-local/
rm -f ../wiki-local/README.md  # Opcional: o README do docs/wiki/ costuma ser interno
```

### 4. Fazer commit e push no wiki.git

```bash
cd ../wiki-local
git add .
git commit -m "docs: publicar wiki inicial — catalogo de tabelas e campos"
git push origin master
```

### 5. Verificar

Acesse https://github.com/Enio-Telles/audit_pyside/wiki e confirme que todas as paginas aparecem no sidebar.

## Como atualizar depois

Sempre que uma pagina da wiki for atualizada:

1. Editar o arquivo em `docs/wiki/` no repositorio principal
2. Abrir PR com a mudanca
3. Apos o merge na `main`, o workflow `Sincronizar Wiki` cuidara da publicacao.

## Regra de manutencao

- Nunca editar o wiki.git diretamente sem atualizar tambem `docs/wiki/` no repo principal
- O PR e o lugar certo para revisar mudancas antes de publicar
