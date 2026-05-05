# Procedimento de Release e Auto-update

Este documento descreve o processo de publicacao de uma nova versao do `audit_pyside` para que o mecanismo de auto-update funcione corretamente.

## Prerequisitos

1. Certificado Code Signing (Authenticode) instalado no ambiente de build.
2. Versao atualizada em `src/__init__.py`.

## Passo a Passo

### 1. Preparacao do Bundle

Gere o bundle PyInstaller:

```bash
uv run pyinstaller audit_pyside.spec --clean --noconfirm
```

### 2. Assinatura Digital

Assine o executavel principal e as DLLs criticas:

```powershell
signtool sign /f seu_certificado.pfx /p sua_senha /tr http://timestamp.digicert.com /td sha256 /fd sha256 "dist\FiscalParquetAnalyzer\FiscalParquetAnalyzer.exe"
```

### 3. Empacotamento

Crie um arquivo `.zip` com o conteudo da pasta `dist/FiscalParquetAnalyzer/`.

**Nota:** Nao inclua `dados/`, `workspace/`, `sql/` ou `.env` no zip se eles existirem no seu ambiente de dev. O bundle deve conter apenas os binarios e recursos internos.

### 4. Geracao de Checksums

Gere o hash SHA-256 do arquivo zip:

```powershell
CertUtil -hashfile FiscalParquetAnalyzer.zip SHA256 > checksums.txt
```

Formate o `checksums.txt` para conter apenas o hash:
```
<hash_sha256>
```

### 5. Publicacao no GitHub

1. Vá para a página de Releases do repositório.
2. Crie um novo Draft Release.
3. A `tag_name` deve ser a versao definida em `src/__init__.py` (ex: `v0.2.0`).
4. Faça o upload do `FiscalParquetAnalyzer.zip` e do `checksums.txt` como assets.
5. Publique a release.

## Mecanismo de Rollback

O auto-update cria automaticamente um backup em `FiscalParquetAnalyzer.bak/` antes de aplicar a nova versao.
Se a nova versao falhar ao iniciar, o script `.bat` executara o rollback restaurando a pasta `.bak/`.

Para rollback manual:
1. Feche o aplicativo.
2. Renomeie `FiscalParquetAnalyzer/` para `FiscalParquetAnalyzer.broken/`.
3. Renomeie `FiscalParquetAnalyzer.bak/` para `FiscalParquetAnalyzer/`.
4. Inicie o aplicativo.
