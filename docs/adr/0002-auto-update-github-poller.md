# ADR-002: Auto-update via GitHub Releases Poller

| Campo | Valor |
|---|---|
| **Status** | Accepted |
| **Data** | 2026-05-15 |
| **Decision date** | 2026-05-15 |
| **Deciders** | Enio Carstens Telles (owner) |
| **PR de origem** | feat/p7-autoupdate-github-poller |

---

## Contexto

O `audit_pyside` e distribuido como bundle PyInstaller one-dir para Windows. Para facilitar a distribuicao de novas versoes e garantir que os usuarios utilizem a versao mais recente, e necessaria uma estrategia de auto-update que seja segura, preserve os dados do usuario e seja compativel com o layout atual.

## Drivers de decisão

1. **Segurança**: Verificacao de integridade (SHA-256) e assinatura de codigo (Authenticode).
2. **Preservação de Dados**: `dados/`, `workspace/`, `sql/` e `.env` devem ser preservados.
3. **Experiência do Usuário**: Consentimento explicito antes do download e rollback automatico em caso de falha.
4. **Facilidade de Manutenção**: Baixa dependencia de ferramentas externas complexas.

## Decisão

Implementar um **GitHub Releases Poller** customizado.

### Fluxo de Funcionamento

1. **Verificação**: Um worker `QThread` consulta o endpoint `GET /repos/Enio-Telles/audit_pyside/releases/latest` na inicializacao.
2. **Comparação**: A `tag_name` da release e comparada com a `__version__` embutida no app.
3. **Consentimento**: Se houver nova versao, um dialog exibe as notas de release e solicita confirmacao.
4. **Download**: O artefato `.zip` e baixado para um diretorio temporario.
5. **Integridade**: O hash SHA-256 do download e verificado contra um arquivo `checksums.txt` (asset da release).
6. **Swap (Troca)**:
   - O app gera um script `.bat` temporario.
   - O app solicita ao usuario para fechar ou reinicia automaticamente.
   - O script `.bat` aguarda o processo terminar.
   - Cria um backup `FiscalParquetAnalyzer.bak/`.
   - Extrai o novo bundle sobrescrevendo o atual, **exceto** pelos diretorios protegidos.
   - Tenta iniciar a nova versao.
7. **Rollback**: Se a nova versao falhar ao iniciar (exit code != 0), o script restaura o backup `.bak/`.

## Consequências

- **Prós**:
  - Controle total sobre o processo de update.
  - Compatibilidade garantida com a estrutura de pastas atual.
  - Minimo de dependencias externas.
- **Contras**:
  - Requer gestao manual de assets de release (upload do zip e checksums.txt).
  - Necessidade de gerenciar a execucao do script `.bat` externo.

## Referências

- `docs/auto-update-research.md`
- `docs/PLAN.md`
