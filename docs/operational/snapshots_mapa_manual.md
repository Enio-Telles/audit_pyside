# Snapshots do Mapa Manual de Agregação

Resumo operacional das cópias (snapshots) geradas quando o mapa manual
`mapa_agrupamento_manual_<cnpj>.parquet` é atualizado.

Localização
- Arquivos canônicos: `dados/CNPJ/<cnpj>/analises/produtos/mapa_agrupamento_manual_<cnpj>.parquet`
- Snapshots: `dados/CNPJ/<cnpj>/analises/produtos/snapshots/`
- Arquivo de auditoria de linhas sem match: `auditoria_mapa_agrupamento_manual_sem_match_<cnpj>.parquet`
- Histórico de eventos (log): `dados/CNPJ/<cnpj>/analises/produtos/log_agregacoes_<cnpj>.json`

Formato do snapshot
- Nome: `mapa_agrupamento_manual_<cnpj>_<YYYYMMDDTHHMMSS>.parquet`
- Gerados automaticamente antes de qualquer sobrescrita do mapa manual.

Objetivo
- Preservar o estado anterior do mapa manual para possibilitar rollback
  seguro e auditoria de mudanças.

Política de retenção (recomendação)
- Manter as últimas 10 snapshots por CNPJ e/ou por 180 dias, o que ocorrer
  primeiro. Essa política equilibra capacidade de recuperação e uso de disco.
- Implementação: pode ser executada como job de manutenção periódico que
  lista `snapshots/`, ordena por timestamp e remove os arquivos antigos.

Rollback (reversão)

1. Pela UI (Agregação tab):
   - Abra a aba Agregação no aplicativo PySide; clique em "Reverter Mapa Manual";
   - Escolha o snapshot desejado e confirme; o serviço restaurará o arquivo
     `mapa_agrupamento_manual_<cnpj>.parquet` e disparará reprocessamento.

2. Pela linha de comando (exemplo):

```powershell
cd \path\to\repo\audit_pyside
python -c "from interface_grafica.services.aggregation_service import ServicoAgregacao; ServicoAgregacao().reverter_mapa_manual('84654326000394', 'mapa_agrupamento_manual_84654326000394_20260420T123456.parquet')"
```

3. Automação / scripts: A função `reverter_mapa_manual(cnpj, snapshot_name=None)`
   aceita `snapshot_name=None` para reverter ao snapshot mais recente.

Auditoria e rastreabilidade
- Toda operação de snapshot e atualização do mapa grava um registro em
  `log_agregacoes_<cnpj>.json` com `tipo` e `timestamp`.
- A auditoria de linhas sem match é escrita em
  `auditoria_mapa_agrupamento_manual_sem_match_<cnpj>.parquet` quando aplicável.

Testes e verificação
- Testes unitários existentes:
  - `tests/test_salvar_mapa_manual.py` cobre gravação, auditoria, snapshots e rollback.
  - `tests/test_ui_reverter_mapa_manual.py` cobre a chamada pela UI.
- Para rodar os testes locais rapidamente (no root do repo):

```powershell
set PYTHONPATH=%CD%;%CD%\src
python -m pytest -q tests/test_salvar_mapa_manual.py::test_salvar_mapa_manual_snapshot_and_rollback
```

Operação segura
- Falhas no snapshot não impedem a gravação do novo mapa (snapshot é
  tentativa conservadora). Entretanto, recomenda-se monitorar o log de
  erros do job para garantir que snapshots estejam sendo criados.

Observações
- A retenção e política de limpeza não estão implementadas automaticamente
  pelo `ServicoAgregacao` — sugere-se um job cron/Task Scheduler que execute
  a limpeza conforme a política de retenção descrita.

## Inspecionar artifacts do workflow

O workflow `Cleanup manual-map snapshots` (arquivo `.github/workflows/cleanup-snapshots.yml`) produz dois artifacts principais por execução:

- `cleanup-report` — arquivo de texto (`cleanup-report.txt`) com o stdout/stderr completo do run;
- `cleanup-removed-snapshots` — arquivo JSON (`cleanup-removed-snapshots.json`) com detalhes por CNPJ sobre arquivos removidos/retidos (campos: `removed`, `removed_files`, `kept_files`, `would_remove_files`).

Como baixar e inspecionar (GitHub Web UI):

1. Acesse o repositório no GitHub: https://github.com/Enio-Telles/audit_pyside
2. Clique em **Actions** e selecione o workflow "Cleanup manual-map snapshots" (ou o arquivo `cleanup-snapshots.yml`).
3. Na lista de execuções, abra a execução desejada (pela data/hora).
4. Na página da execução, localize a seção **Artifacts** e clique em `cleanup-report` ou `cleanup-removed-snapshots` para baixar.
5. Abra `cleanup-report.txt` para revisar logs; abra `cleanup-removed-snapshots.json` para analisar o resumo por CNPJ.

Como baixar via `gh` CLI (exemplo):

```bash
# listar últimas execuções do workflow
gh run list --workflow cleanup-snapshots.yml --repo Enio-Telles/audit_pyside --limit 5

# supondo que o run id seja 123456789, baixar o JSON do run
gh run download 123456789 --name cleanup-removed-snapshots --repo Enio-Telles/audit_pyside --dir ./artifacts

# baixar também o relatório de logs
gh run download 123456789 --name cleanup-report --repo Enio-Telles/audit_pyside --dir ./artifacts
```

Exemplo simplificado do conteúdo JSON (resumo):

```json
{
  "99999999000107": {
    "removed": 7,
    "removed_files": [
      "dados/CNPJ/99999999000107/analises/produtos/snapshots/mapa_agrupamento_manual_99999999000107_20260101T000000.parquet"
    ],
    "kept_files": [
      "dados/CNPJ/99999999000107/analises/produtos/snapshots/mapa_agrupamento_manual_99999999000107_20260401T000000.parquet"
    ],
    "would_remove_files": []
  }
}
```

Observações importantes:

- Artifacts expiram por padrão (normalmente 90 dias). Baixe ou armazene os relatórios se precisar retenção mais longa.
- Para execuções manuais (`workflow_dispatch`) use `dry_run=true` para validar sem remover nada — o JSON conterá `would_remove_files`.
- Se desejar, podemos estender o workflow para publicar os JSONs em um bucket (S3/Blob) ou anexá-los automaticamente a uma issue/relatório.
