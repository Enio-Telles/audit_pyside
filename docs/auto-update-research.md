# Pesquisa: Auto-update do bundle PyInstaller Windows

**Data:** 2026-05-01
**Escopo:** P7-AUTOUPDATE
**Status:** Rascunho para decisao arquitetural

---

## Contexto

O `audit_pyside` e distribuido como bundle PyInstaller one-dir para Windows em `dist/FiscalParquetAnalyzer/`. Hoje a atualizacao exige substituicao manual do diretorio do aplicativo, o que aumenta o risco de versoes desatualizadas em campo.

Este documento compara alternativas de auto-update. Nao propoe mudanca de codigo de producao.

---

## Criterios de comparacao

| Criterio | Peso | Motivo |
|---|---|---|
| Esforco de implementacao | Alto | Afeta release, suporte e rollback. |
| Assinatura de codigo | Alto | Evita alertas do SmartScreen/Defender. |
| Preservacao de dados | Alto | `dados/`, `workspace/`, `sql/` e `.env` nao podem ser sobrescritos. |
| Update silencioso | Medio | Reduz friccao operacional. |
| Rollback | Medio | Permite recuperar versao anterior. |
| Dependencias externas | Baixo | Quanto menor, mais facil auditar e operar. |

---

## Opcao 1 — PyUpdater

PyUpdater integra com PyInstaller para empacotar, assinar e distribuir atualizacoes.

| Criterio | Avaliacao |
|---|---|
| Esforco | Alto: exige migrar parte do fluxo de build/release e manter configuracao propria. |
| Assinatura | Parcial: assina pacotes, mas nao substitui Authenticode do Windows. |
| Update silencioso | Parcial: download pode ser em background; troca exige app fechado. |
| Rollback | Limitado/manual. |
| Dependencias | Biblioteca + hospedagem de artefatos. |

Riscos especificos:

- Pode aumentar a complexidade do CI de release.
- Precisa preservar diretorios externos ao bundle.
- Nao elimina a necessidade de certificado Code Signing.

---

## Opcao 2 — GitHub Releases Poller customizado

Implementar um worker leve que consulta a ultima release do GitHub, compara com a versao embutida e oferece download/restart quando houver versao nova.

| Criterio | Avaliacao |
|---|---|
| Esforco | Medio: poller simples; substituicao segura no Windows e a parte critica. |
| Assinatura | Nao incluida: exige Authenticode separado. |
| Update silencioso | Parcial: download pode ser silencioso; swap exige restart. |
| Rollback | Implementavel guardando versao anterior. |
| Dependencias | Baixas: GitHub Releases + cliente HTTP. |
| Auditoria | Alta: codigo sob controle do projeto. |

Riscos especificos:

- Windows nao substitui executavel em uso.
- Proxy/firewall corporativo pode bloquear chamadas ao GitHub.
- Update deve aguardar estado idle para nao interromper escrita de Parquet/exportacao.
- O mecanismo deve preservar `dados/`, `workspace/`, `sql/` e `.env`.

---

## Opcao 3 — MSIX / Windows Package Manager

Empacotar o app como MSIX e distribuir por winget, Store ou canal corporativo.

| Criterio | Avaliacao |
|---|---|
| Esforco | Muito alto: requer manifest, assinatura, pipeline MSIX e revisao de paths. |
| Assinatura | Obrigatoria. |
| Update silencioso | Excelente: gerenciado pelo Windows. |
| Rollback | Excelente: suporte nativo. |
| Compatibilidade atual | Baixa: sandbox MSIX conflita com paths livres atuais. |

Riscos especificos:

- Pode exigir migrar paths para `%LOCALAPPDATA%`/`%APPDATA%`.
- Mudanca de path resolution merece ADR proprio.
- Exige certificado e pipeline Windows mais formal.

---

## Comparativo resumido

| Criterio | PyUpdater | GitHub Releases Poller | MSIX |
|---|---|---|---|
| Esforco inicial | Alto | Medio | Muito alto |
| Manutencao | Media | Baixa | Alta |
| Authenticode | Necessario | Necessario | Obrigatorio |
| Update silencioso | Parcial | Parcial | Sim |
| Rollback | Limitado | Implementavel | Nativo |
| Compatibilidade com layout atual | Alta | Alta | Baixa |
| Controle do codigo | Medio | Alto | Baixo |

---

## Recomendacao preliminar

Para o estagio atual, a opcao mais adequada e **GitHub Releases Poller customizado**.

Motivos:

- Mantem controle total do codigo.
- Tem menor dependencia externa.
- E compativel com o layout PyInstaller one-dir atual.
- Permite evoluir de forma incremental, sem mexer em regra fiscal ou pipeline.

PyUpdater pode ser reavaliado se houver necessidade de updates diferenciais. MSIX so deve ser considerado se houver decisao de distribuicao Windows gerenciada e revisao de arquitetura de paths.

---

## Pre-requisitos comuns

- Definir `__version__` versionado e embutido no bundle.
- Adquirir certificado Code Signing antes de distribuicao automatica.
- Garantir que `dados/`, `workspace/`, `sql/` e `.env` nunca sejam sobrescritos.
- Criar ADR antes da implementacao.

---

## Proximos passos se aprovado

1. Abrir ADR para decisao de auto-update.
2. Definir versao embutida no bundle.
3. Criar branch `feat/p7-autoupdate-github-poller`.
4. Implementar worker PySide6 de verificacao de release.
5. Implementar script auxiliar de troca apos fechamento do app.
6. Documentar procedimento de release em `docs/operational/`.
