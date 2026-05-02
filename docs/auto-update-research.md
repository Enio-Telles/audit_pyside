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

**Restricao com layout PyInstaller one-dir atual:**
PyUpdater espera controlar o ciclo de build completo (substituindo `pyinstaller audit_pyside.spec` por `pyupdater build`). O spec atual usa `one-dir` com `upx=False` e inclui varios hooks customizados para polars, PySide6 e oracledb. Migrar para o fluxo do PyUpdater exige revalidar todos esses ajustes no novo contexto. Alem disso, o mecanismo de update do PyUpdater substitui archives comprimidos, enquanto o layout atual distribui um diretorio solto com `dados/`, `sql/`, `workspace/` e `.env` ao lado — estrutura que o PyUpdater nao gerencia nativamente. Custo de compatibilidade estimado: 1–2 dias extras sobre o esforco base.

---

## GitHub Releases Poller

Implementar um worker leve que consulta a ultima release do GitHub, compara com a versao embutida e oferece download/restart quando houver versao nova.

**Endpoint utilizado:**
```
GET https://api.github.com/repos/Enio-Telles/audit_pyside/releases/latest
```
A resposta contem `tag_name` (versao da release) e `assets` (lista de artefatos com URL de download). Sem token, o rate limit e 60 req/hora por IP — suficiente para uso normal em ambiente corporativo.

| Criterio | Avaliacao |
|---|---|
| Esforco | Medio: poller simples; substituicao segura no Windows e a parte critica. |
| Assinatura | Nao incluida: exige Authenticode separado. |
| Update silencioso | Parcial: download pode ser silencioso; swap exige restart. |
| Rollback | Implementavel guardando versao anterior. |
| Dependencias | Baixas: GitHub Releases + cliente HTTP. |
| Auditoria | Alta: codigo sob controle do projeto. |

**Fluxo de update — passos obrigatorios:**

1. Worker `QThread` consulta o endpoint na inicializacao (timeout de 5s; falha silenciosa sem bloquear a UI).
2. Compara `tag_name` da resposta com `__version__` embutida no bundle.
3. Se houver versao nova, exibe dialog com descricao e solicita consentimento do usuario antes de qualquer download — veja [Fluxo de consentimento do usuario](#fluxo-de-consentimento-do-usuario).
4. Apos confirmacao, o worker faz download do artefato `.zip` para diretorio temporario.
5. Verifica integridade do artefato antes da troca — veja [Hash SHA-256 e assinatura Authenticode](#hash-sha-256-e-assinatura-authenticode).
6. O app exibe mensagem de "Reinicie o aplicativo para aplicar a atualizacao".
7. Ao fechar, um script auxiliar (`.bat` gerado dinamicamente no diretorio temporario) aguarda o processo principal encerrar, extrai o novo bundle sobre o diretorio atual — **preservando `dados/`, `workspace/`, `sql/` e `.env`** — e relanca o executavel.

Riscos especificos:

- Windows nao substitui executavel em uso.
- Proxy/firewall corporativo pode bloquear chamadas ao GitHub.
- Update deve aguardar estado idle para nao interromper escrita de Parquet/exportacao.
- O mecanismo deve preservar `dados/`, `workspace/`, `sql/` e `.env`.

---

## Hash SHA-256 e assinatura Authenticode

**Verificacao de integridade antes da troca:** o artefato baixado deve ter seu hash SHA-256 comparado com o valor publicado na release (campo `body` da release ou arquivo `checksums.txt` publicado como asset). Se o hash nao bater, o update e abortado e o usuario e informado.

**Prerequisito obrigatorio antes da implementacao:** adquirir certificado Code Signing (Authenticode). Sem ele, o `.exe` baixado sera bloqueado pelo SmartScreen independente da opcao escolhida.

---

## Fluxo de consentimento do usuario

Se houver versao nova: exibe dialog informativo com descricao da release e pergunta ao usuario se deseja baixar e aplicar — **o consentimento explicito do usuario e obrigatorio antes de qualquer download ou substituicao**.

---

## Rollback

**Caminho de rollback:**

- Antes da troca, o script auxiliar copia o diretorio `FiscalParquetAnalyzer/` atual para `FiscalParquetAnalyzer.bak/` no mesmo nivel.
- Se o novo bundle falhar ao iniciar (verificado por exit code do processo relanado), o script substitui `FiscalParquetAnalyzer/` pelo conteudo de `FiscalParquetAnalyzer.bak/` e exibe mensagem de erro.
- O usuario tambem pode restaurar manualmente renomeando o diretorio `.bak`.
- O diretorio `.bak` e removido somente apos o primeiro lancamento bem-sucedido da nova versao.

---

## Cobertura MSIX

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

## ADR em docs/adr/

**Viabilidade atual — bloqueio formal:**
A Opcao 3 (MSIX) **nao e viavel no estagio atual sem um ADR formal** que trate:
(a) a migracao da resolucao de paths em `src/utilitarios/project_paths.py` de `Path(__file__).parents[2]` para caminhos do perfil do usuario (`%LOCALAPPDATA%`, `%APPDATA%`);
(b) a compatibilidade do sandbox MSIX com os diretorios `dados/`, `sql/`, `workspace/` e `.env` posicionados ao lado do executavel;
(c) a estrategia de empacotamento MSIX em convivencia com o spec PyInstaller atual.
Sem esse ADR aprovado, qualquer trabalho em MSIX cria risco de regressao na distribuicao existente. Esta opcao deve permanecer como backlog de longo prazo (P4+).

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

PyUpdater pode ser reavaliado se houver necessidade de updates diferenciais. MSIX so deve ser considerado se houver decisao de distribuicao Windows gerenciada e ADR formal aprovado para mudanca de path resolution.

---

## Decisao recomendada

| Criterio | Opcao escolhida | Justificativa |
|---|---|---|
| Esforco | GitHub Releases Poller | Esforco medio; sem migracao de pipeline de build. |
| Controle do codigo | GitHub Releases Poller | 100% no repositorio; auditavel e depuravel sem dependencia de lib externa. |
| Seguranca | GitHub Releases Poller + Authenticode | Verificacao de hash SHA-256 antes da troca; exige certificado Code Signing para eliminar alertas SmartScreen. |
| Compatibilidade com PyInstaller one-dir | GitHub Releases Poller | Nenhuma mudanca no spec ou no layout de runtime; `dados/`, `workspace/`, `sql/` e `.env` preservados. |
| Dependencia de infra externa | GitHub Releases Poller | Apenas GitHub Releases (ja usado) + cliente HTTP; sem servidor de artefatos adicional. |

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
