# Documentação Técnica — audit_pyside

Este arquivo é o **índice vivo** de toda a documentação técnica do repositório.
Sempre que um documento for criado, movido ou removido, este índice deve ser atualizado no mesmo PR.

O projeto é uma **aplicação desktop Python/PySide6** com pipeline analítico-fiscal.
Para regras de agente e convenções de desenvolvimento, veja [`AGENTS.md`](../AGENTS.md) na raiz.
O backend FastAPI foi removido em 2026-04-22 — veja [ADR-001](adr/0001-futuro-backend-fastapi.md) e o [anexo de auditoria](adr/0001-annex-consumers-audit.md).

---

## Arquitetura

| Camada | Pasta | AGENTS.md responsável | Doc canônica |
|---|---|---|---|
| Pipeline (raw → marts) | `src/transformacao/` | [`src/transformacao/AGENTS.md`](../src/transformacao/AGENTS.md) | [Catálogo de tabelas](tabelas/README.md) |
| Interface PySide6 | `src/interface_grafica/` | [`src/interface_grafica/AGENTS.md`](../src/interface_grafica/AGENTS.md) | — |
| SQL / extração | `sql/` | `AGENTS.md` (raiz) | [scripts_usage.md](scripts_usage.md) |
| Testes | `tests/` | `AGENTS.md` (raiz) | — |
| Documentação | `docs/` | `AGENTS.md` (raiz) | Este arquivo |

---

## Documentos vivos

### Fiscal & regras de negócio

| Documento | Descrição |
|---|---|
| [conversao_unidades.md](conversao_unidades.md) | Regras de conversão de unidades (`q_conv`, `q_conv_fisica`), fatores e casos especiais |
| [agregacao_produtos_canonico.md](agregacao_produtos_canonico.md) | Regras canônicas de agrupamento de produtos (`id_agrupado`) |
| [mov_estoque.md](mov_estoque.md) | Movimentação de estoque — eventos, saldos, C170/NF-e |
| [abordagem_qconv_fisica.md](abordagem_qconv_fisica.md) | Detalhamento da abordagem para `q_conv_fisica` |
| [tabelas/c176_xml.md](tabelas/c176_xml.md) | Schema e regras do Registro C176 (PMU) |
| [tabelas/movimentacao_estoque.md](tabelas/movimentacao_estoque.md) | Schema da tabela de movimentação de estoque |
| [tabelas/fatores_conversao.md](tabelas/fatores_conversao.md) | Schema dos fatores de conversão |

> **Gaps de documentação:**
> - `TODO`: `docs/auditoria_conversao_agregacao_estoque.md` — documento de auditoria
>   consolidado de conversão, agregação e estoque ainda não existe. Criá-lo em P1-04/P2.
> - `TODO`: `docs/plano_melhorias_backend_frontend_arquitetura.md` — plano de melhorias
>   consolidado ainda não existe como arquivo canônico (conteúdo disperso em `docs/PLAN.md`
>   e `docs/archive/`). Consolidar em P2.
> - `TODO`: Regras C176/PMU/inventário unificadas em um único doc fiscal.

### Catálogo de tabelas Parquet

| Documento | Descrição |
|---|---|
| [tabelas/README.md](tabelas/README.md) | Índice do catálogo de tabelas |
| [tabelas/produtos_final.md](tabelas/produtos_final.md) | Schema de `produtos_final_{cnpj}.parquet` |
| [tabelas/descricao_produtos.md](tabelas/descricao_produtos.md) | Schema de `descricao_produtos` |
| [tabelas/fontes_produtos.md](tabelas/fontes_produtos.md) | Schema de fontes de produtos |
| [tabelas/calculos_mensais.md](tabelas/calculos_mensais.md) | Schema dos cálculos mensais |
| [tabelas/calculos_anuais.md](tabelas/calculos_anuais.md) | Schema dos cálculos anuais |
| [tabelas/c170_xml.md](tabelas/c170_xml.md) | Schema do C170 |
| [tabelas/itens.md](tabelas/itens.md) | Schema da tabela de itens |
| [tabelas/item_unidades.md](tabelas/item_unidades.md) | Schema de item × unidades |
| [tabelas/tb_documentos.md](tabelas/tb_documentos.md) | Schema da tabela de documentos |

### Metodologia e análise

| Documento | Descrição |
|---|---|
| [metodologia_mds_plan.md](metodologia_mds_plan.md) | Plano da Metodologia MDS |
| [analise_metodologia_mds_runtime_2026-04-20.md](analise_metodologia_mds_runtime_2026-04-20.md) | Análise de runtime da MDS (2026-04-20) |
| [tabela_mensal.md](tabela_mensal.md) | Estrutura da tabela mensal |
| [tabela_anual.md](tabela_anual.md) | Estrutura da tabela anual |
| [tabela_periodo.md](tabela_periodo.md) | Estrutura da tabela por período |

### Arquitetura & plano

| Documento | Descrição |
|---|---|
| [PLAN.md](PLAN.md) | Plano de execução P0–P5 (fonte de verdade do roadmap) |
| [plano_q.md](plano_q.md) | Plano de melhorias da qualidade dos dados |
| [ADR-001](adr/0001-futuro-backend-fastapi.md) | Decisão sobre o futuro do backend FastAPI |

> **Gap de documentação:**
> - `TODO`: `docs/plano_melhorias_backend_frontend_arquitetura.md` — consolidar a partir
>   de `PLAN.md` + `docs/archive/Plano de Melhorias - Arquitetura.md`.

### Operações & runbooks

| Documento | Descrição |
|---|---|
| [scripts_usage.md](scripts_usage.md) | Como usar os scripts em `scripts/` (generate_parquet_references, generate_output_samples, etc.) |
| [codex_usage.md](codex_usage.md) | Como usar o Codex com este repositório |
| [operational/snapshots_mapa_manual.md](operational/snapshots_mapa_manual.md) | Runbook de snapshots do mapa manual |
| [branch_consolidation_2026-04-23.md](branch_consolidation_2026-04-23.md) | Registro da consolidação de branches em `main` |
| [branch_cleanup.md](branch_cleanup.md) | Procedimento de limpeza de branches |
| [PR_followups.md](PR_followups.md) | Follow-ups e débitos técnicos de PRs anteriores |

> **Gap de documentação:**
> - `TODO`: `docs/runbook_sync_repo.md` — runbook de sincronização do repositório ainda
>   não existe como arquivo canônico. Criar em P2.

### Referências normativas

| Documento / Arquivo | Descrição |
|---|---|
| [referencias/fatores_conversao_unidades.md](referencias/fatores_conversao_unidades.md) | Tabela de fatores de conversão de unidades |
| `referencias/Guia Prático EFD - Versão 3.2.1.pdf` | Guia prático EFD (SPED) |
| `referencias/MOC_CTe_VisaoGeral_v4.00.pdf` | Manual de Orientação ao Contribuinte — CT-e |
| `referencias/Manual de Orientação ao Contribuinte - MOC - versão 7.0 - NF-e e NFC-e.pdf` | MOC NF-e e NFC-e v7.0 |
| `referencias/Manual_CTe_v2_0.pdf` | Manual CT-e v2.0 |
| `referencias/moc7-anexo-i-leiaute-e-rv (2).pdf` | MOC v7 Anexo I — Leiaute e Regras de Validação |

### Análises e diagnósticos

| Documento | Descrição |
|---|---|
| [analise_audit_pyside.md](analise_audit_pyside.md) | Análise geral do projeto |
| [agente_audit_pyside.md](agente_audit_pyside.md) | Descrição do agente audit_pyside |
| [agente_audit_pyside_pyside_only.md](agente_audit_pyside_pyside_only.md) | Configuração do agente PySide-only |

---

## Build do executável (PyInstaller)

O empacotamento usa **PyInstaller 6+** via `audit_pyside.spec` na raiz do repositório.
O spec gera uma distribuição **one-dir** (`dist/FiscalParquetAnalyzer/`) pronta para distribuição Windows.

### Release automatizado com assinatura de código (GitHub Actions)

O workflow `.github/workflows/release-windows.yml` é disparado automaticamente em pushes de tags `v*`.
Ele executa as seguintes etapas em `windows-latest`:

1. Instala dependências com `uv sync`.
2. Deriva a versão numérica a partir da tag (ex.: `v1.2.3` → `1.2.3.0`).
3. Gera um arquivo de recurso VERSIONINFO do Windows e embute a versão no `.exe`.
4. Compila o bundle com `pyinstaller audit_pyside.spec --clean --noconfirm`.
5. **Assina** `FiscalParquetAnalyzer.exe` com `signtool.exe` usando o certificado PFX configurado nos secrets.
6. Empacota o bundle assinado em `FiscalParquetAnalyzer-<versao>-windows.zip`.
7. Faz o upload do zip como asset da **GitHub Release** correspondente à tag.

#### Secrets obrigatórios

Configure os seguintes secrets no repositório (Settings → Secrets and variables → Actions):

| Secret | Descrição |
|---|---|
| `WINDOWS_CERT_PFX_BASE64` | Certificado de code-signing PKCS#12 (.pfx) codificado em Base64. Gere com: `[Convert]::ToBase64String([IO.File]::ReadAllBytes("cert.pfx"))` no PowerShell. |
| `WINDOWS_CERT_PASSWORD` | Senha que protege o arquivo `.pfx`. |

> **Como gerar o Base64 do certificado no PowerShell:**
> ```powershell
> [System.Convert]::ToBase64String(
>     [System.IO.File]::ReadAllBytes("C:\caminho\para\cert.pfx")
> ) | Set-Clipboard
> ```
> Cole o conteúdo copiado no valor do secret `WINDOWS_CERT_PFX_BASE64`.

> **Nota de segurança:** O arquivo PFX temporário é gravado em `${{ runner.temp }}` durante a
> assinatura e removido imediatamente após, mesmo em caso de falha (step `Remove certificate file`
> usa `if: always()`). Nunca commite o arquivo `.pfx` diretamente no repositório.

#### Como publicar uma release

```bash
# Criar e empurrar a tag de versão
git tag v1.0.0
git push origin v1.0.0
```

O workflow `release-windows.yml` será disparado automaticamente.
Se a GitHub Release não existir, ela será criada pela action com os assets assinados.

### Pré-requisitos

```bash
# Instalar dependências de desenvolvimento (inclui pyinstaller)
uv sync --all-extras
```

### Comando de build

```bash
uv run pyinstaller audit_pyside.spec --clean --noconfirm
# Output: dist/FiscalParquetAnalyzer/FiscalParquetAnalyzer.exe (Windows)
#         dist/FiscalParquetAnalyzer/FiscalParquetAnalyzer     (Linux/macOS)
```

O executável final estará em `dist/FiscalParquetAnalyzer/FiscalParquetAnalyzer.exe`.

### Como testar o bundle

```bash
# Windows
./dist/FiscalParquetAnalyzer/FiscalParquetAnalyzer.exe

# Linux/macOS (headless smoke test)
AUDIT_LOG_JSON=1 ./dist/FiscalParquetAnalyzer/FiscalParquetAnalyzer 2>&1 | head -20

# Verificar ausência de ImportError/FileNotFoundError na saída de erro
```

### Transição para o executável

**Pré-requisitos:**

- Python 3.11+ e `uv` instalados no sistema alvo de build.
- `uv sync --group dev` para instalar o PyInstaller e demais dependências de dev.
- Sistema operacional do build deve coincidir com o alvo (bundle Windows não roda em Linux).

**Passos:**

1. Clone o repositório e sincronize as dependências:

   ```bash
   git clone https://github.com/Enio-Telles/audit_pyside.git
   cd audit_pyside
   uv sync --group dev
   ```

2. Gere o bundle:

   ```bash
   uv run pyinstaller audit_pyside.spec --clean --noconfirm
   ```

3. Copie os diretórios de runtime para ao lado do diretório gerado:

   ```bash
   cp -r dados/ sql/ workspace/ .env dist/FiscalParquetAnalyzer/
   ```

4. Execute e valide que a GUI abre e a conexão Oracle funciona.

> **Nota:** UPX está explicitamente **desabilitado** no spec (`upx=False`). Em alguns ambientes,
> UPX comprimido interage mal com as DLLs do PySide6/Qt e causa falhas na inicialização ou
> falsos-positivos em antivírus.

### Rollback

Para voltar a executar via Python diretamente:

```bash
# Sem bundle — modo de desenvolvimento
uv run python app.py
```

Se o bundle falhar ao iniciar (`ImportError`, `FileNotFoundError`, ou janela vazia):

1. Limpe artefatos de build anteriores:

   ```bash
   rm -rf build/ dist/ __pycache__/
   ```

2. Rebuild com flag de depuração:

   ```bash
   uv run pyinstaller audit_pyside.spec --clean --noconfirm --debug all
   ```

3. Analise o log de boot para o módulo faltante e adicione em `hiddenimports` ou `datas` no [audit_pyside.spec](../audit_pyside.spec).

**Quando NÃO usar o bundle:**

- Durante desenvolvimento ativo em `src/transformacao/` (o bundle é snapshot estático).
- Ao depurar exceções fiscais (use `uv run python app.py` para traceback completo).
- Ao alterar SQLs em `sql/` sem rebuild.

### Estrutura de runtime esperada

O bundle **não** inclui dados externos. Antes de executar, posicione ao lado do diretório `FiscalParquetAnalyzer/`:

```text
FiscalParquetAnalyzer/   ← gerado pelo build
dados/                   ← dados Parquet e referências
sql/                     ← scripts SQL de extração
workspace/               ← estado da aplicação (criado automaticamente)
.env                     ← credenciais Oracle (não commitar)
```

### Notas de path resolution

O módulo `src/utilitarios/project_paths.py` deriva `PROJECT_ROOT` de `Path(__file__).parents[2]`.
Dentro do bundle one-dir, `PROJECT_ROOT` aponta para `sys._MEIPASS` (diretório de extração temporário).
Para instalações portáteis, mantenha `dados/`, `sql/`, `workspace/` e `.env` no mesmo diretório que
o executável, ou defina as variáveis de ambiente `DATA_ROOT` e `SQL_ROOT` antes do lançamento.

### Solução de problemas comuns

| Sintoma | Causa provável | Solução |
|---|---|---|
| `ModuleNotFoundError: polars` | Hook polars não detectado | Adicionar `polars._utils.udfs` em `hiddenimports` no spec |
| Tema não carrega (janela sem estilo) | QSS não incluído no bundle | Verificar entrada `datas` no spec |
| `oracledb` falha ao conectar | Biblioteca nativa faltando | Usar modo thin (padrão); modo thick requer DLL do Oracle Client |

---

## Como contribuir

### Nomenclatura de branches

| Tipo | Padrão | Exemplo |
|---|---|---|
| Feature | `feat/<modulo>-<objetivo>` | `feat/estoque-saldo-inicial` |
| Fix | `fix/<modulo>-<problema>` | `fix/conversao-unidade-nula` |
| Docs | `docs/<tema>` | `docs/adr-backend` |
| Chore / infra | `chore/<escopo>` | `chore/p1-consolidacao-docs` |
| Refactor | `refactor/<modulo>-<escopo>` | `refactor/main-window-decompose` |

### AGENTS por escopo

Antes de contribuir em uma área, leia o AGENTS.md correspondente:

- **Pipeline / transformação** → [`src/transformacao/AGENTS.md`](../src/transformacao/AGENTS.md)
- **GUI PySide6** → [`src/interface_grafica/AGENTS.md`](../src/interface_grafica/AGENTS.md)
- **Qualquer outra área** → [`AGENTS.md`](../AGENTS.md) (raiz)

## Transição

| Origem anterior | Situação em P1-01 | Destino canônico atual |
|---|---|---|
| `.agent.md` | Removido | [`AGENTS.md`](../AGENTS.md) |
| `docs/AGENTS.md` | Removido | [`AGENTS.md`](../AGENTS.md) |
| `tests/AGENTS.md` | Removido | [`AGENTS.md`](../AGENTS.md) |
| Escopo de pipeline | Mantido como escopo canônico | [`src/transformacao/AGENTS.md`](../src/transformacao/AGENTS.md) |
| Escopo de GUI PySide6 | Mantido como escopo canônico | [`src/interface_grafica/AGENTS.md`](../src/interface_grafica/AGENTS.md) |
| Backend stub | Removido em P2 (ADR-001 Opção B) | [`AGENTS.md`](../AGENTS.md) + [ADR-001](adr/0001-futuro-backend-fastapi.md) |

### Regras de PR

- Nunca commitar direto na `main`.
- PRs devem ter escopo único e ser revisáveis em uma sessão.
- Não misturar refatoração ampla com correção de regra fiscal.
- Descrição da PR: **Objetivo / Contexto / Reaproveitamento / Arquitetura / Implementação / Validação / Riscos / MVP**.
- Mudanças em schema Parquet, chaves de join, conversão ou estoque requerem seção explícita de **Riscos e Rollback**.
- ADRs para decisões arquiteturais significativas: `docs/adr/NNNN-kebab-case.md`.

---

## Status atual (P0–P5)

> Fonte de verdade: [`docs/PLAN.md`](PLAN.md). Esta seção apenas espelha o estado de alto nível.

| Fase | Descrição | Status |
|---|---|---|
| P0 | Estabilização da base (limpeza, compat, CI básico) | Em andamento |
| P0-04 | Limpeza de artefatos obsoletos (copy dirs, tmp, patch files) | ✅ Concluído (PR atual) |
| P1 | Consolidação de docs e AGENTS | Em andamento (PR atual) |
| P1-01 | Consolidação de AGENTS.md (11 → 4) | ✅ Concluído (PR atual) |
| P1-02 | Índice mestre `docs/README.md` | ✅ Concluído (PR atual) |
| P1-03 | ADR-001 draft (futuro do backend) | ✅ Concluído (PR atual) |
| P1-04/05/06 | pyproject+uv, ruff+mypy+pre-commit, CI | Concluído (PR atual) |
| P2 | Remoção do backend FastAPI (ADR-001 Opção B) | Em andamento (PR atual) |
| P3 | Decomposição de `main_window.py` | Planejado |
| P4 | Multi-tenant / autenticação | Não iniciado |
| P5 | Performance e escalabilidade | Não iniciado |

---

## Gaps de documentação (TODO)

- `docs/auditoria_conversao_agregacao_estoque.md` — doc consolidado de auditoria de conversão, agregação e estoque.
- `docs/plano_melhorias_backend_frontend_arquitetura.md` — plano de melhorias consolidado de arquitetura.
- `docs/runbook_sync_repo.md` — runbook de sincronização do repositório.
- ADRs adicionais para decisões de P3 (decomposição GUI) e P4 (autenticação).
