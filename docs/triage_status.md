# Relatorio de Triagem de PRs e Seguranca - 2026-05-07

## PRs Classificadas

| PR # | Categoria | Status | Justificativa / Acao |
| :--- | :--- | :--- | :--- |
| **#189** | CI/GUI Windows | **FIXED** | Logica de skip para Windows CI validada em tests/ui/test_main_window_smoke.py. |
| **#227** | Seguranca | **FIXED** | Risco de Command Injection via 'explorer' mitigado com os.startfile. |
| **#230** | Seguranca | **FIXED** | Validacao de identificadores Oracle implementada em SqlService. |
| **#238** | Performance | **FIXED** | Corregida regressao de linhas vazias com valores None em tabelas HTML. |
| **#223** | Fiscal | **BLOCKED** | Toca _produtos_final_impl.py; exige gate fiscal completo (ADR + Diff Harness). |
| **#240** | Fiscal | **BLOCKED** | Exige gate fiscal completo antes do merge. |
| **#225** | Documentacao | **MERGE** | Higiene e batch de docstrings. |
| **#242** | Documentacao | **MERGE** | Higiene e batch de docstrings. |
| **#244** | Documentacao | **MERGE** | Higiene e batch de docstrings. |
| **#245** | Documentacao | **MERGE** | Higiene e batch de docstrings. |
| **#246** | Documentacao | **MERGE** | Higiene e batch de docstrings. |
| **#222** | CI/GUI | **DECIDE** | Decisao pendente entre #222 e #243 sobre localizacao do pytest-qt. |
| **#243** | CI/GUI | **DECIDE** | Decisao pendente sobre GUI smoke no Windows. |
| **#235** | Performance | **REVIEW** | Risco de schema divergente entre chunks; exige revisao. |
| **#247** | Performance | **REVIEW** | Necessita validacao de benchmarks. |
| **#248** | Performance | **REVIEW** | Necessita revisao humana da metodologia de benchmark. |
| **#233** | Qualidade | **REVIEW** | Testes duplicados com o mesmo nome identificados. |
| **#241** | Feature | **DRAFT** | Auto-update depende de politica de release e assinatura. |

## Status de Bloqueios Imediatos

- **#223:** Bloqueado ate aprovacao humana e Diff Harness real.
- **#230:** Implementado regex allowlist e constante de modulo. Testes desacoplados.
- **#227:** Fix de seguranca isolado de artefatos temporarios.
- **#222:** Aguardando decisao sobre jobs de GUI smoke no Windows.
- **#235:** Necessita verificar consistencia de schema entre lotes (chunks).
- **#238:** Lógica de filtragem de valores None/vazios implementada.
