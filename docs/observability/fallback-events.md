# Observability — Eventos de fallback

Este documento descreve o schema dos eventos de observabilidade (structured logging)
introduzidos para tratar caminhos de *fallback* no processamento fiscal (PR #163).

## Visão geral

Os eventos são emitidos via `structlog` como eventos estruturados. A configuração
global de logging (`configure_structlog()`) determina campos adicionais (timestamp,
level, host, etc.). Aqui documentamos os campos aplicados por estes eventos para
facilitar a integração com consumidores de logs/telemetria.

---

## `fatores_conversao.fallback`

Descrição: evento agregado por `fator_origem` emitido quando a estratégia principal
de obtenção de fatores de conversão não encontra dados suficientes e um caminho de
fallback foi aplicado.

Campos (obrigatórios):

- `event` (string): `fatores_conversao.fallback`
- `motivo` (string): razão/resumo do fallback aplicado (ex.: `missing_master`, `join_empty`)
- `fator_origem` (string): origem do fator que disparou o fallback (agregado)
- `n_linhas` (int): número de linhas afetadas/contadas para este `fator_origem`
- `cnpj` (string): CNPJ do cliente/empresa processada (ver `docs/observability/logging-policy.md`)

Exemplo JSON:

{
  "event": "fatores_conversao.fallback",
  "motivo": "missing_master",
  "fator_origem": "tabela_produtos_x",
  "n_linhas": 42,
  "cnpj": "12.345.678/0001-90"
}

---

## `mov_estoque.fallback`

Descrição: evento emitido quando `apply_conversion_factors` falha durante o
processamento de movimentação de estoque e é necessário registrar o fallback.

Campos (obrigatórios):

- `event` (string): `mov_estoque.fallback`
- `motivo` (string): razão fixa usada no código (ex.: `apply_conversion_factors_falhou`)
- `exc_type` (string): nome do tipo de exceção que ocorreu (ex.: `TypeError`, `ValueError`)
- `cnpj` (string): CNPJ do cliente/empresa processada (ver `docs/observability/logging-policy.md`)

Exemplo JSON:

{
  "event": "mov_estoque.fallback",
  "motivo": "apply_conversion_factors_falhou",
  "exc_type": "TypeError",
  "cnpj": "12.345.678/0001-90"
}

---

Notas:

- Os consumidores de logs devem tratar `n_linhas` como inteiro e `cnpj` como
  identificador (string). Se a plataforma de ingestão acrescentar `timestamp`
  ou `level`, esses campos são complementares.
- Evitar mudanças retroativas nos nomes de chave; se for necessário, adotar
  versão do schema (ex.: `schema_version`) para compatibilidade.

Para dúvidas sobre armazenamento e exposição de `cnpj`, ver `docs/observability/logging-policy.md`.
