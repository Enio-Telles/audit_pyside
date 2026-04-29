# Observability — Política de logging (proposta)

Propósito: documentar tratamento mínimo recomendado para dados sensíveis em logs,
com foco no uso do campo `cnpj` nos eventos de fallback adicionados por PR #163.

## Resumo da proposta

- **Classificação:** `cnpj` é um identificador sensível usado para rastreabilidade.
- **Ambientes não-produtivos:** mascarar ou pseudonimizar `cnpj` (ex.: preservar só 4 últimos dígitos).
- **Produção:** permitir registro do `cnpj` apenas quando necessário para auditoria/tracing,
  armazenando logs em plataforma com controle de acesso (RBAC) e retenção definida.
- **Retenção recomendada (sugestão):** definir retenção operativa (ex.: 90 dias) conforme
  política legal/ops da organização — confirmar com Security/Legal.
- **Acesso:** acesso a logs que contenham `cnpj` deve ser registrado e restrito a papéis
  autorizados (ex.: equipe de suporte/ops com justificativa).

## Máscara recomendada (non-prod)

Exemplo simples para mascaramento (pseudocódigo):

mask_cnpj(cnpj: str) -> str:
    # preserva últimos 4 caracteres, substitui o resto por X
    return 'X' * (len(cnpj) - 4) + cnpj[-4:]

Use máscaras em ambientes de desenvolvimento e staging por padrão.

## Ações necessárias / confirmação

1. **Ops / Security:** confirmar a janela de retenção apropriada e requisitos legais.
2. **Legal:** confirmar se o `cnpj` pode ser registrado em logs para auditoria e sob quais condições.
3. **Infra:** garantir que o pipeline de logs tenha RBAC e suporte masking/PII removal para não-prod.

### Checklist para aprovação

- [ ] Retenção definida (dias)
- [ ] Regras de acesso documentadas (quem pode ver logs com `cnpj`)
- [ ] Máscara aplicada por padrão em não-prod
- [ ] Observability consumers informados (namespaces / event schema)

Coloque aqui o resultado da confirmação (data/responsável) quando aprovado.
