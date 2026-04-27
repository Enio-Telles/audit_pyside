---
name: security-review
description: Revisa código e configuração com foco em riscos de segurança, exposição de segredos, permissões excessivas, validação insuficiente e práticas inseguras. Use esta skill para auditoria técnica de segurança em repositórios GitHub e mudanças sensíveis. Keywords: security review, secrets exposure, auth, authorization, input validation, insecure config, least privilege, GitHub security, code audit, dependency risk.
---

Esta skill faz revisão técnica de segurança em código, configuração e automações de repositórios.

## Quando usar
Use esta skill quando a tarefa envolver:
- revisar mudanças sensíveis
- auditar autenticação e autorização
- verificar exposição de segredos
- avaliar permissões de workflows
- revisar validação de entrada
- analisar endpoints, webhooks ou integrações
- identificar riscos óbvios de segurança no código

## Objetivo
Reduzir riscos de segurança práticos e acionáveis no repositório, sem gerar ruído com alertas genéricos.

## Instruções
1. Identifique a superfície de risco:
   - autenticação
   - autorização
   - entrada de usuário
   - upload de arquivos
   - execução de comandos
   - acesso a banco ou APIs
   - segredos e variáveis sensíveis
   - workflows e tokens

2. Revise o código e a configuração procurando:
   - permissões excessivas
   - falta de validação
   - sanitização insuficiente
   - tratamento inseguro de segredos
   - logs com dados sensíveis
   - defaults perigosos
   - exposição desnecessária de endpoints ou capacidades

3. Classifique os achados:
   - crítico
   - alto
   - médio
   - baixo
   - observação

4. Para cada achado:
   - explique o risco real
   - explique em que contexto ele aparece
   - sugira mitigação proporcional
   - destaque impacto operacional da correção
