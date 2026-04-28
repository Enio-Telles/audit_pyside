---
name: releases-changelog
description: Organiza releases, versionamento e changelog de projetos em GitHub. Use esta skill para preparar versões, estruturar notas de release, sugerir semantic versioning e reduzir risco em publicações. Keywords: release management, changelog, semantic versioning, semver, release notes, tag, version bump, deployment checklist, rollback.
---

Esta skill ajuda a preparar releases previsíveis, bem documentadas e seguras.

## Quando usar
Use esta skill quando a tarefa envolver:
- preparar nova versão
- decidir bump de versão
- escrever release notes
- gerar changelog
- organizar checklist de publicação
- revisar impacto de mudanças antes de release
- estruturar rollback ou plano de contingência

## Objetivo
Tornar releases mais rastreáveis, claras para usuários e seguras para o time.

## Instruções
1. Identifique o escopo da release.
2. Separe mudanças por categoria:
   - breaking changes
   - features
   - fixes
   - internal changes
   - docs
3. Sugira versionamento com base em impacto:
   - major para quebra de compatibilidade
   - minor para nova funcionalidade compatível
   - patch para correções e ajustes compatíveis
4. Monte changelog objetivo:
   - o que mudou
   - por que importa
   - ações necessárias para consumidores
5. Quando houver risco relevante, inclua:
   - plano de validação
   - plano de rollback
   - dependências externas impactadas

## Regras
- Não trate breaking change como detalhe.
- Não misture mudança interna irrelevante com destaque principal da release.
- Não gerar release notes vagas.
- Não esquecer migrações, se houver.

## Saída esperada
- versão sugerida
- resumo da release
- changelog
- riscos
- checklist pré-release
- checklist pós-release
