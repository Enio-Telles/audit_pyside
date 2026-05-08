# AGENT – Documentação (docs/)

Este agente se aplica ao diretório `docs/`, que contém toda a documentação técnica, decisões de arquitetura, catálogos e guias de uso.

## Responsabilidades

- **Registrar decisões de arquitetura**: explique por que determinada solução foi escolhida (por exemplo, uso de Polars, design de camadas, escolha de chaves).
- **Catalogar datasets**: documente o schema, origem, finalidade e periodicidade de cada Parquet ou SQL materializado.
- **Manter guias**: descreva como executar pipelines, ajustar fatores manuais, rodar testes e depurar erros.
- **Atualizar sempre**: quando um schema, contrato ou processo mudar, a documentação correspondente deve ser atualizada no mesmo PR.

## Convenções

- Use Markdown com títulos hierárquicos e listas, tornando a leitura escaneável.
- Inclua exemplos de código e trechos de SQL quando relevantes.
- Vincule arquivos relacionados (por exemplo, `docs/pipelines/mercadorias.md` pode referenciar `sql/extracao_nfe_raw.sql` e `src/transformacao/mercadorias.py`).
- Mantenha um sumário ou índice (`README.md`) para facilitar navegação.

## Anti‑padrões

- **Documentos desatualizados** que divergem do comportamento atual do código.
- **Falta de documentação** para mudanças que introduzem novos campos, chaves ou contratos.
- **Explicar apenas a implementação** sem justificar a decisão de design.