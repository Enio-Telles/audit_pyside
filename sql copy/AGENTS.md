# AGENT – SQL (sql/)

Este agente cobre o diretório `sql/`, que armazena scripts de extração e manifest SQL utilizados pelas pipelines.

## Responsabilidades

- **Centralizar consultas reutilizáveis**: todas as extrações do Oracle (ou outras fontes) devem estar documentadas aqui.
- **Manter manifestos**: cada script deve incluir comentários indicando a origem (tabelas), filtros aplicados, chaves de resultado e data de criação.
- **Facilitar manutenção**: permitir que atualizações de consulta sejam feitas em um único local, sem espalhar SQL pelo código Python ou UI.

## Convenções

- Organize scripts por camada ou domínio (por exemplo, `raw/`, `base/`, `mercadorias/`).
- Nomeie arquivos de forma descritiva (`extracao_nfe_raw.sql`, `normalizacao_documentos_base.sql`).
- Não realize `INSERT` ou `UPDATE` aqui; as consultas devem ser puramente de leitura.
- Utilize variáveis de período e CNPJ parametrizadas para facilitar reuso.
- Sempre documente as colunas retornadas e as chaves que permitirão integração com o pipeline.

## Anti‑padrões

- Embutir SQL diretamente em scripts Python ou na interface gráfica.
- Escrever consultas sem documentação, dificultando a compreensão e a reutilização.
- Duplicar consultas semelhantes em vários scripts sem refatoração.