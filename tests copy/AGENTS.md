# AGENT – Testes (tests/)

Este agente abrange os testes automatizados do projeto, localizados em `tests/`.

## Responsabilidades

- **Validar o comportamento** dos pipelines de extração, normalização, agregação e conversão.
- **Testar reconciliação** entre camadas, garantindo que somatórios e totais no `base` correspondam ao `raw` e que métricas no `curated` representem corretamente os dados originais.
- **Cobrir cenários de borda** (por exemplo, documentos com campos faltantes, unidades não cadastradas, valores monetários muito altos ou zerados).
- **Garantir integridade das chaves** `id_agrupado`, `id_agregado` e `__qtd_decl_final_audit__`.
- **Verificar a persistência e a interface**: testes de UI devem assegurar que edições manuais são refletidas nos datasets e que logs são gerados.

## Convenções

- Utilize frameworks de teste adequados ao contexto (`pytest` para Python).
- Organize testes por módulo ou domínio (`tests/test_transformacao/`, `tests/test_interface_grafica/`).
- Use fixtures com datasets de exemplo representativos; evite depender de dados reais ou sensíveis.
- Escreva testes de performance para operações de Polars que processam grandes volumes, certificando que permanecem dentro de limites aceitáveis.

## Anti‑padrões

- **Testes frágeis** que dependem de datasets externos em constante mudança.
- **Ignorar cenários de erro**; os testes devem cobrir inputs inválidos e comportamentos inesperados.
- **Baixa cobertura** em áreas críticas (conversão de unidades, movimentação de estoque, reconciliação fiscal).