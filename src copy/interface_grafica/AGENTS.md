# AGENT – Interface Gráfica (src/interface_grafica)

Este agente se aplica ao código **PySide6** em `src/interface_grafica`. A aplicação de desktop deve servir como ferramenta operacional para auditores sem comprometer as regras de negócio.

## Responsabilidades

- **Visualizar dados** de maneira operacional, exibindo tabelas com filtros por CNPJ, período e outras dimensões.  
- **Permitir ajustes manuais** somente nos campos autorizados pelo pipeline (por exemplo, ajuste de `__qtd_decl_final_audit__`), registrando sempre logs de quem alterou o quê, quando e por quê.  
- **Consumir APIs e datasets** já existentes em `backend/` ou `src/transformacao/` sem duplicar lógica fiscal ou de cálculo.  
- **Manter estado de contexto** por aba ou janela, para que o usuário não perca o período ou CNPJ selecionado ao navegar.

## Convenções

- **Separação de responsabilidades**: componentes de UI não devem conter cálculos complexos. Use controladores ou serviços para buscar dados.  
- **Padrões de interface**: use tabelas com paginação e filtros, evitando dashboards decorativos que não agregam valor operacional.  
- **Sincronização de dados**: quando o usuário ajusta `__qtd_decl_final_audit__` ou outro campo manualmente, atualize imediatamente a fonte de dados (Parquet ou API) e refaça as agregações necessárias.  
- **Naming**: reflita nomes de campos do pipeline na interface para que o usuário compreenda a correspondência exata.  
- **Log de ações**: registre as interações do usuário (alterações, exportações, filtros aplicados) de modo que possam ser auditadas.

## Formato A–E

Ao propor uma nova tela ou recurso na UI, utilize o formato A–E.  
Descreva que dados serão exibidos, quais funções ou endpoints existentes serão reutilizados e quais ajustes manuais devem ser permitidos.

## Anti‑padrões

- **Processar grandes volumes** de dados no cliente. Utilize paginação e virtualização.  
- **Criar regras de negócio** (ex.: calcular tributos) diretamente na interface. Essa lógica deve residir no pipeline ou backend.  
- **Desalinhamento com schema**: exibir colunas que não existem ou omitir colunas obrigatórias sem tratamento.  
- **Falta de persistência**: não registrar logs ou não atualizar datasets após mudanças manuais.