# Dossiê Fiscal Otimizado — Pacote SQL + Plano Polars

Este pacote contém uma proposta de **atomização do dossiê NIF** em consultas SQL singulares, desenhadas para:
- reduzir repetição de regras;
- separar extração, enriquecimento e apresentação;
- permitir agregação posterior em Python/Polars;
- viabilizar integração com outras bases em **Parquet**.

## Princípios aplicados

1. **Uma chave de contribuinte por vez**  
   Resolver `CO_CNPJ_CPF` e `CO_CAD_ICMS` no início e reutilizar essa base em todas as consultas.

2. **Nada de HTML na camada SQL operacional**  
   As consultas aqui retornam dados crus ou enriquecidos, mas sem formatação visual.

3. **Totais fora do detalhe**  
   Linhas de subtotal e títulos visuais foram removidas. Agregações ficam em consultas dedicadas.

4. **Separação entre fatos e agregados**
   - Fatos: linhas detalhadas, reusáveis.
   - Agregados: resumem fatos por ano/período/status.

5. **Integração orientada a lakehouse/parquet**
   Cada consulta foi pensada para ser extraída para Parquet e depois combinada com outras fontes em Polars.

## Estrutura

- `consultas_sql/00_base/`  
  Resolução de parâmetros, contribuinte e chaves.
- `consultas_sql/10_cadastro/`  
  Cadastro, endereços, atividades, veículos, processos.
- `consultas_sql/20_societario/`  
  Sócios, empresas relacionadas, inadimplência de empresas vinculadas.
- `consultas_sql/30_documentos_fiscais/`  
  NFe/NFCe, VAF, MDF-e, IP de transmissão.
- `consultas_sql/40_arrecadacao_regularidade/`  
  Conta corrente, regime especial, parcelamentos, DIMP.
- `consultas_sql/50_fiscalizacao_conformidade/`  
  Vistorias, ações fiscais, autos, notificações.
- `consultas_sql/90_orquestracao/`  
  Manifestação de dependências e exemplo de consolidação lógica.
- `docs/`  
  Plano de implementação em Polars e matriz de dependências.

## Ordem sugerida de execução

1. `00_base`
2. `10_cadastro`
3. `20_societario`
4. `30_documentos_fiscais`
5. `40_arrecadacao_regularidade`
6. `50_fiscalizacao_conformidade`
7. consolidação em Polars

## Observações importantes

- As consultas foram reestruturadas a partir do XML do dossiê original.
- A sintaxe foi mantida em **Oracle SQL** sempre que possível.
- Alguns nomes/tipos devem ser validados no ambiente real antes de produção.
- As agregações finais do dossiê foram deslocadas para o plano em Polars, o que reduz acoplamento e custo de manutenção.

Origem analisada: `dossie_nif.xml`.
