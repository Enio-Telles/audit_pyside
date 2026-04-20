# MAPEAMENTO DA TABELA EFD_REGC170

## Descrição Geral
A tabela `efd_regc170` armazena os **itens de documentos fiscais** do **Bloco C (Operações de Saída)** da EFD-ICMS/IPI. O registro C170 representa cada item detalhado de uma operação de saída registrada no C100 (documento fiscal).

---

## MAPEAMENTO DE CAMPOS

### 1. **Controle e Relacionamento**

| Campo | Tipo | Descrição EFD |
|-------|------|---------------|
| `id` | INTEGER | Identificador único da tabela (chave primária) |
| `arquivo_id` | INTEGER | FK para `efd_arquivo` - Referência ao arquivo EFD processado |
| `efd_resumo_id` | INTEGER | FK para `efd_resumo` - Referência ao registro C100 (documento fiscal) |
| `cod_reg` | INTEGER | Código do Registro = **170** (constante para todos os registros) |

---

### 2. **Informações de Operação**

| Campo | Tipo | Descrição EFD |
|-------|------|---------------|
| `ind_oper` | INTEGER | **Indicador da Operação** (0=Entrada, 1=Saída, 2=Transferência) |
| `ind_emit` | INTEGER | **Indicador do Emitente** (0=Própria empresa, 1=Terceiro) |
| `dt_doc` | TIMESTAMP | Data de emissão do documento fiscal |
| `dt_e_s` | TIMESTAMP | Data de entrada/saída da mercadoria |

---

### 3. **Identificação do Documento Fiscal (Cabeçalho C100)**

| Campo | Tipo | Descrição EFD |
|-------|------|---------------|
| `cod_mod` | VARCHAR(2) | **Código do Modelo** (55=NF-e, 65=NFC-e, 28=CTe, 29=CTe complementar) |
| `ser` | VARCHAR(3) | **Série** do documento |
| `num_doc` | VARCHAR(9) | **Número** do documento fiscal |
| `chave_acesso` | VARCHAR(44) | **Chave de Acesso** de 44 dígitos (NFe/CTe) |
| `cod_sit` | VARCHAR(2) | **Código de Situação** (00=Documento regular, 01=Cancelado, 02=Denegado) |
| `status` | INTEGER | Status de processamento no sistema |

---

### 4. **Participante/Interveniente**

| Campo | Tipo | Descrição EFD |
|-------|------|---------------|
| `cod_part` | VARCHAR(60) | **Código do Participante** (referência ao C150 - outros participantes) |

---

### 5. **Identificação do Produto/Serviço**

| Campo | Tipo | Descrição EFD |
|-------|------|---------------|
| `codigo_produto` | VARCHAR(60) | **Código do Produto** conforme tabela interna da empresa |
| `produto_id_anterior` | INTEGER | ID anterior para histórico de conversão de produtos |
| `descricao_produto` | VARCHAR(200) | Descrição do produto armazenada |
| `chave_produto` | VARCHAR(250) | Chave única para identificação do produto (hash) |
| `num_item` | INTEGER | **Número sequencial do item** no documento (1, 2, 3, ...) |

---

### 6. **Quantidades e Unidades**

| Campo | Tipo | Descrição EFD |
|-------|------|---------------|
| `qtd` | DOUBLE PRECISION | **Quantidade** do item (em unidade comercializada) |
| `unid` | VARCHAR(20) | **Unidade Comercializada** (UN, KG, L, M, etc.) |
| `unid_ant` | VARCHAR(6) | Unidade anterior (para compatibilidade) |
| `unid_convertida` | VARCHAR(10) | Unidade convertida (para análise de conformidade) |
| `operador_conversao` | INTEGER | Operador matemático para conversão (1=multiplicação, 2=divisão) |
| `fator_conversao` | DOUBLE PRECISION | **Fator de Conversão** da unidade inventariada para comercializada |

---

### 7. **Valores do Item**

| Campo | Tipo | Descrição EFD |
|-------|------|---------------|
| `vl_item` | DOUBLE PRECISION | **Valor Total do Item** (sem desconto) = QTD × VUNIT |
| `vl_desc` | DOUBLE PRECISION | **Valor de Desconto** do item (se houver) |
| `vl_ipi` | DOUBLE PRECISION | **Valor do IPI** do item (quando aplicável) |
| `vl_abat_int` | DOUBLE PRECISION | **Valor de Abatimento** não tributado |

---

### 8. **ICMS - Tributação**

| Campo | Tipo | Descrição EFD |
|-------|------|---------------|
| `cst_icms` | VARCHAR(3) | **Código de Situação Tributária** (00, 10, 20, 30, 40, 41, 50, 60, 70, 80, 90) |
| `cfop` | VARCHAR(4) | **Código Fiscal de Operação e Prestação** (5100, 6100, 5102, etc.) |
| `vl_bc_icms` | DOUBLE PRECISION | **Valor da Base de Cálculo do ICMS** |
| `aliq_icms` | DOUBLE PRECISION | **Alíquota do ICMS** (em %) - Alíquota interna/interestadual |
| `aliq_nf` | DOUBLE PRECISION | **Alíquota da NF** (conforme informado na nota) |
| `vl_icms` | DOUBLE PRECISION | **Valor do ICMS** calculado |

---

### 9. **ICMS ST (Substituição Tributária)**

| Campo | Tipo | Descrição EFD |
|-------|------|---------------|
| `vl_bc_icms_st` | DOUBLE PRECISION | **Valor da Base de Cálculo da ST** |
| `vl_icms_st` | DOUBLE PRECISION | **Valor do ICMS ST** retido/cobrado |

---

### 10. **DIFAL (Diferencial de Alíquota - Operações Interestaduais)**

| Campo | Tipo | Descrição EFD |
|-------|------|---------------|
| `valor_difal` | DOUBLE PRECISION | **Valor do DIFAL** (ICMS Diferencial entre UF origem/destino) |
| `aliq_difal_inter` | DOUBLE PRECISION | **Alíquota do DIFAL Interestadual** (%) |

---

### 11. **Finalidade e Classificação**

| Campo | Tipo | Descrição EFD |
|-------|------|---------------|
| `finalidade_mercadoria` | INTEGER | **Finalidade da Mercadoria** (0=Revenda, 1=Consumo, 2=Ativo, 3=Uso/Consumo) |

---

### 12. **Flags de Análise e Conformidade**

| Campo | Descrição |
|-------|-----------|
| `excluir_erro_aliquota` | Flag para excluir item com erro de alíquota |
| `intimar_erro_aliquota` | Flag para intimar error de alíquota |
| `excluir_consumo` | Excluir análise de consumo próprio |
| `intimar_consumo` | Intimar para consumo próprio |
| `excluir_ativo` | Excluir análise de ativo imobilizado |
| `intimar_ativo` | Intimar para ativo imobilizado |
| `excluir_credito_st` | Excluir análise de crédito ST |
| `intimar_credito_st` | Intimar para crédito ST |
| `excluir_tranferencia_credito` | Excluir análise de transferência de crédito |
| `intimar_tranferencia_credito` | Intimar para transferência de crédito |
| `excluir_devolucao` | Excluir análise de devolução |
| `intimar_devolucao` | Intimar para devolução |
| `excluir_difal` | Excluir análise de DIFAL |
| `intimar_difal` | Intimar para DIFAL |
| `excluir_isento` | Excluir operações isentas |
| `intimar_isento` | Intimar operações isentas |
| `excluir_credito_sn` | Excluir análise de crédito para Simples Nacional |
| `intimar_credito_sn` | Intimar crédito para Simples Nacional |
| `excluir_cte_st` | Excluir análise de CTe com ST |
| `intimar_cte_st` | Intimar CTe com ST |
| `verificado_credito` | Flag de verificação de crédito realizada |
| `excluir_calculo_estorno` | Excluir do cálculo de estorno |
| `excluir_levantamento_estoque` | Excluir do levantamento de estoque |

---

### 13. **Informações de Devolução**

| Campo | Tipo | Descrição |
|-------|------|-----------|
| `item_devolucao` | INTEGER | Item do documento original em caso de devolução |
| `qtde_devolucao` | DOUBLE PRECISION | Quantidade devolvida |
| `icms_devolucao` | DOUBLE PRECISION | Valor de ICMS devolvido |

---

## OBSERVAÇÕES IMPORTANTES

### Registro C170 na EFD
- **Obrigatoriedade**: Um registro C170 para cada item de saída registrado no C100
- **Localização**: Segue imediatamente após o registro C100 no arquivo EFD
- **Quantidade**: Pode haver múltiplos registros C170 por C100 (um para cada item)
- **Relacionamento**: Cada C170 deve estar vinculado a um C100 válido

### CST ICMS Mais Comuns em Saídas:
- **00**: Operação Tributada
- **10**: Operação Tributada com Substituição Tributária
- **20**: Operação Isenta
- **40**: Isenta
- **60**: ICMS CT (CT-e)
- **90**: Outras

### CFOP Mais Comuns em Saídas:
- **5100-5109**: Vendas de mercadoria
- **5201-5209**: Devoluções
- **5301-5309**: Transferências
- **6100-6109**: Vendas interestaduais

---

## ANÁLISES TÍPICAS COM C170

1. **Auditoria de Alíquotas**: Verificar se alíquota informada (aliq_nf) vs alíquota tabela (aliq_icms)
2. **Análise de Crédito**: Validar direito de crédito conforme CST e finalidade
3. **Conferência de Estoque**: Comparar quantidades com movimentação física
4. **DIFAL**: Validar operações interestaduais para ICMS Diferencial
5. **Devolução**: Rastrear itens devolvidos e estornos de crédito
6. **Substituto Tributário**: Validar retenção de ST em operações inter
