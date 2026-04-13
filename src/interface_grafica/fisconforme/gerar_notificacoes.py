"""
Módulo principal para geração de notificações fisconforme.

Este módulo orquestra todo o processo de geração de notificações fiscais:
1. Recebe uma lista de CNPJs
2. Extrai os dados cadastrais de cada CNPJ do Oracle
3. Preenche o modelo de notificação com os dados extraídos
4. Gera um arquivo de notificação para cada CNPJ

Os arquivos são salvos no formato: notificacao_det_<cnpj>.txt

Autor: Gerado automaticamente
Data: 2026-04-01
"""

import sys
import logging
from pathlib import Path
from typing import List, Dict, Any, Optional

# Adiciona o diretório pai ao path para permitir imports relativos
# (removido — usamos imports relativos do pacote)

# Importa os módulos especializados
from .extracao import (
    extrair_dados_cadastrais,
    extrair_dados_multiplos_cnpjs,
    extrair_dados_malha,
    validar_cnpj,
    limpar_cnpj
)
from .preenchimento import (
    processar_notificacao,
    ler_modelo_notificacao,
    preencher_modelo,
    salvar_notificacao
)

# Importa resolvedor de caminhos do pacote integrado
from .path_resolver import get_resource_path, get_root_dir, get_env_path, get_modelo_path

# Configuração do logging para rastrear execuções
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        # Log em arquivo (no diretório de trabalho atual)
        logging.FileHandler('geracao_notificacoes.log', encoding='utf-8'),
        # Log no console
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


# =============================================================================
# CONFIGURAÇÕES DE CAMINHOS (compatível com PyInstaller)
# =============================================================================

# Diretório raiz do projeto Fisconforme_nao_atendidos
ROOT_DIR = get_root_dir()

# Diretório de saída para as notificações geradas (sempre relativo ao cwd)
DIR_SAIDA_NOTIFICACOES = Path('notificacoes')

# Arquivo do modelo de notificação
MODELO_NOTIFICACAO = get_modelo_path()

# Diretório para arquivos Parquet intermediários
DIR_PARQUET = ROOT_DIR / "dados" / "fisconforme" / "data_parquet"


# =============================================================================
# FUNÇÕES AUXILIARES
# =============================================================================

def formatar_tabela_html(dados: List[Dict[str, Any]]) -> str:
    """
    Formata uma lista de dicionários em uma tabela HTML.
    Filtra apenas as colunas: FISCONF, ID MALHA, notificacao, malha e periodo.

    Args:
        dados: Lista de dicionários onde cada dicionário representa
               uma linha da tabela com colunas como chaves.

    Returns:
        String contendo o HTML da tabela formatada.

    Exemplo:
        >>> dados = [
        ...     {"MALHA": "10120", "PERIODO": "01/2024", "VALOR": "R$ 1.000,00"},
        ...     {"MALHA": "10061", "PERIODO": "02/2024", "VALOR": "R$ 2.000,00"}
        ... ]
        >>> formatar_tabela_html(dados)
        '<table>...</table>'
    """
    if not dados:
        return "<p>Nenhuma pendência encontrada.</p>"

    # Mapeamento de chaves possíveis para colunas da tabela
    # Chave no dado -> Nome amigável na coluna
    mapeamento = {
        'id_pendencia': 'ID FISCONF',
        'malhas_id': 'ID MALHA',
        'id_notificacao': 'NOTIFICACAO',
        'titulo_malha': 'MALHA',
        'periodo': 'PERIODO',
        'status_pendencia': 'STATUS'
    }

    # Filtra os dados para incluir apenas as colunas permitidas
    dados_filtrados = []
    for linha in dados:
        linha_filtrada = {}
        for chave_origem, nome_coluna in mapeamento.items():
            # Busca case-insensitive no dicionário de dados
            valor_encontrado = None
            for k, v in linha.items():
                if k.upper() == chave_origem.upper():
                    valor_encontrado = v
                    break
            
            if valor_encontrado is not None:
                linha_filtrada[nome_coluna] = valor_encontrado
        
        if linha_filtrada:
            dados_filtrados.append(linha_filtrada)

    if not dados_filtrados:
        return "<p>Nenhuma pendência encontrada.</p>"

    # Usa as colunas que efetivamente existem nos dados filtrados
    colunas = list(dados_filtrados[0].keys())

    # Constrói o HTML da tabela
    html_lines = [
        '<table border="1" cellpadding="5" cellspacing="0" style="border-collapse: collapse; width: 100%; font-family: Arial, sans-serif; font-size: 12px;">',
        '  <thead>',
        '    <tr style="background-color: #f0f0f0;">'
    ]

    # Adiciona cabeçalho
    for coluna in colunas:
        html_lines.append(f'      <th style="text-align: left; padding: 8px;">{coluna}</th>')

    html_lines.append('    </tr>')
    html_lines.append('  </thead>')
    html_lines.append('  <tbody>')

    # Adiciona linhas de dados
    for i, linha in enumerate(dados_filtrados):
        estilo_linha = 'background-color: #f9f9f9;' if i % 2 == 0 else ''
        html_lines.append(f'    <tr style="{estilo_linha}">')

        for coluna in colunas:
            valor = linha.get(coluna, '')
            if valor is None:
                valor = ''
            html_lines.append(f'      <td style="padding: 8px;">{valor}</td>')

        html_lines.append('    </tr>')

    html_lines.append('  </tbody>')
    html_lines.append('</table>')

    return '\n'.join(html_lines)


def analisar_modelo(caminho_modelo: Optional[Path] = None) -> Dict[str, Any]:
    """
    Analisa o modelo de notificação para identificar placeholders disponíveis.

    Args:
        caminho_modelo: Caminho opcional para o arquivo modelo.
                       Se None, usa o caminho padrão.

    Returns:
        Dicionário com informações sobre o modelo:
        - 'caminho_modelo': Path do arquivo modelo
        - 'total_placeholders': Quantidade de placeholders únicos
        - 'placeholders': Lista de nomes dos placeholders
        - 'erro': Mensagem de erro (se houver)
    """
    try:
        conteudo = ler_modelo_notificacao(caminho_modelo)

        if not conteudo:
            return {'erro': 'Não foi possível ler o modelo de notificação'}

        # Importa função do módulo preenchimento
        from preenchimento import identificar_placeholders

        placeholders = identificar_placeholders(conteudo)

        return {
            'caminho_modelo': caminho_modelo or MODELO_NOTIFICACAO,
            'total_placeholders': len(placeholders),
            'placeholders': placeholders
        }

    except Exception as e:
        logger.error(f"Erro ao analisar modelo: {e}")
        return {'erro': str(e)}


# =============================================================================
# FUNÇÕES DE ORQUESTRAÇÃO PRINCIPAL
# =============================================================================

def gerar_notificacao_para_cnpj(
    cnpj: str,
    dados_manuais: Optional[Dict[str, str]] = None,
    dados_tabela: Optional[List[Dict[str, Any]]] = None,
    diretorio_saida: Optional[Path] = None,
    forcar_reatribuicao: bool = False,
    periodo_analise: Optional[tuple[str, str]] = None
) -> Dict[str, Any]:
    """
    Gera uma notificação fiscal para um único CNPJ.
    
    Esta é a função principal que coordena todo o fluxo de geração
    de notificação para um CNPJ específico.
    
    Fluxo de execução:
    1. Validação do CNPJ
    2. Extração dos dados cadastrais do Oracle
    3. Extração da tabela de pendências (opcional)
    4. Preenchimento do modelo de notificação (com dados manuais e tabela)
    5. Salvamento do arquivo de notificação
    
    Args:
        cnpj: CNPJ da empresa (pode estar formatado com pontos e traço)
        dados_manuais: Dicionário com dados fornecidos manualmente.
                      Chaves esperadas: AUDITOR, MATRICULA, DSF, CONTATO, ORGAO
        dados_tabela: Lista de dicionários com dados de pendências para inserir
                     na tabela HTML. Se None, será extraída automaticamente.
        diretorio_saida: Diretório onde o arquivo será salvo. Se None, usa
                        o diretório padrão notificacoes/<DSF>/.
        forcar_reatribuicao: Se True, força a reatribuição de campos faltantes
                            mesmo que já exista arquivo gerado
    
    Returns:
        Dicionário com o resultado do processamento contendo:
        - 'sucesso': Booleano indicando se o processo foi concluído
        - 'cnpj': CNPJ limpo (apenas números)
        - 'arquivo_saida': Caminho do arquivo gerado (se sucesso)
        - 'erro': Mensagem de erro (se falhou)
        - 'etapa_falha': Nome da etapa onde falhou (se aplicável)
    
    Exemplo de retorno de sucesso:
        {
            'sucesso': True,
            'cnpj': '12345678000190',
            'arquivo_saida': Path('.../notificacoes/DSF-001/notificacao_det_12345678000190.txt')
        }
    
    Exemplo de retorno de falha:
        {
            'sucesso': False,
            'cnpj': '12345678000190',
            'erro': 'CNPJ inválido',
            'etapa_falha': 'validacao'
        }
    """
    logger.info(f"Iniciando geração de notificação para CNPJ: {cnpj}")
    
    # Inicializa resultado com estrutura padrão
    resultado = {
        'sucesso': False,
        'cnpj': limpar_cnpj(cnpj) if cnpj else None,
        'arquivo_saida': None,
        'erro': None,
        'etapa_falha': None
    }
    
    # ==========================================================================
    # ETAPA 1: VALIDAÇÃO DO CNPJ
    # ==========================================================================
    logger.info("Etapa 1/4: Validação do CNPJ")
    
    if not cnpj:
        resultado['erro'] = "CNPJ não fornecido"
        resultado['etapa_falha'] = 'validacao'
        logger.error(f"Falha na validação: {resultado['erro']}")
        return resultado
    
    if not validar_cnpj(cnpj):
        resultado['erro'] = f"CNPJ inválido: {cnpj}"
        resultado['etapa_falha'] = 'validacao'
        logger.error(f"Falha na validação: {resultado['erro']}")
        return resultado
    
    cnpj_limpo = limpar_cnpj(cnpj)
    logger.info(f"CNPJ validado e limpo: {cnpj_limpo}")
    
    # ==========================================================================
    # ETAPA 2: EXTRAÇÃO DE DADOS CADASTRAIS DO ORACLE
    # ==========================================================================
    logger.info("Etapa 2/4: Extração de dados cadastrais do Oracle")
    
    try:
        dados_cadastrais = extrair_dados_cadastrais(cnpj)
        
        if not dados_cadastrais:
            resultado['erro'] = "Nenhum dado encontrado para este CNPJ"
            resultado['etapa_falha'] = 'extracao'
            logger.warning(f"Falha na extração: {resultado['erro']}")
            return resultado
        
        logger.info(f"Dados extraídos com sucesso. Campos encontrados: {list(dados_cadastrais.keys())}")
    
    except ValueError as e:
        resultado['erro'] = str(e)
        resultado['etapa_falha'] = 'extracao'
        logger.error(f"Falha na extração (ValueError): {e}")
        return resultado
    
    except ConnectionError as e:
        resultado['erro'] = str(e)
        resultado['etapa_falha'] = 'extracao'
        logger.error(f"Falha na extração (ConnectionError): {e}")
        return resultado
    
    except Exception as e:
        resultado['erro'] = f"Erro inesperado na extração: {e}"
        resultado['etapa_falha'] = 'extracao'
        logger.error(f"Falha na extração (Exception): {e}")
        return resultado
    
    # ==========================================================================
    # ETAPA 3: TABELA DE PENDÊNCIAS (dados fornecidos externamente)
    # ==========================================================================
    logger.info("Etapa 3/4: Verificação da tabela de pendências")
    
    if dados_tabela is None:
        try:
            logger.info(f"Extraindo dados de malha diretamente do Oracle para o período: {periodo_analise}")
            d_ini, d_fim = periodo_analise if periodo_analise else (None, None)
            dados_tabela = extrair_dados_malha(cnpj, d_ini, d_fim)
            if not dados_tabela:
                logger.info("Nenhuma pendência encontrada no Oracle para este CNPJ no período informado.")
            else:
                logger.info(f"Extraídas {len(dados_tabela)} pendências do Oracle.")
        except Exception as e:
            logger.error(f"Erro ao extrair pendências do Oracle: {e}")
            dados_tabela = []
    else:
        logger.info(f"Dados da tabela fornecidos externamente: {len(dados_tabela)} linha(s)")
    
    # ==========================================================================
    # ETAPA 4: PREENCHIMENTO E SALVAMENTO DA NOTIFICAÇÃO
    # ==========================================================================
    logger.info("Etapa 4/4: Preenchimento e salvamento da notificação")
    
    try:
        # Formata a tabela HTML
        tabela_html = formatar_tabela_html(dados_tabela) if dados_tabela else "<p>Nenhuma pendência encontrada.</p>"
        
        # Log para depuração
        logger.info(f"Tabela HTML gerada: {len(tabela_html)} caracteres")
        if dados_tabela:
            logger.info(f"Dados da tabela: {len(dados_tabela)} linha(s)")
            for i, linha in enumerate(dados_tabela[:3], start=1):  # Log das 3 primeiras linhas
                logger.info(f"  Linha {i}: {linha}")
        else:
            logger.warning("Nenhum dado de pendência disponível para a tabela")
        
        # Adiciona a tabela aos dados para preenchimento
        dados_completos = dados_cadastrais.copy()
        dados_completos['TABELA'] = tabela_html
        
        # Log dos dados completos
        logger.info(f"Dados completos para preenchimento: {list(dados_completos.keys())}")
        
        # Usa diretório fornecido ou padrão
        if diretorio_saida is None:
            diretorio_saida = DIR_SAIDA_NOTIFICACOES
        
        caminho_arquivo = processar_notificacao(
            cnpj=cnpj,
            dados=dados_completos,
            diretorio_saida=diretorio_saida,
            dados_manuais=dados_manuais
        )

        if not caminho_arquivo:
            resultado['erro'] = "Falha ao processar notificação"
            resultado['etapa_falha'] = 'preenchimento'
            logger.error(f"Falha no preenchimento: {resultado['erro']}")
            return resultado

        # Sucesso! Preenche resultado final
        resultado['sucesso'] = True
        resultado['arquivo_saida'] = caminho_arquivo
        logger.info(f"Notificação gerada com sucesso: {caminho_arquivo}")

    except Exception as e:
        resultado['erro'] = f"Erro ao processar notificação: {e}"
        resultado['etapa_falha'] = 'preenchimento'
        logger.error(f"Falha no preenchimento (Exception): {e}")
        return resultado

    return resultado


def gerar_notificacoes_em_lote(
    lista_cnpjs: List[str],
    dados_manuais: Optional[Dict[str, str]] = None,
    periodo_analise: Optional[tuple[str, str]] = None,
    parar_no_primeiro_erro: bool = False,
    arquivo_dsf: Optional[Path] = None
) -> Dict[str, Any]:
    """
    Gera notificações fiscais para múltiplos CNPJs em lote.

    Processa uma lista de CNPJs sequencialmente, gerando um arquivo
    de notificação para cada CNPJ válido que possuir dados no banco.

    Args:
        lista_cnpjs: Lista de CNPJs para processamento
        dados_manuais: Dicionário com dados fornecidos manualmente.
                      Exemplo: {"AUDITOR": "João Silva", "MATRICULA": "12345",
                               "DSF": "20263710400285", "CONTATO": "(69) 99999-9999",
                               "ORGAO": "SEFIN"}
        periodo_analise: Tupla opcional com (data_inicial, data_final) no formato MM/YYYY
        parar_no_primeiro_erro: Se True, interrompe o processamento
                               ao encontrar o primeiro erro
        arquivo_dsf: Caminho do arquivo PDF da DSF para conversão em imagens

    Returns:
        Dicionário com o resumo do processamento contendo:
        - 'total': Quantidade total de CNPJs fornecidos
        - 'sucessos': Quantidade de notificações geradas com sucesso
        - 'falhas': Quantidade de falhas no processamento
        - 'resultados': Dicionário detalhado por CNPJ
        - 'arquivos_gerados': Lista de caminhos dos arquivos gerados
        - 'cnpjs_por_dsf': Dicionário com CNPJs agrupados por DSF

    Exemplo:
        >>> cnpjs = ["12.345.678/0001-90", "98.765.432/0001-10"]
        >>> dados_manuais = {
        ...     "AUDITOR": "João Silva",
        ...     "MATRICULA": "12345",
        ...     "DSF": "20263710400285",
        ...     "CONTATO": "(69) 99999-9999",
        ...     "ORGAO": "SEFIN"
        ... }
        >>> periodo = ("01/2024", "12/2024")
        >>> gerar_notificacoes_em_lote(cnpjs, dados_manuais=dados_manuais, periodo_analise=periodo)
        {
            'total': 2,
            'sucessos': 2,
            'falhas': 0,
            'resultados': {...},
            'arquivos_gerados': [Path(...), Path(...)],
            'cnpjs_por_dsf': {'20263710400285': ['12345678000190', ...]}
        }
    """
    logger.info(f"Iniciando processamento em lote com {len(lista_cnpjs)} CNPJ(s)")

    # Extrai DSF dos dados manuais
    dsf = dados_manuais.get('DSF', 'DSF_GERAL') if dados_manuais else 'DSF_GERAL'

    # Extrai mês e ano da data atual para compor o nome da pasta
    from datetime import datetime
    data_atual = datetime.now()
    mes_ano = data_atual.strftime("%m%Y")

    # Define diretório de saída como notificacoes/ (sem subpastas por CNPJ)
    dir_saida_base = DIR_SAIDA_NOTIFICACOES
    dir_saida_base.mkdir(parents=True, exist_ok=True)
    logger.info(f"Notificações serão salvas em: {dir_saida_base}")

    # Inicializa estrutura de resultados
    resumo = {
        'total': len(lista_cnpjs),
        'sucessos': 0,
        'falhas': 0,
        'resultados': {},
        'arquivos_gerados': [],
        'dsf': dsf,
        'diretorio_saida': dir_saida_base,
        'cnpjs_por_dsf': {dsf: []},  # Agrupa CNPJs por DSF
        'arquivo_dsf': arquivo_dsf
    }

    # Processa cada CNPJ sequencialmente
    for indice, cnpj in enumerate(lista_cnpjs, start=1):
        logger.info(f"Processando CNPJ {indice}/{len(lista_cnpjs)}: {cnpj}")

        # Gera notificação para este CNPJ (salva diretamente na pasta notificacoes/)
        cnpj_limpo = limpar_cnpj(cnpj) if cnpj else str(indice)
        
        # Gera notificação para este CNPJ
        resultado = gerar_notificacao_para_cnpj(
            cnpj=cnpj,
            dados_manuais=dados_manuais,
            dados_tabela=None,  # Será extraído automaticamente
            diretorio_saida=dir_saida_base,  # Salva diretamente na pasta notificacoes/
            forcar_reatribuicao=False,
            periodo_analise=periodo_analise
        )
        cnpj_result = resultado['cnpj']

        # Armazena resultado detalhado
        resumo['resultados'][cnpj_result] = resultado

        # Agrupa CNPJ por DSF
        dsf_result = dados_manuais.get('DSF', dsf) if dados_manuais else dsf
        if dsf_result not in resumo['cnpjs_por_dsf']:
            resumo['cnpjs_por_dsf'][dsf_result] = []
        if resultado['sucesso']:
            resumo['cnpjs_por_dsf'][dsf_result].append(cnpj_result)

        # Atualiza contadores
        if resultado['sucesso']:
            resumo['sucessos'] += 1
            resumo['arquivos_gerados'].append(resultado['arquivo_saida'])
        else:
            resumo['falhas'] += 1

            # Verifica se deve parar no primeiro erro
            if parar_no_primeiro_erro:
                logger.warning(
                    f"Parando processamento devido a erro no CNPJ {cnpj}: "
                    f"{resultado['erro']}"
                )
                break

    # Salva arquivo de resumo com CNPJs agrupados por DSF
    try:
        import json
        resumo_cnpjs_dsf = {
            'dsf': dsf,
            'mes_ano': mes_ano,
            'total_cnpjs': len(lista_cnpjs),
            'cnpjs_por_dsf': resumo['cnpjs_por_dsf'],
            'arquivo_dsf': str(arquivo_dsf) if arquivo_dsf else None
        }
        
        arquivo_resumo = dir_saida_base / "resumo_cnpjs_por_dsf.json"
        with open(arquivo_resumo, 'w', encoding='utf-8') as f:
            json.dump(resumo_cnpjs_dsf, f, indent=2, ensure_ascii=False)
        
        logger.info(f"Resumo de CNPJs por DSF salvo em: {arquivo_resumo}")
    except Exception as e:
        logger.warning(f"Não foi possível salvar o resumo de CNPJs por DSF: {e}")

    # Log de resumo final
    logger.info(
        f"Processamento em lote concluído: "
        f"{resumo['sucessos']} sucesso(s), {resumo['falhas']} falha(s)"
    )

    return resumo


def listar_cnpjs_para_processamento(
    arquivo_entrada: Optional[Path] = None
) -> List[str]:
    """
    Lista CNPJs para processamento a partir de arquivo ou entrada manual.
    
    Função utilitária para carregar uma lista de CNPJs de um arquivo texto
    (um CNPJ por linha) ou para solicitar entrada manual do usuário.
    
    Suporta múltiplos formatos de CNPJ no arquivo:
    - Apenas números: 12345678000190
    - Com formatação: 12.345.678/0001-90
    - Com espaços: 12.345.678 / 0001-90
    - Misto: 12345678/0001-90, 12.345.678000190, etc.
    
    Args:
        arquivo_entrada: Caminho opcional para arquivo contendo CNPJs.
                        Se None, solicita entrada manual do usuário.
    
    Returns:
        Lista de CNPJs para processamento (formato original, sem limpeza)
    
    Formato do arquivo de entrada:
        Um CNPJ por linha, linhas em branco e comentários (#) são ignorados:
        
        12.345.678/0001-90
        98765432000110
        # Este é um comentário
        11.222.333/0001-44
        11222333000144
   """
    cnpjs = []
    
    if arquivo_entrada and Path(arquivo_entrada).exists():
        # Lê CNPJs de arquivo
        logger.info(f"Lendo CNPJs do arquivo: {arquivo_entrada}")
        
        try:
            with open(arquivo_entrada, 'r', encoding='utf-8') as f:
                linha_num = 0
                for linha in f:
                    linha_num += 1
                    linha = linha.strip()
                    
                    # Ignora linhas vazias e comentários
                    if not linha or linha.startswith('#'):
                        continue
                    
                    # Extrai CNPJ da linha (pode ter vários formatos)
                    cnpj_extraido = extrair_cnpj_da_linha(linha)
                    
                    if cnpj_extraido:
                        # Valida se o CNPJ extraído é válido
                        if validar_cnpj(cnpj_extraido):
                            cnpjs.append(cnpj_extraido)
                            logger.info(f"Linha {linha_num}: CNPJ válido encontrado: {cnpj_extraido}")
                        else:
                            logger.warning(f"Linha {linha_num}: CNPJ inválido: {cnpj_extraido}")
                    else:
                        logger.warning(f"Linha {linha_num}: Nenhum CNPJ válido encontrado na linha: '{linha}'")
        
        except Exception as e:
            logger.error(f"Erro ao ler arquivo {arquivo_entrada}: {e}")
            print(f"[ERRO] Não foi possível ler o arquivo: {e}")
            return []
        
        logger.info(f"{len(cnpjs)} CNPJ(s) válidos lidos do arquivo")
        print(f"\n✅ {len(cnpjs)} CNPJ(s) válidos encontrados no arquivo: {arquivo_entrada}")
        
        # Exibe resumo dos CNPJs encontrados
        if cnpjs:
            print("\nCNPJs encontrados:")
            for i, cnpj in enumerate(cnpjs[:10], start=1):  # Mostra até 10
                print(f"  {i}. {cnpj}")
            if len(cnpjs) > 10:
                print(f"  ... e mais {len(cnpjs) - 10} CNPJ(s)")
    
    else:
        # Solicita entrada manual do usuário
        print("\n" + "=" * 60)
        print("Geração de Notificações Fisconforme")
        print("=" * 60)
        print("\nInforme os CNPJs para processamento.")
        print("Digite um CNPJ por linha. Deixe em branco para finalizar.")
        print("=" * 60)
        
        while True:
            try:
                entrada = input(f"\nCNPJ {len(cnpjs) + 1} (ou Enter para finalizar): ").strip()
                
                if not entrada:
                    break
                
                # Valida o CNPJ informado
                if validar_cnpj(entrada):
                    cnpjs.append(entrada)
                    print(f"  ✅ CNPJ adicionado: {entrada}")
                else:
                    print(f"  ❌ CNPJ inválido: {entrada}")
            
            except KeyboardInterrupt:
                print("\n\nOperação cancelada pelo usuário.")
                break
            except EOFError:
                break
        
        print("\n" + "=" * 60)
        print(f"Total de CNPJs informados: {len(cnpjs)}")
        print("=" * 60)
    
    return cnpjs


def extrair_cnpj_da_linha(linha: str) -> Optional[str]:
    """
    Extrai um CNPJ de uma linha de texto, suportando vários formatos.
    
    Esta função usa expressões regulares para identificar e extrair CNPJs
    em diferentes formatos:
    - Apenas números: 12345678000190
    - Formatado: 12.345.678/0001-90
    - Parcialmente formatado: 12345678/0001-90, 12.345.678000190
    - Com espaços: 12.345.678 / 0001-90
    
    Args:
        linha: String contendo potencialmente um CNPJ
    
    Returns:
        CNPJ formatado se encontrado e válido, None caso contrário
    
    Exemplos:
        >>> extrair_cnpj_da_linha("12.345.678/0001-90")
        '12.345.678/0001-90'
        
        >>> extrair_cnpj_da_linha("12345678000190")
        '12345678000190'
        
        >>> extrair_cnpj_da_linha("CNPJ: 12.345.678/0001-90 - Empresa X")
        '12.345.678/0001-90'
    """
    import re
    
    # Padrão 1: CNPJ formatado completo (XX.XXX.XXX/XXXX-XX)
    padrao_formatado = r'\b(\d{1,2}\.\d{3}\.\d{3}\s*/\s*\d{4}-\d{2})\b'
    
    # Padrão 2: CNPJ com barra (XXXXXXXX/XXXX-XX ou XXXXXXXX/XXXXXXXX)
    padrao_com_barra = r'\b(\d{7,8}\s*/\s*\d{4,8}[-]?\d{0,2})\b'
    
    # Padrão 3: CNPJ apenas números (14 dígitos)
    padrao_numeros = r'\b(\d{14})\b'
    
    # Padrão 4: CNPJ com alguns pontos mas sem barra completa
    padrao_parcial = r'\b(\d{1,2}\.\d{3}\.\d{3}\d{6,8})\b'
    
    # Tenta cada padrão em ordem de especificidade
    padroes = [padrao_formatado, padrao_com_barra, padrao_parcial, padrao_numeros]
    
    for padrao in padroes:
        match = re.search(padrao, linha)
        if match:
            cnpj_encontrado = match.group(1)
            
            # Limpa espaços extras
            cnpj_limpo = cnpj_encontrado.replace(' ', '')
            
            # Valida o CNPJ encontrado
            if validar_cnpj(cnpj_limpo):
                return cnpj_limpo
    
    # Se nenhum padrão encontrou CNPJ válido, tenta extrair todos os dígitos
    # e verificar se formam um CNPJ válido
    todos_digitos = re.sub(r'\D', '', linha)
    
    if len(todos_digitos) == 14 and validar_cnpj(todos_digitos):
        return todos_digitos
    
    return None


def listar_arquivos_txt_no_diretorio(diretorio: Optional[Path] = None) -> List[Path]:
    """
    Lista todos os arquivos TXT em um diretório.
    
    Args:
        diretorio: Diretório para buscar arquivos. Se None, usa o diretório raiz.
    
    Returns:
        Lista de caminhos de arquivos TXT encontrados
    """
    if diretorio is None:
        diretorio = ROOT_DIR
    
    arquivos_txt = list(diretorio.glob("*.txt"))
    
    # Filtra arquivos que podem conter CNPJs (exclui modelos e logs)
    arquivos_cnpjs = [
        f for f in arquivos_txt 
        if 'modelo' not in f.name.lower() 
        and 'log' not in f.name.lower()
        and 'readme' not in f.name.lower()
    ]
    
    return arquivos_cnpjs


def listar_arquivos_dsf(diretorio: Optional[Path] = None) -> List[Path]:
    """
    Lista todos os arquivos PDF no diretório dsf.

    Args:
        diretorio: Diretório para buscar arquivos. Se None, usa ROOT_DIR/dsf.

    Returns:
        Lista de caminhos de arquivos PDF encontrados, ordenados por nome
    """
    if diretorio is None:
        diretorio = ROOT_DIR / "dsf"

    if not diretorio.exists():
        return []

    arquivos_pdf = sorted(diretorio.glob("*.pdf"))
    return arquivos_pdf


def coletar_arquivo_dsf() -> Optional[Path]:
    """
    Coleta do usuário o caminho do arquivo PDF da DSF para conversão em imagens.

    Oferece três opções:
    1. Selecionar da pasta dsf/
    2. Informar caminho manualmente
    3. Pular (não usar DSF)

    Returns:
        Caminho do arquivo PDF da DSF, ou None se não informado
    """
    print("\n" + "=" * 60)
    print("ARQUIVO PDF DA DSF")
    print("=" * 60)
    print("\nInforme o caminho do arquivo PDF da DSF para conversão em imagens.")
    print("Este arquivo será usado para gerar as imagens inseridas nas notificações.")
    print("-" * 60)

    # Lista arquivos na pasta dsf/
    dsf_dir = ROOT_DIR / "dsf"
    arquivos_dsf = listar_arquivos_dsf(dsf_dir)

    try:
        # Opção 1: Selecionar da pasta dsf/
        if arquivos_dsf:
            print(f"\n📁 Arquivos PDF encontrados em {dsf_dir}:")
            print("-" * 60)
            for i, arquivo in enumerate(arquivos_dsf, start=1):
                tamanho_mb = arquivo.stat().st_size / (1024 * 1024)
                print(f"  [{i}] {arquivo.name} ({tamanho_mb:.2f} MB)")
            print("-" * 60)

            print("\nComo deseja proceder?")
            print("  [1] Selecionar arquivo da lista acima")
            print("  [2] Informar caminho manualmente")
            print("  [3] Pular (gerar notificações sem imagens da DSF)")
            print("-" * 60)

            while True:
                opcao = input("\nEscolha uma opção (1-3): ").strip()

                if opcao == '3':
                    print("⚠️  Nenhum arquivo PDF informado. As notificações serão geradas sem imagens da DSF.")
                    return None

                elif opcao == '1':
                    while True:
                        try:
                            idx = input(f"  Número do arquivo (1-{len(arquivos_dsf)}): ").strip()
                            if not idx:
                                break

                            idx_int = int(idx)
                            if 1 <= idx_int <= len(arquivos_dsf):
                                pdf_path = arquivos_dsf[idx_int - 1]
                                print(f"✅ Arquivo PDF selecionado: {pdf_path.name}")
                                return pdf_path
                            else:
                                print(f"  ❌ Opção inválida. Digite um número entre 1 e {len(arquivos_dsf)}.")
                        except ValueError:
                            print("  ❌ Digite apenas números.")

                elif opcao == '2':
                    break  # Vai para entrada manual

                else:
                    print("  ❌ Opção inválida. Digite 1, 2 ou 3.")

        # Opção 2: Entrada manual do caminho
        print("\nInforme o caminho do arquivo PDF da DSF:")
        while True:
            caminho_pdf = input("\nCaminho do arquivo PDF (ou Enter para pular): ").strip()

            if not caminho_pdf:
                print("⚠️  Nenhum arquivo PDF informado. As notificações serão geradas sem imagens da DSF.")
                return None

            # Converte para Path
            pdf_path = Path(caminho_pdf)

            # Verifica se o arquivo existe
            if not pdf_path.exists():
                print(f"❌ Arquivo não encontrado: {pdf_path}")
                print("   Verifique o caminho e tente novamente.")
                continue

            # Verifica se é um arquivo PDF
            if pdf_path.suffix.lower() != '.pdf':
                print(f"⚠️  O arquivo não tem extensão .pdf: {pdf_path.suffix}")
                continuar = input("   Deseja continuar mesmo assim? (S/N): ").strip().upper()
                if continuar not in ('S', 'SIM', 'Y', 'YES'):
                    continue

            print(f"✅ Arquivo PDF selecionado: {pdf_path}")
            return pdf_path

    except (KeyboardInterrupt, EOFError):
        print("\n\nColeta de arquivo DSF cancelada.")
        return None


def coletar_dados_manuais() -> Dict[str, str]:
    """
    Coleta os dados manuais do usuário via entrada interativa.

    Solicita ao usuário que informe os dados que serão usados em todas
    as notificações:
    - AUDITOR: Nome do auditor fiscal
    - MATRICULA: Número de matrícula do auditor
    - DSF: Identificação da DSF (Divisão de Serviço Fiscal)
    - CONTATO: Telefone/Email para contato
    - ORGAO: Nome do órgão expedidor

    Returns:
        Dicionário com os campos manuais preenchidos

    Exemplo:
        {
            "AUDITOR": "João Silva",
            "MATRICULA": "12345",
            "DSF": "20263710400285",
            "CONTATO": "(69) 99999-9999",
            "ORGAO": "SEFIN"
        }
    """
    print("\n" + "=" * 60)
    print("DADOS DO AUDITOR / ÓRGÃO EXPEDIDOR")
    print("=" * 60)

    # Tenta carregar dados salvos anteriormente
    dados_salvos = carregar_dados_salvos()

    if dados_salvos:
        print(f"\n📂 {len(dados_salvos)} configuração(ões) encontrada(s):")
        print("-" * 60)
        for i, (nome, dados) in enumerate(dados_salvos.items(), start=1):
            print(f"  [{i}] {nome}")
            print(f"      Auditor: {dados.get('AUDITOR', 'N/A')} | Matrícula: {dados.get('MATRICULA', 'N/A')}")
        print("-" * 60)
        print("\nOpções:")
        print("  - Digite o número da configuração desejada")
        print("  - Ou pressione Enter para cadastrar nova configuração")

        try:
            opcao = input("\nEscolha uma opção (número ou Enter para novo): ").strip()

            if opcao.isdigit() and 1 <= int(opcao) <= len(dados_salvos):
                # Usuário selecionou uma configuração salva
                nomes = list(dados_salvos.keys())
                config_selecionada = nomes[int(opcao) - 1]
                dados_selecionados = dados_salvos[config_selecionada]

                print(f"\n✅ Configuração '{config_selecionada}' selecionada!")
                print("\nDados carregados:")
                for chave, valor in dados_selecionados.items():
                    if chave not in ('DSF', 'DSF_NUM'):  # Não mostra DSF antiga para não confundir
                        print(f"  {chave}: {valor}")

                dados_manuais = dados_selecionados.copy()
            else:
                # Se não selecionou, cadastra novo
                print("\n📝 Cadastro de nova configuração")
                dados_manuais = {}

        except (ValueError, KeyboardInterrupt, EOFError):
            dados_manuais = {}
    else:
        dados_manuais = {}

    # Se não temos dados (ou se escolheu cadastrar novo), coleta manualmente
    if not dados_manuais:
        try:
            auditor = input("\nNome do AUDITOR: ").strip()
            if auditor:
                dados_manuais['AUDITOR'] = auditor

            matricula = input("Matrícula do auditor (MATRICULA): ").strip()
            if matricula:
                dados_manuais['MATRICULA'] = matricula

            contato = input("Contato (telefone/email): ").strip()
            if contato:
                dados_manuais['CONTATO'] = contato

            orgao = input("Órgão expedidor (ORGAO): ").strip()
            if orgao:
                dados_manuais['ORGAO'] = orgao

        except (KeyboardInterrupt, EOFError):
            print("\n\nColeta de dados cancelada.")
            return {}

    # Passo obrigatório: Sempre pedir o número da DSF
    try:
        dsf_atual = dados_manuais.get('DSF', '')
        prompt_dsf = f"\nNúmero da DSF para esta geração [{dsf_atual}]: " if dsf_atual else "\nNúmero da DSF para esta geração: "

        while True:
            dsf = input(prompt_dsf).strip()
            if dsf:
                dados_manuais['DSF'] = dsf
                break
            elif dsf_atual:
                # Se já tinha no config e deu Enter, mantém
                break
            else:
                print("❌ O número da DSF é obrigatório.")

    except (KeyboardInterrupt, EOFError):
        print("\n\nColeta de DSF cancelada.")
        return {}

    # Validação mínima: pelo menos um campo (exceto DSF que já é obrigatório)
    if len(dados_manuais) <= 1 and 'DSF' in dados_manuais:
        logger.info("Apenas DSF informada.")

    # Pergunta se deseja salvar
    print("\n" + "-" * 60)
    print("DADOS COLETADOS:")
    for chave, valor in dados_manuais.items():
        print(f"  {chave}: {valor}")
    print("-" * 60)

    try:
        salvar = input("\nDeseja salvar esta configuração para uso futuro? (S/N): ").strip().upper()
        if salvar in ('S', 'SIM', 'Y', 'YES'):
            nome_config = input("Nome para esta configuração (ex: 'Padrao', 'Auditor_Joao'): ").strip()
            if not nome_config:
                nome_config = f"Config_{len(dados_salvos) + 1}"

            salvar_dados_manuais(nome_config, dados_manuais)
            print(f"✅ Configuração '{nome_config}' salva com sucesso!")

    except (KeyboardInterrupt, EOFError):
        pass

    print("=" * 60)
    return dados_manuais


def coletar_periodo_analise() -> tuple[str, str]:
    """
    Coleta do usuário o período de análise para extração das pendências.
    
    Solicita as datas inicial e final no formato MM/YYYY para usar
    como parâmetros na query SQL de extração de pendências.
    
    Returns:
        Tupla contendo (data_inicial, data_final) no formato MM/YYYY
    
    Exemplo:
        ("01/2024", "12/2024")
    """
    print("\n" + "=" * 60)
    print("PERÍODO DE ANÁLISE DAS PENDÊNCIAS")
    print("=" * 60)
    print("\nInforme o período para extração das pendências.")
    print("Formato: MM/YYYY (ex: 01/2024)")
    print("-" * 60)
    
    data_inicial = "01/2024"  # Valor padrão
    data_final = "12/2024"    # Valor padrão
    
    try:
        # Coleta data inicial
        entrada_inicial = input(f"\nData inicial [{data_inicial}]: ").strip()
        if entrada_inicial:
            # Valida formato
            if validar_formato_data(entrada_inicial):
                data_inicial = entrada_inicial
            else:
                print(f"⚠️  Formato inválido. Usando padrão: {data_inicial}")
        
        # Coleta data final
        entrada_final = input(f"Data final [{data_final}]: ").strip()
        if entrada_final:
            # Valida formato
            if validar_formato_data(entrada_final):
                data_final = entrada_final
            else:
                print(f"⚠️  Formato inválido. Usando padrão: {data_final}")
        
    except (KeyboardInterrupt, EOFError):
        print("\n\nColeta de período cancelada.")
    
    print("\n" + "-" * 60)
    print(f"Período de análise definido:")
    print(f"  Início: {data_inicial}")
    print(f"  Fim:    {data_final}")
    print("=" * 60)
    
    return data_inicial, data_final


def validar_formato_data(data: str) -> bool:
    """
    Valida se uma string está no formato MM/YYYY.
    
    Args:
        data: String no formato MM/YYYY
    
    Returns:
        True se o formato for válido, False caso contrário
    """
    import re
    padrao = r'^\d{2}/\d{4}$'
    
    if not re.match(padrao, data):
        return False
    
    # Valida mês (01-12)
    try:
        mes, ano = data.split('/')
        mes_int = int(mes)
        if mes_int < 1 or mes_int > 12:
            return False
    except:
        return False
    
    return True


def carregar_config_db() -> Dict[str, str]:
    """
    Carrega especificamente as configurações do banco de dados Oracle do .env.
    Retorna padrões da SEFIN se não preenchido.
    """
    try:
        from dotenv import dotenv_values
        
        # Usa o resolvedor de caminhos para encontrar .env
        env_path = get_env_path()

        # Padrões SEFIN
        padroes = {
            'ORACLE_HOST': 'exa01-scan.sefin.ro.gov.br',
            'ORACLE_PORT': '1521',
            'ORACLE_SERVICE': 'sefindw',
            'DB_USER': '',
            'DB_PASSWORD': ''
        }

        if not env_path.exists():
            logger.warning(f"Arquivo .env não encontrado em: {env_path}")
            return padroes

        env_vars = dotenv_values(env_path)

        return {
            'ORACLE_HOST': env_vars.get('ORACLE_HOST', padroes['ORACLE_HOST']),
            'ORACLE_PORT': env_vars.get('ORACLE_PORT', padroes['ORACLE_PORT']),
            'ORACLE_SERVICE': env_vars.get('ORACLE_SERVICE', padroes['ORACLE_SERVICE']),
            'DB_USER': env_vars.get('DB_USER', ''),
            'DB_PASSWORD': env_vars.get('DB_PASSWORD', '')
        }
    except Exception as e:
        logger.warning(f"Erro ao carregar configurações de DB: {e}")
        return {}


def salvar_config_db(dados: Dict[str, str]) -> bool:
    """
    Salva as configurações de conexão Oracle no arquivo .env.
    """
    try:
        # Usa o resolvedor de caminhos para encontrar .env
        env_path = get_env_path()

        # Lê conteúdo atual
        conteudo_atual = ""
        if env_path.exists():
            with open(env_path, 'r', encoding='utf-8') as f:
                conteudo_atual = f.read()

        # Chaves a serem atualizadas
        chaves = ['ORACLE_HOST', 'ORACLE_PORT', 'ORACLE_SERVICE', 'DB_USER', 'DB_PASSWORD']

        conteudo_final = conteudo_atual
        for chave in chaves:
            valor = str(dados.get(chave, '')).strip()
            import re
            # Substitui se já existir, senão adiciona ao final
            if re.search(fr'^{chave}=', conteudo_final, flags=re.MULTILINE):
                conteudo_final = re.sub(
                    fr'^{chave}=.*$',
                    f"{chave}={valor}",
                    conteudo_final,
                    flags=re.MULTILINE
                )
            else:
                conteudo_final = conteudo_final.rstrip() + f"\n{chave}={valor}\n"

        with open(env_path, 'w', encoding='utf-8') as f:
            f.write(conteudo_final.strip() + '\n')

        logger.info(f"Configurações de banco de dados salvas em: {env_path}")
        return True
    except Exception as e:
        logger.error(f"Erro ao salvar configurações de DB: {e}")
        return False


def carregar_dados_salvos() -> Dict[str, Dict[str, str]]:
    """
    Carrega configurações salvas no arquivo .env.

    Procura por variáveis no formato:
    - CONFIG_AUDITOR_NOME: Nome da configuração
    - CONFIG_AUDITOR_NOME_AUDITOR: Nome do auditor
    - CONFIG_AUDITOR_NOME_MATRICULA: Matrícula
    - etc.

    Returns:
        Dicionário com configurações salvas
    """
    import re

    try:
        from dotenv import dotenv_values
        
        # Usa o resolvedor de caminhos para encontrar .env
        env_path = get_env_path()

        if not env_path.exists():
            return {}

        # Carrega todas as variáveis do .env
        env_vars = dotenv_values(env_path)
        
        # Encontra todas as configurações salvas
        configs = {}
        padrao_config = re.compile(r'^CONFIG_(.+)_AUDITOR$')
        
        for chave, valor in env_vars.items():
            match = padrao_config.match(chave)
            if match:
                nome_config = match.group(1)
                
                # Carrega todos os campos desta configuração
                configs[nome_config] = {
                    'AUDITOR': valor,
                    'MATRICULA': env_vars.get(f'CONFIG_{nome_config}_MATRICULA', ''),
                    'DSF': env_vars.get(f'CONFIG_{nome_config}_DSF', ''),
                    'CONTATO': env_vars.get(f'CONFIG_{nome_config}_CONTATO', ''),
                    'ORGAO': env_vars.get(f'CONFIG_{nome_config}_ORGAO', '')
                }
        
        return configs
    
    except Exception as e:
        logger.warning(f"Erro ao carregar configurações salvas: {e}")
        return {}


def salvar_dados_manuais(nome_config: str, dados: Dict[str, str]) -> None:
    """
    Salva uma configuração no arquivo .env.

    Args:
        nome_config: Nome identificador da configuração
        dados: Dicionário com os dados a serem salvos
    """
    try:
        # Usa o resolvedor de caminhos para encontrar .env
        env_path = get_env_path()

        # Sanitiza o nome_config para evitar espaços nas chaves do .env
        import re
        nome_config_sanitizado = re.sub(r'[^a-zA-Z0-9_]', '_', nome_config).strip('_')
        nome_config_sanitizado = re.sub(r'_+', '_', nome_config_sanitizado)

        # Prepara linhas para adicionar/atualizar
        linhas_config = []
        campos = ['AUDITOR', 'MATRICULA', 'DSF', 'CONTATO', 'ORGAO']

        for campo in campos:
            valor = dados.get(campo, '')
            linhas_config.append(f"CONFIG_{nome_config_sanitizado}_{campo}={valor}")

        # Lê conteúdo atual do .env
        conteudo_atual = ""
        if env_path.exists():
            with open(env_path, 'r', encoding='utf-8') as f:
                conteudo_atual = f.read()

        # Remove configurações antigas com mesmo nome (se houver)
        import re
        conteudo_sem_config_antiga = re.sub(
            rf'^CONFIG_{re.escape(nome_config_sanitizado)}_.*$\n?',
            '',
            conteudo_atual,
            flags=re.MULTILINE
        )

        # Adiciona nova configuração
        secao_existe = '# CONFIGURAÇÕES SALVAS DE AUDITORES' in conteudo_sem_config_antiga

        if not secao_existe:
            # Adiciona cabeçalho da seção
            conteudo_final = conteudo_sem_config_antiga.rstrip() + '\n\n'
            conteudo_final += '# =============================================================================\n'
            conteudo_final += '# CONFIGURAÇÕES SALVAS DE AUDITORES\n'
            conteudo_final += '# =============================================================================\n'
        else:
            conteudo_final = conteudo_sem_config_antiga

        # Adiciona linhas da configuração
        conteudo_final += '\n'.join(linhas_config) + '\n'

        # Escreve de volta no arquivo
        with open(env_path, 'w', encoding='utf-8') as f:
            f.write(conteudo_final)

        logger.info(f"Configuração '{nome_config_sanitizado}' salva em: {env_path}")

    except Exception as e:
        logger.error(f"Erro ao salvar configuração: {e}")
        print(f"[ERRO] Não foi possível salvar a configuração: {e}")


def exibir_relatorio_final(resumo: Dict[str, Any]) -> None:
    """
    Exibe relatório formatado do processamento.

    Args:
        resumo: Dicionário retornado por gerar_notificacoes_em_lote
    """
    print("\n" + "=" * 70)
    print("RELATÓRIO FINAL DO PROCESSAMENTO")
    print("=" * 70)

    # Resumo estatístico
    print(f"\n📊 RESUMO ESTATÍSTICO")
    print(f"   Total de CNPJs processados: {resumo['total']}")
    print(f"   ✅ Sucessos: {resumo['sucessos']}")
    print(f"   ❌ Falhas: {resumo['falhas']}")

    # Taxa de sucesso
    if resumo['total'] > 0:
        taxa_sucesso = (resumo['sucessos'] / resumo['total']) * 100
        print(f"   📈 Taxa de sucesso: {taxa_sucesso:.1f}%")

    # DSF e estrutura de pastas
    print(f"\n📁 ESTRUTURA DE PASTAS")
    dsf = resumo.get('dsf', 'N/A')
    dir_saida = resumo.get('diretorio_saida', 'N/A')
    print(f"   DSF: {dsf}")
    print(f"   Diretório base: {dir_saida}")
    print(f"   Formato: <mes>_DSF_<numero_dsf>/<cnpj>/")
    
    # Arquivo DSF
    arquivo_dsf = resumo.get('arquivo_dsf')
    if arquivo_dsf:
        print(f"   Arquivo PDF da DSF: {arquivo_dsf}")

    # CNPJs agrupados por DSF
    cnpjs_por_dsf = resumo.get('cnpjs_por_dsf', {})
    if cnpjs_por_dsf:
        print(f"\n📋 CNPJs AGRUPADOS POR DSF")
        print("-" * 70)
        for dsf_key, cnpjs_lista in cnpjs_por_dsf.items():
            print(f"\n   DSF: {dsf_key}")
            print(f"   Total de CNPJs: {len(cnpjs_lista)}")
            if cnpjs_lista:
                print(f"   CNPJs:")
                for cnpj in cnpjs_lista[:10]:  # Mostra até 10
                    print(f"      - {cnpj}")
                if len(cnpjs_lista) > 10:
                    print(f"      ... e mais {len(cnpjs_lista) - 10} CNPJ(s)")
        
        # Salva resumo em arquivo TXT para fácil visualização
        try:
            arquivo_txt = dir_saida / "resumo_cnpjs.txt"
            with open(arquivo_txt, 'w', encoding='utf-8') as f:
                f.write("=" * 70 + "\n")
                f.write("RESUMO DE CNPJs POR DSF\n")
                f.write("=" * 70 + "\n\n")
                f.write(f"DSF: {dsf}\n")
                f.write(f"Diretório: {dir_saida}\n")
                f.write(f"Total de CNPJs: {resumo['total']}\n")
                f.write(f"Sucessos: {resumo['sucessos']}\n")
                f.write(f"Falhas: {resumo['falhas']}\n\n")
                f.write("-" * 70 + "\n")
                f.write("CNPJs POR DSF\n")
                f.write("-" * 70 + "\n\n")
                for dsf_key, cnpjs_lista in cnpjs_por_dsf.items():
                    f.write(f"DSF: {dsf_key}\n")
                    f.write(f"Total: {len(cnpjs_lista)} CNPJ(s)\n\n")
                    for cnpj in cnpjs_lista:
                        f.write(f"  {cnpj}\n")
                    f.write("\n")
            print(f"\n   ✅ Resumo salvo em: {arquivo_txt}")
        except Exception as e:
            logger.warning(f"Não foi possível salvar o resumo em TXT: {e}")

    # Detalhes por CNPJ
    print(f"\n📋 DETALHAMENTO POR CNPJ")
    print("-" * 70)

    for cnpj, resultado in resumo['resultados'].items():
        if resultado['sucesso']:
            status = "✅ SUCESSO"
            detalhe = f"Arquivo: {resultado['arquivo_saida']}"
        else:
            status = "❌ FALHA"
            detalhe = f"Erro: {resultado['erro']} (Etapa: {resultado['etapa_falha']})"

        print(f"\n   CNPJ: {cnpj}")
        print(f"   Status: {status}")
        print(f"   {detalhe}")

    # Arquivos gerados
    if resumo['arquivos_gerados']:
        print(f"\n📁 ARQUIVOS GERADOS ({len(resumo['arquivos_gerados'])})")
        print("-" * 70)
        for arquivo in resumo['arquivos_gerados']:
            print(f"   📄 {arquivo}")

        # Instrução para abrir a pasta
        print(f"\n💡 Para visualizar os arquivos:")
        print(f"   1. Abra o Explorer de Arquivos")
        print(f"   2. Navegue até: {dir_saida}")
        print(f"   3. Ou execute: explorer {dir_saida}")

        # Tenta abrir automaticamente no Windows
        try:
            import subprocess
            subprocess.run(['explorer', str(dir_saida)], check=False)
            print(f"\n✅ Explorer aberto em: {dir_saida}")
        except Exception as e:
            logger.warning(f"Não foi possível abrir o explorer: {e}")

    print("\n" + "=" * 70)
    print("Processamento concluído!")
    print("=" * 70)


def perguntar_proxima_acao() -> str:
    """
    Pergunta ao final do processamento se o usuário deseja iniciar uma nova DSF
    ou encerrar o programa.

    Returns:
        "nova_dsf" para iniciar um novo ciclo
        "fechar" para encerrar
    """
    print("\n" + "=" * 70)
    print("PRÓXIMA AÇÃO")
    print("=" * 70)
    print("  [1] Processar nova DSF")
    print("  [2] Fechar o programa")

    while True:
        try:
            opcao = input("\nEscolha uma opção (1-2): ").strip()
            if opcao == '1':
                return "nova_dsf"
            if opcao == '2':
                return "fechar"
            print("❌ Opção inválida. Escolha 1 ou 2.")
        except (KeyboardInterrupt, EOFError):
            print("\n\nEncerrando o programa.")
            return "fechar"


# =============================================================================
# PONTO DE ENTRADA PRINCIPAL
# =============================================================================

def main():
    """
    Função principal executada quando o script é chamado diretamente.

    Oferece três formas de execução:
    1. Via linha de comando: python app.py cnpj1 cnpj2 ...
    2. Via arquivo TXT: python app.py --arquivo cnpjs.txt
    3. Via entrada interativa: python app.py
    """
    logger.info("=" * 60)
    logger.info("INICIANDO GERAÇÃO DE NOTIFICAÇÕES FISCONFORME")
    logger.info("=" * 60)

    # Verifica se há CNPJs na linha de comando
    cnpjs_argumento = sys.argv[1:] if len(sys.argv) > 1 else None

    # Verifica se há argumento de arquivo
    arquivo_cnpjs = None
    if '--arquivo' in sys.argv or '-a' in sys.argv:
        try:
            idx = sys.argv.index('--arquivo') if '--arquivo' in sys.argv else sys.argv.index('-a')
            if idx + 1 < len(sys.argv):
                arquivo_cnpjs = Path(sys.argv[idx + 1])
        except (ValueError, IndexError):
            pass

    # Obtém lista de CNPJs
    if cnpjs_argumento and not arquivo_cnpjs:
        # Filtra argumentos de controle
        cnpjs_argumento = [c for c in cnpjs_argumento if not c.startswith('--') and not c.startswith('-')]
        logger.info(f"CNPJs fornecidos via linha de comando: {len(cnpjs_argumento)}")
        lista_cnpjs = cnpjs_argumento
    elif arquivo_cnpjs:
        # Lê de arquivo TXT
        logger.info(f"Lendo CNPJs do arquivo: {arquivo_cnpjs}")
        lista_cnpjs = listar_cnpjs_para_processamento(arquivo_cnpjs)
    else:
        # Modo interativo com menu de opções
        logger.info("Nenhum CNPJ fornecido via linha de comando. Modo interativo.")
        lista_cnpjs = menu_selecao_cnpjs()

    # Verifica se há CNPJs para processar
    if not lista_cnpjs:
        print("\n⚠️  Nenhum CNPJ fornecido. Encerrando programa.")
        logger.info("Programa encerrado: nenhum CNPJ fornecido")
        return

    # Coleta o arquivo PDF da DSF
    print("\n📄 Coletando arquivo PDF da DSF...")
    arquivo_dsf = coletar_arquivo_dsf()

    # Coleta dados manuais do usuário (AUDITOR, MATRICULA, DSF, CONTATO, ORGAO)
    print("\n📝 Coletando dados do auditor/órgão expedidor...")
    dados_manuais = coletar_dados_manuais()

    # Coleta período de análise
    print("\n📅 Coletando período de análise...")
    periodo_analise = coletar_periodo_analise()
    data_inicial, data_final = periodo_analise

    # Analisa o modelo de notificação
    print("\n📄 Analisando modelo de notificação...")
    analise = analisar_modelo()

    if 'erro' not in analise:
        print(f"   Modelo: {analise['caminho_modelo']}")
        print(f"   Campos necessários: {analise['total_placeholders']}")
        logger.info(f"Modelo analisado: {analise['total_placeholders']} campos")

        # Mostra quais campos serão preenchidos manualmente
        if dados_manuais:
            print(f"   Campos manuais informados: {list(dados_manuais.keys())}")
    else:
        print(f"   ⚠️  Erro ao analisar modelo: {analise.get('erro', 'Desconhecido')}")
        logger.warning(f"Erro ao analisar modelo: {analise.get('erro')}")

    # Processa notificações em lote
    print("\n⚙️  Processando notificações...")
    print(f"   Período: {data_inicial} a {data_final}")
    if arquivo_dsf:
        print(f"   Arquivo DSF: {arquivo_dsf}")
    
    resumo = gerar_notificacoes_em_lote(
        lista_cnpjs=lista_cnpjs,
        dados_manuais=dados_manuais,
        periodo_analise=periodo_analise,
        arquivo_dsf=arquivo_dsf
    )

    # Exibe relatório final
    exibir_relatorio_final(resumo)

    # Log de encerramento
    logger.info("=" * 60)
    logger.info("GERAÇÃO DE NOTIFICAÇÕES CONCLUÍDA")
    logger.info("=" * 60)


    proxima_acao = perguntar_proxima_acao()
    if proxima_acao == "nova_dsf":
        if len(sys.argv) > 1:
            print("\nâ„¹ï¸  Reiniciando em modo interativo para processar uma nova DSF.")
            sys.argv = [sys.argv[0]]
        main()
        return


def menu_selecao_cnpjs() -> List[str]:
    """
    Exibe menu interativo para seleção de origem dos CNPJs.
    
    Oferece três opções:
    1. Digitar CNPJs manualmente
    2. Informar caminho de arquivo TXT
    3. Selecionar arquivo TXT do diretório
    
    Returns:
        Lista de CNPJs para processamento
    """
    print("\n" + "=" * 60)
    print("SELEÇÃO DE CNPJs PARA PROCESSAMENTO")
    print("=" * 60)
    print("\nComo deseja fornecer os CNPJs?")
    print("-" * 60)
    print("  [1] Digitar CNPJs manualmente (um por linha)")
    print("  [2] Informar caminho de arquivo TXT")
    print("  [3] Selecionar arquivo TXT do diretório")
    print("  [4] Sair")
    print("-" * 60)
    
    while True:
        try:
            opcao = input("\nEscolha uma opção (1-4): ").strip()
            
            if opcao == '1':
                # Digitação manual
                return listar_cnpjs_para_processamento()
            
            elif opcao == '2':
                # Informar caminho do arquivo
                try:
                    caminho = input("\nCaminho do arquivo TXT: ").strip()
                    if caminho:
                        arquivo = Path(caminho)
                        if arquivo.exists():
                            return listar_cnpjs_para_processamento(arquivo)
                        else:
                            print(f"❌ Arquivo não encontrado: {arquivo}")
                    else:
                        print("❌ Caminho não informado.")
                except Exception as e:
                    print(f"❌ Erro: {e}")
            
            elif opcao == '3':
                # Selecionar do diretório
                arquivos = listar_arquivos_txt_no_diretorio()
                
                if not arquivos:
                    print("\n⚠️  Nenhum arquivo TXT encontrado no diretório.")
                    print(f"Diretório: {ROOT_DIR}")
                    continue
                
                print(f"\n📂 {len(arquivos)} arquivo(s) TXT encontrado(s):")
                print("-" * 60)
                for i, arquivo in enumerate(arquivos, start=1):
                    print(f"  [{i}] {arquivo.name}")
                print("-" * 60)
                
                try:
                    selecao = input("\nEscolha um arquivo (número): ").strip()
                    if selecao.isdigit() and 1 <= int(selecao) <= len(arquivos):
                        arquivo_selecionado = arquivos[int(selecao) - 1]
                        return listar_cnpjs_para_processamento(arquivo_selecionado)
                    else:
                        print("❌ Seleção inválida.")
                except Exception as e:
                    print(f"❌ Erro: {e}")
            
            elif opcao == '4':
                # Sair
                print("\nOperação cancelada.")
                return []
            
            else:
                print("❌ Opção inválida. Escolha 1, 2, 3 ou 4.")
        
        except (KeyboardInterrupt, EOFError):
            print("\n\nOperação cancelada pelo usuário.")
            return []


# =============================================================================
# EXECUÇÃO DO SCRIPT
# =============================================================================

if __name__ == "__main__":
    main()
