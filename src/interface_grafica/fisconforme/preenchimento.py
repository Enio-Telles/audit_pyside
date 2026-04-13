"""
Módulo de preenchimento do modelo de notificação.

Este módulo é responsável por ler o modelo de notificação fiscal,
substituir os placeholders {{CAMPO}} pelos dados extraídos do Oracle,
e gerar os arquivos de notificação para cada CNPJ.

Autor: Gerado automaticamente
Data: 2024-04-02
"""

import re
import logging
import base64
import unicodedata
from pathlib import Path
from typing import Dict, Any, Optional, List
import pymupdf as fitz  # PyMuPDF (fitz)

# Importa funções do módulo de extração
from .extracao import (
    limpar_cnpj,
    extrair_dados_cadastrais,
    normalizar_texto_para_chave
)

# Configuração do logging para rastrear execuções
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# =============================================================================
# CONFIGURAÇÕES DE CAMINHOS
# =============================================================================

# Importa resolvedor de caminhos do pacote integrado
from .path_resolver import get_resource_path, get_root_dir, get_modelo_path

# Diretório raiz do projeto
ROOT_DIR = get_root_dir()

# Arquivo do modelo de notificação
MODELO_NOTIFICACAO = get_modelo_path()

# Diretório de saída para as notificações geradas (sempre relativo ao cwd)
DIR_SAIDA_NOTIFICACOES = Path('notificacoes')

# Diretório contendo os PDFs das DSFs
DIR_DSF = ROOT_DIR / "dados" / "fisconforme" / "dsf"

# Ajustes visuais para imagens da DSF no HTML exportado
DSF_RENDER_DPI = 170
DSF_IMG_WIDTH_PX = 565
DSF_IMG_HEIGHT_PX = 800
DSF_IMG_CONTAINER_MARGIN_BOTTOM_PX = 16


def ler_modelo_notificacao(caminho_modelo: Optional[Path] = None) -> Optional[str]:
    """
    Lê o conteúdo do arquivo modelo de notificação.
    
    O modelo contém placeholders no formato {{CAMPO}} que serão substituídos
    pelos dados reais extraídos do banco de dados.
    
    Args:
        caminho_modelo: Caminho opcional para o arquivo modelo.
                       Se None, usa o caminho padrão.
    
    Returns:
        Conteúdo do modelo como string, ou None se houver erro na leitura
    """
    if caminho_modelo is None:
        caminho_modelo = MODELO_NOTIFICACAO
    
    try:
        if not caminho_modelo.exists():
            logger.error(f"Arquivo modelo não encontrado: {caminho_modelo}")
            return None
        
        with open(caminho_modelo, 'r', encoding='utf-8') as arquivo:
            conteudo = arquivo.read()
        
        logger.info(f"Modelo de notificação lido com sucesso: {caminho_modelo}")
        return conteudo
    
    except Exception as e:
        logger.error(f"Erro ao ler arquivo modelo {caminho_modelo}: {e}")
        return None


def identificar_placeholders(conteudo: str) -> List[str]:
    """
    Identifica todos os placeholders únicos no conteúdo do modelo.
    """
    padrao = r'\{\{([A-Z0-9_ÃÁÀÂÊÉÍÓÕÒÚÇ]+)\}\}'
    matches = re.findall(padrao, conteudo)
    placeholders_unicos = sorted(set(matches))
    return placeholders_unicos


def validar_dados_para_preenchimento(
    dados: Dict[str, Any], 
    placeholders: List[str]
) -> Dict[str, str]:
    """
    Valida se todos os placeholders têm dados correspondentes.
    """
    dados_validados = {}
    campos_faltantes = []
    
    for placeholder in placeholders:
        if placeholder in dados:
            valor = str(dados[placeholder]) if dados[placeholder] is not None else ""
            dados_validados[placeholder] = valor.strip()
        else:
            campos_faltantes.append(placeholder)
            dados_validados[placeholder] = "[DADO NÃO DISPONÍVEL]"
    
    if campos_faltantes:
        logger.warning(f"Campos faltantes nos dados: {campos_faltantes}")
    
    return dados_validados


def preencher_modelo(
    conteudo_modelo: str,
    dados: Dict[str, Any],
    dados_manuais: Optional[Dict[str, str]] = None
) -> str:
    """
    Substitui todos os placeholders no modelo pelos dados fornecidos.
    """
    # Identifica todos os placeholders no modelo
    placeholders = identificar_placeholders(conteudo_modelo)

    # Mescla dados manuais e do Oracle em um único dicionário normalizado
    dados_completos = {}
    
    # 1. Processa dados do Oracle
    if dados:
        for chave, valor in dados.items():
            chave_norm = normalizar_texto_para_chave(chave)
            dados_completos[chave_norm] = str(valor).strip() if valor is not None else ""

    # 2. Processa dados manuais (sobrescrevem dados do Oracle se houver conflito)
    if dados_manuais:
        for chave, valor in dados_manuais.items():
            chave_norm = normalizar_texto_para_chave(chave)
            dados_completos[chave_norm] = str(valor).strip() if valor is not None else ""
        logger.info(f"Dados manuais mesclados (normalizados): {list(dados_completos.keys())}")

    # 3. Valida e prepara dados
    placeholders_sem_dsf_imagens = [p for p in placeholders if p != 'DSF_IMAGENS']
    dados_validados = validar_dados_para_preenchimento(dados_completos, placeholders_sem_dsf_imagens)
    
    # Atualiza dados_completos com o que foi validado
    dados_completos.update(dados_validados)

    # Processamento especial para {{DSF_IMAGENS}}
    if 'DSF_IMAGENS' in placeholders:
        dsf_num = dados_completos.get('DSF', '').strip()
        if dsf_num and dsf_num != '[DADO NÃO DISPONÍVEL]':
            try:
                imagens_html = converter_pdf_para_base64_html(dsf_num)
                if imagens_html:
                    dados_completos['DSF_IMAGENS'] = imagens_html
                    logger.info(f"Imagens da DSF {dsf_num} convertidas com sucesso")
                else:
                    dados_completos['DSF_IMAGENS'] = ''
                    logger.warning(f"Nenhuma imagem gerada para DSF {dsf_num}")
            except Exception as e:
                dados_completos['DSF_IMAGENS'] = ''
                logger.error(f"Erro ao converter PDF da DSF {dsf_num}: {e}")
        else:
            dados_completos['DSF_IMAGENS'] = ''
            logger.warning(f"DSF não disponível ou inválida: {dsf_num}")

    # Realiza as substituições
    conteudo_preenchido = conteudo_modelo

    for placeholder in placeholders:
        padrao_placeholder = '{{' + placeholder + '}}'
        valor = dados_completos.get(placeholder, '[DADO NÃO DISPONÍVEL]')
        conteudo_preenchido = conteudo_preenchido.replace(padrao_placeholder, valor)

    # Limpeza pós-preenchimento
    conteudo_preenchido = re.sub(
        r'(\s*<p\s+style="page-break-before:\s*always;">\s*&nbsp;\s*</p>\s*){2,}',
        '<p style="page-break-before: always;">&nbsp;</p>',
        conteudo_preenchido
    )
    
    logger.info("Substituição de placeholders concluída.")
    return conteudo_preenchido


def converter_pdf_para_base64_html(dsf_numero: str) -> str:
    """
    Converte as páginas do PDF da DSF em imagens base64.
    Limita o tamanho das imagens para evitar arquivos muito grandes.
    Renderiza com uma folga visual para não ultrapassar a largura útil da página.
    """
    if not DIR_DSF.exists():
        logger.warning(f"Diretório DSF não encontrado: {DIR_DSF}")
        return ''

    dsf_limpo = re.sub(r'[^0-9]', '', dsf_numero)
    caminho_pdf = None
    for arquivo in DIR_DSF.glob(f"*{dsf_limpo}*.pdf"):
        caminho_pdf = arquivo
        break

    if not caminho_pdf or not caminho_pdf.exists():
        logger.warning(f"Arquivo PDF para DSF {dsf_numero} não encontrado.")
        return ''

    try:
        doc = fitz.open(str(caminho_pdf))
        html_imagens = []

        for i in range(len(doc)):
            page = doc.load_page(i)
            # Usa DPI moderado e largura máxima conservadora para evitar
            # que a imagem da DSF ultrapasse a largura útil da página.
            escala = DSF_RENDER_DPI / 72
            pix = page.get_pixmap(matrix=fitz.Matrix(escala, escala))
            img_data = pix.tobytes("png")
            
            # Verifica tamanho da imagem (limite de 500KB por página)
            if len(img_data) > 500 * 1024:
                logger.warning(f"Página {i+1} da DSF {dsf_numero} excede 500KB ({len(img_data)/1024:.1f}KB)")
            
            base64_data = base64.b64encode(img_data).decode('ascii')

            tag_img = (
                f'<div style="margin-bottom: {DSF_IMG_CONTAINER_MARGIN_BOTTOM_PX}px; text-align: center; page-break-inside: avoid;">\n'
                f'  <img src="data:image/png;base64,{base64_data}" \n'
                f'       width="{DSF_IMG_WIDTH_PX}" height="{DSF_IMG_HEIGHT_PX}" \n'
                f'       style="display: inline-block; width: {DSF_IMG_WIDTH_PX}px; height: {DSF_IMG_HEIGHT_PX}px; border: 1px solid #D0D5DD; box-sizing: border-box;" \n'
                f'       alt="Página {i+1} da DSF {dsf_numero}" />\n'
                f'</div>'
            )
            html_imagens.append(tag_img)

        doc.close()
        logger.info(f"DSF {dsf_numero} convertida: {len(html_imagens)} páginas")
        return "\n".join(html_imagens)
    except fitz.FileDataError as e:
        logger.error(f"Arquivo PDF corrompido ou inválido: {caminho_pdf} - {e}")
        return ''
    except Exception as e:
        logger.error(f"Erro ao converter PDF: {caminho_pdf} - {e}")
        return ''


def salvar_notificacao(conteudo: str, cnpj: str, diretorio_saida: Optional[Path] = None) -> Path:
    """
    Salva a notificação preenchida.
    """
    if diretorio_saida is None:
        diretorio_saida = DIR_SAIDA_NOTIFICACOES
    
    diretorio_saida.mkdir(parents=True, exist_ok=True)
    cnpj_limpo = re.sub(r'[^0-9]', '', cnpj)
    nome_arquivo = f"notificacao_det_{cnpj_limpo}.txt"
    caminho_completo = diretorio_saida / nome_arquivo
    
    with open(caminho_completo, 'w', encoding='utf-8') as arquivo:
        arquivo.write(conteudo)
    
    logger.info(f"Notificação salva em: {caminho_completo}")
    return caminho_completo


def processar_notificacao(
    cnpj: str,
    dados: Dict[str, Any],
    caminho_modelo: Optional[Path] = None,
    diretorio_saida: Optional[Path] = None,
    dados_manuais: Optional[Dict[str, str]] = None
) -> Optional[Path]:
    """
    Orquestra o processo para um CNPJ.
    """
    try:
        logger.info(f"Processando notificação para CNPJ: {cnpj}")
        conteudo_modelo = ler_modelo_notificacao(caminho_modelo)
        if not conteudo_modelo:
            return None
        
        conteudo_preenchido = preencher_modelo(conteudo_modelo, dados, dados_manuais)
        return salvar_notificacao(conteudo_preenchido, cnpj, diretorio_saida)
    except Exception as e:
        logger.error(f"Erro ao processar CNPJ {cnpj}: {e}")
        return None
