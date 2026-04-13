import os
from docxtpl import DocxTemplate, InlineImage
from docx.shared import Mm
from pathlib import Path

# Configuração de caminhos base
BASE_DIR = Path("c:/Sistema_react")
# O modelo original será usado como base para garantir fidelidade visual (logotipos e estilos)
TEMPLATE_PATH = BASE_DIR / "modelo" / "Template_Relatorio_fisconforme.docx"
OUTPUT_DIR = BASE_DIR / "dados" / "notificacoes"

class ReportDocxService:
    """
    Serviço especializado na geração de relatórios Microsoft Word (.docx).
    Foco: Manter o visual clássico e os logotipos originais do modelo.
    """

    def __init__(self, template_path: str = str(TEMPLATE_PATH)):
        self.template_path = template_path
        # Validação da existência do modelo antes de qualquer processamento
        if not os.path.exists(self.template_path):
            raise FileNotFoundError(f"Erro: O modelo DOCX não foi localizado em: {self.template_path}")

    def gerar_relatorio(self, dados: dict, output_filename: str) -> str:
        """
        Gera um arquivo .docx preenchido com base no modelo (template).
        
        Args:
            dados: Dicionário contendo as variáveis (Jinja2) para substituição no documento.
            output_filename: Nome desejado para o arquivo de saída.
            
        Returns:
            str: Caminho completo do arquivo gerado com sucesso.
        """
        # DocxTemplate permite usar a engine Jinja2 dentro de arquivos .docx
        # Isso significa que podemos usar tags como {{NOME}} ou {% for item in items %}
        doc = DocxTemplate(self.template_path)
        
        # Renderização do documento: o docxtpl varre o arquivo Word procurando
        # por placeholders e os substitui pelos valores do dicionário 'dados'.
        # Nota: Imagens e estilos originais do Word são preservados automaticamente.
        doc.render(dados)
        
        # Garante a estrutura de diretórios para salvar o resultado
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        
        output_path = OUTPUT_DIR / output_filename
        
        # Salvamento final do documento processado
        doc.save(str(output_path))
        
        return str(output_path)

# Estrutura de dados esperada pelo serviço (exemplo de integração):
# {
#     "RAZAO_SOCIAL": "NOME DA EMPRESA LTDA",
#     "CNPJ": "00.000.000/0001-00",
#     "IE": "123456789",
#     "DSF": "2024/001",
#     "AUDITOR": "Nome do Auditor",
#     "CARGO_TITULO": "Auditor Fiscal de Tributos Estaduais",
#     "MATRICULA": "12345-6",
#     "CONTATO": "contato@sefin.ro.gov.br",
#     "ORGAO_ORIGEM": "SEFIN/RO",
#     "TABELA": [ ... lista de pendências ... ]
# }
