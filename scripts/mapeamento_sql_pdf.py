import os
import glob
import re
import oracledb
import sqlglot
import polars as pl
from dotenv import load_dotenv

# Dependência PyMuPDF importada como fitz
try:
    import fitz
except ImportError:
    print("Erro ao importar fitz. Certifique-se de que PyMuPDF está instalado.")

load_dotenv("c:/Sistema_react/.env")

SQL_DIR = "c:/Sistema_react/sql"
MAPPED_DIR = "c:/Sistema_react/dados/referencias/referencias/mapeamento"
PDF_DIR = "c:/Sistema_react/dados/referencias"

# Configuração de Logs Simples
def log_info(msg):
    print(f"[INFO] {msg}")

def extract_tables_from_sqls():
    """Lê arquivos .sql e extrai a lista de schemas e tabelas usando sqlglot."""
    tables_found = set()
    for sql_file in glob.glob(os.path.join(SQL_DIR, "*.sql")):
        with open(sql_file, "r", encoding="utf-8") as f:
            sql_content = f.read()
            
        try:
            # sqlglot pode interpretar múltiplos tipos, oracle é o mais próximo
            for statement in sqlglot.parse(sql_content, dialect="oracle"):
                if not statement:
                    continue
                # Encontra todas as tabelas (Table object)
                for table in statement.find_all(sqlglot.exp.Table):
                    schema = table.args.get('db')
                    name = table.args.get('this')
                    if schema and name:
                        schema_name = schema.name.upper()
                        table_name = name.name.upper()
                        # Filtramos dual
                        if table_name != 'DUAL':
                            tables_found.add((schema_name, table_name))
                    elif name:
                        # Se não há esquema explícito nos sqls mas precisamos mapear, 
                        # podemos considerar schema padrão vazio, ou não processar.
                        pass
        except Exception as e:
            # Fallback regex para arquivos que sqlglot eventualmente não conseguir ler
            # Encontra "schema.tabela"
            matches = re.findall(r'\b([a-zA-Z_0-9]+)\.([a-zA-Z_0-9]+)\b', sql_content)
            for s, t in matches:
                # Filtrar palavras chaves 
                if s.upper() not in ['D', 'C100', 'M', 'O', 'E', 'NVL', 'TO_CHAR', 'SUBSTR', 'TO_DATE'] and t.upper() != 'DUAL':
                    # Exemplo simples para evitar falso positivo do alias
                    if len(s) > 2 and len(t) > 3:
                        tables_found.add((s.upper(), t.upper()))

    return list(tables_found)

def extract_sql_comments():
    """Lê os arquivos .sql e extrai comentários que referenciam campos/colunas (ex: d.coluna, -- descricao co...)."""
    sql_comments = {}
    for sql_file in glob.glob(os.path.join(SQL_DIR, "*.sql")):
        with open(sql_file, "r", encoding="utf-8") as f:
            for line in f:
                if '--' in line:
                    # tenta capturar algo como : alias.coluna_name , -- comentário texto
                    match = re.search(r'(?:[A-Za-z0-9_]+\.)?([A-Za-z0-9_]+)\s*(?:AS\s+[A-Za-z0-9_]+)?\s*,?\s*--\s*(.*)', line, re.IGNORECASE)
                    if match:
                        col_name = match.group(1).upper()
                        comment = match.group(2).strip()
                        if col_name and comment:
                            sql_comments[col_name] = comment
    return sql_comments

def get_all_tables_from_schemas(schemas):
    """Conecta no banco de dados e traz os nomes de todas as tabelas e views dos schemas identificados."""
    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")
    host = os.getenv("ORACLE_HOST")
    port = os.getenv("ORACLE_PORT", "1521")
    service = os.getenv("ORACLE_SERVICE")

    dsn = oracledb.makedsn(host, port, service_name=service)
    
    tables = []
    try:
        with oracledb.connect(user=user, password=password, dsn=dsn) as connection:
            with connection.cursor() as cursor:
                placeholders = ', '.join([f":{i+1}" for i in range(len(schemas))])
                query = f"""
                    SELECT OWNER, OBJECT_NAME 
                    FROM ALL_OBJECTS 
                    WHERE OWNER IN ({placeholders})
                      AND OBJECT_TYPE IN ('TABLE', 'VIEW')
                    ORDER BY OWNER, OBJECT_NAME
                """
                cursor.execute(query, list(schemas))
                for row in cursor:
                    tables.append((row[0], row[1]))
    except Exception as e:
        log_info(f"Aviso - Não foi possível ler listagem de objetos para os schemas {schemas}: {e}")
    
    return tables

def get_columns_from_db(schema_name, table_name):
    """Conecta no banco de dados e traz todos os campos de uma tabela específica."""
    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")
    host = os.getenv("ORACLE_HOST")
    port = os.getenv("ORACLE_PORT", "1521")
    service = os.getenv("ORACLE_SERVICE")

    dsn = oracledb.makedsn(host, port, service_name=service)
    
    records = []
    try:
        with oracledb.connect(user=user, password=password, dsn=dsn) as connection:
            with connection.cursor() as cursor:
                query = """
                    SELECT COLUMN_NAME, DATA_TYPE, DATA_LENGTH, DATA_PRECISION, NULLABLE
                    FROM ALL_TAB_COLUMNS
                    WHERE OWNER = :owner
                      AND TABLE_NAME = :table_name
                    ORDER BY COLUMN_ID
                """
                cursor.execute(query, {"owner": schema_name, "table_name": table_name})
                for row in cursor:
                    records.append({
                        "COLUNA": row[0],
                        "TIPO_DADO": f"{row[1]} ({row[2]},{row[3]})" if row[3] else f"{row[1]} ({row[2]})",
                        "NULLABLE": row[4]
                    })
    except Exception as e:
        log_info(f"Aviso - Não foi possível ler colunas para {schema_name}.{table_name}: {e}")
    
    return records

def build_pdf_corpus():
    """Lê todo o texto dos PDFs em memória para poder criar um motorzinho de busca heurística."""
    pdf_files = glob.glob(os.path.join(PDF_DIR, "*.pdf"))
    corpus = []
    
    for pdf_path in pdf_files:
        filename = os.path.basename(pdf_path)
        log_info(f"Lendo PDF: {filename}...")
        try:
            doc = fitz.open(pdf_path)
            lines = []
            for page in doc:
                text = page.get_text()
                # Separar as linhas (eliminando quebras de página complexas)
                for line in text.split('\n'):
                    cl = line.strip()
                    if cl:
                        lines.append(cl)
            corpus.append({"arquivo": filename, "linhas": lines})
        except Exception as e:
            log_info(f"Erro lendo PDF {filename}: {e}")
            
    return corpus

def infer_description_from_pdf(column_name, pdf_corpus):
    """Busca o column_name nos PDFs e captura o contexto próximo que pode ser a explicação."""
    # Para o MOC Mapeamento: os nomes do banco (ex: TOT_VBC) possuem partes chave (ex: VBC).
    # Vamos fazer uma heurística simples: buscar pela exata coluna ou por partes da coluna separada por _
    parts = [p for p in column_name.split("_") if len(p) > 2]
    
    # Se a coluna for curta ex: VBC
    if len(column_name) >= 3:
        target_exact = column_name.upper()
    else:
        target_exact = "NON_MATCH" # só para evitar falsos curtos demais
        
    for doc in pdf_corpus:
        linhas = doc['linhas']
        for i, line in enumerate(linhas):
            line_up = line.upper()
            
            # Heurística 1: Exact match com prefixo (tipo TAG XML de MOC: <vBC> ou - vBC)
            if target_exact in line_up.split() or f"<{target_exact}>" in line_up:
                # Retorna esta linha e a próxima como contexto descritivo
                context = []
                for offset in range(-1, 3):
                    if 0 <= i + offset < len(linhas):
                        context.append(linhas[i + offset])
                return {"origem": doc['arquivo'], "descricao": " | ".join(context)}
                
            # Heurística 2: Part match (se tem parts chaves do banco de dados e está perto da palavra "Descrição" ou "Campo")
            # Mas vamos ser conservadores para não ler lixo
            
    return {"origem": "ND", "descricao": "Sem descrição encontrada no PDF."}

def main():
    os.makedirs(MAPPED_DIR, exist_ok=True)
    
    log_info("Extraindo referências de tabelas dos SQLs...")
    tabelas = extract_tables_from_sqls()
    log_info(f"Encontrados schemas/tabelas nos SQLs: {tabelas}")
    
    schemas_encontrados = list(set([s for s, t in tabelas if s.isalnum() or '_' in s]))
    if not schemas_encontrados:
        log_info("Nenhum schema válido encontrado nos SQLs. Finalizando.")
        return
        
    log_info(f"Schemas considerados para varredura completa: {schemas_encontrados}")
    
    log_info("Extraindo dicionário de descrições dos próprios SQLs...")
    sql_comments = extract_sql_comments()

    log_info("Lendo Corpus de PDFs...")
    corpus = build_pdf_corpus()
    
    log_info("Buscando a listagem de TODAS as tabelas dos schemas no Banco de Dados...")
    tabelas_db = get_all_tables_from_schemas(schemas_encontrados)
    
    if not tabelas_db:
        log_info("Nenhuma tabela retornada do Banco de Dados para os schemas identificados.")
        return
        
    log_info(f"Iniciando processamento de {len(tabelas_db)} tabelas extraídas do Banco de Dados...")
    
    pulados = 0
    processados = 0
    
    for schema, tabela in tabelas_db:
        sub_dir = os.path.join(MAPPED_DIR, schema.lower())
        os.makedirs(sub_dir, exist_ok=True)
        
        out_file = os.path.join(sub_dir, f"{schema.lower()}_{tabela.lower()}.parquet")
        
        # Recomeçar de onde parou: pular se arquivo já existe
        if os.path.exists(out_file):
            pulados += 1
            continue
            
        log_info(f"Processando {schema}.{tabela}...")
        colunas = get_columns_from_db(schema, tabela)
        
        if not colunas:
            continue
            
        dados_finais = []
        for col in colunas:
            col_name = col["COLUNA"].upper()
            
            # Prioridade 1: Comentários no próprio SQL
            desc_sql = sql_comments.get(col_name, "")
            
            # Prioridade 2: Heurística no PDF
            pdf_info = infer_description_from_pdf(col_name, corpus)
            
            # Seleção consolidada
            desc_consolidada = desc_sql if desc_sql else pdf_info["descricao"]
            
            dados_finais.append({
                "schema": schema,
                "tabela": tabela,
                "coluna": col_name,
                "tipo_dado": col["TIPO_DADO"],
                "nullable": col["NULLABLE"],
                "origem_pdf": pdf_info["origem"],
                "descricao_pdf": pdf_info["descricao"],
                "descricao_sql": desc_sql,
                "descricao_consolidada": desc_consolidada
            })
            
        # Criar DataFrame com Polars e escrever parquet
        df = pl.DataFrame(dados_finais)
        df.write_parquet(out_file)
        processados += 1
        
    log_info(f"Processo concluído: {processados} novas tabelas mapeadas, {pulados} já existiam.")

if __name__ == '__main__':
    main()
