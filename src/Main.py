import os
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.engine import URL
from unidecode import unidecode

# Diretório onde os arquivos CSV estão localizados
csv_directory = r'C:\Users\Matheus Delcor\Documents\Code\Python\DataSynth\database'

# Configuração da conexão SQLAlchemy para SQL Server com autenticação do Windows
connection_string = URL.create(
    "mssql+pyodbc",
    query={
        "odbc_connect": (
            'DRIVER={ODBC Driver 17 for SQL Server};'
            'SERVER=localhost;'
            'DATABASE=PRF;'
            'Trusted_Connection=yes;'
        )
    }
)
engine = create_engine(connection_string, fast_executemany=True)

def process_csv(file_path, table_name, schema):
    # Leitura do CSV com a codificação 'ISO-8859-1'
    df = pd.read_csv(file_path, delimiter=';', encoding='ISO-8859-1')
    print(f"Arquivo {file_path} lido com sucesso usando encoding 'ISO-8859-1'.")

    # Aplicar unidecode para remover acentuações e caracteres estranhos
    df = df.map(lambda x: unidecode(str(x)) if isinstance(x, str) else x)

    # Insere os dados na tabela correspondente usando SQLAlchemy
    try:
        df.to_sql(table_name, con=engine, schema=schema, if_exists='append', index=False)
        print(f"Dados do arquivo {file_path} inseridos com sucesso na tabela {schema}.{table_name}.")
    except Exception as e:
        print(f"Erro durante a inserção dos dados do arquivo {file_path}: {e}")

# Definições dos arquivos e tabelas correspondentes
files_to_tables = {
    'acidentes2024_todas_causas_tipos.csv': 'ACIDENTES_TODAS_CAUSAS',
    'ocorrencias.csv': 'OCORRENCIAS',
    'pessoas.csv': 'PESSOAS'
}

# Processar cada arquivo CSV conforme a tabela correspondente
for filename, table_name in files_to_tables.items():
    file_path = os.path.join(csv_directory, filename)
    if os.path.exists(file_path):
        process_csv(file_path, table_name, 'BRONZE')
    else:
        print(f"Arquivo {file_path} não encontrado.")
