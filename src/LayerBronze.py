import os
import pandas as pd
from sqlalchemy import create_engine

class LayerBronze:
    """ 
    Classe responsável por processar e carregar dados de arquivos CSV para a camada BRONZE do banco de dados.

    Atributos:
        base_directory (str): O diretório base onde os arquivos CSV estão localizados.
        engine (sqlalchemy.engine.base.Engine): A engine de conexão com o banco de dados.
        folders_to_tables (dict): Um dicionário mapeando nomes de pastas para nomes de tabelas no banco de dados.
    """
    
    def __init__(self, base_directory, connection_string):
        """
        Inicializa a classe LayerBronze com o diretório base e a string de conexão.

        Parâmetros:
            base_directory (str): O diretório base onde os arquivos CSV estão localizados.
            connection_string (str): A string de conexão para o banco de dados.
        """
        self.base_directory = base_directory
        self.engine = create_engine(connection_string, fast_executemany=True)
        self.folders_to_tables = {
            'ocorrencias': 'OCORRENCIAS',
            'causas': 'ACIDENTES_TODAS_CAUSAS',
            'pessoas': 'PESSOAS'
        }

    def process_csv(self, file_path, table_name, schema):
        """
        Lê um arquivo CSV e insere os dados na tabela correspondente no banco de dados.

        Parâmetros:
            file_path (str): O caminho para o arquivo CSV.
            table_name (str): O nome da tabela no banco de dados.
            schema (str): O esquema do banco de dados.
        """
        df = pd.read_csv(file_path, delimiter=';', encoding='ISO-8859-1')
        
        try:
            df.to_sql(table_name, con=self.engine, schema=schema, if_exists='append', index=False)
            print(f"Dados do arquivo {file_path} inseridos com sucesso na tabela {schema}.{table_name}.")
        except Exception as e:
            print(f"Erro durante a inserção dos dados do arquivo {file_path}: {e}")

    def process_all_folders(self):
        """
        Processa todos os arquivos CSV em todas as pastas especificadas no dicionário folders_to_tables.
        """
        for folder_name, table_name in self.folders_to_tables.items():
            folder_path = os.path.join(self.base_directory, folder_name)
            if os.path.exists(folder_path) and os.path.isdir(folder_path):
                self.process_files_in_folder(folder_path, table_name)
            else:
                print(f"Pasta {folder_path} não encontrada.")

    def process_files_in_folder(self, folder_path, table_name):
        """
        Processa todos os arquivos CSV em uma pasta específica.

        Parâmetros:
            folder_path (str): O caminho para a pasta contendo os arquivos CSV.
            table_name (str): O nome da tabela no banco de dados.
        """
        for filename in os.listdir(folder_path):
            file_path = os.path.join(folder_path, filename)
            if filename.endswith('.csv'):
                self.process_csv(file_path, table_name, 'BRONZE')
