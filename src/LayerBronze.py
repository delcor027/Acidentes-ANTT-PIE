import os
import pandas as pd
from sqlalchemy import create_engine

class LayerBronze:
    def __init__(self, base_directory, connection_string):
        self.base_directory = base_directory
        self.engine = create_engine(connection_string, fast_executemany=True)
        self.folders_to_tables = {
            'ocorrencias': 'OCORRENCIAS',
            'causas': 'ACIDENTES_TODAS_CAUSAS',
            'pessoas': 'PESSOAS'
        }

    def process_csv(self, file_path, table_name, schema):
        # Leitura do CSV com a codificação 'ISO-8859-1'
        df = pd.read_csv(file_path, delimiter=';', encoding='ISO-8859-1')
        
        # Insere os dados na tabela correspondente usando SQLAlchemy
        try:
            df.to_sql(table_name, con=self.engine, schema=schema, if_exists='append', index=False)
            print(f"Dados do arquivo {file_path} inseridos com sucesso na tabela {schema}.{table_name}.")
        except Exception as e:
            print(f"Erro durante a inserção dos dados do arquivo {file_path}: {e}")

    def process_all_folders(self):
        for folder_name, table_name in self.folders_to_tables.items():
            folder_path = os.path.join(self.base_directory, folder_name)
            if os.path.exists(folder_path) and os.path.isdir(folder_path):
                self.process_files_in_folder(folder_path, table_name)
            else:
                print(f"Pasta {folder_path} não encontrada.")

    def process_files_in_folder(self, folder_path, table_name):
        for filename in os.listdir(folder_path):
            file_path = os.path.join(folder_path, filename)
            if filename.endswith('.csv'):
                self.process_csv(file_path, table_name, 'BRONZE')

if __name__ == '__main__':
    base_directory = r'C:\Users\Matheus Delcor\Documents\Code\Python\DataSynth\database'
    connection_string = 'your_connection_string_here'
    layer_bronze = LayerBronze(base_directory, connection_string)
    layer_bronze.process_all_folders()
