import os
import pandas as pd
from sqlalchemy import create_engine

class LayerBronze:
    def __init__(self, csv_directory, connection_string):
        self.csv_directory = csv_directory
        self.engine = create_engine(connection_string, fast_executemany=True)
        self.files_to_tables = {
            'acidentes2024_todas_causas_tipos.csv': 'ACIDENTES_TODAS_CAUSAS',
            'ocorrencias.csv': 'OCORRENCIAS',
            'pessoas.csv': 'PESSOAS'
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

    def process_all_files(self):
        for filename, table_name in self.files_to_tables.items():
            file_path = os.path.join(self.csv_directory, filename)
            if os.path.exists(file_path):
                self.process_csv(file_path, table_name, 'BRONZE')
            else:
                print(f"Arquivo {file_path} não encontrado.")
