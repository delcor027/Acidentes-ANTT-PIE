import pandas as pd
import duckdb
from sqlalchemy import create_engine

class DataCleaner:
    @staticmethod
    def clean_sex_column(df):
        if 'sexo' in df.columns:
            df['sexo'] = df['sexo'].replace(['Ignorado', 'Não Informado'], None)
        return df

    @staticmethod
    def clean_specific_values(df):
        columns_to_check = ['anoFabricacaoVeiculo', 'anoFabricacao_veiculo']
        for col in columns_to_check:
            if col in df.columns:
                df[col] = df[col].replace(['1917', '1901', '1900', '0'], None)
        return df

    @staticmethod
    def clean_all(df):
        df = DataCleaner.clean_sex_column(df)
        df = DataCleaner.clean_specific_values(df)
        return df


class LayerSilver:
    def __init__(self, connection_string):
        self.engine = create_engine(connection_string, fast_executemany=True)
        self.conn = duckdb.connect(':memory:')

    def load_data(self, table_name):
        query = f"SELECT * FROM BRONZE.{table_name}"
        df = pd.read_sql(query, self.engine)
        return df

    def rename_columns_acidentes_todas_causas(self, df):
        df.columns = [
            'idOcorrencias', 'idPessoas', 'data', 'diaSemana', 'horario', 'uf', 'br', 'km', 'municipio',
            'causaPrincipal', 'causaAcidente', 'ordemTipoAcidente', 'tipoAcidente', 'classificacaoAcidente',
            'faseDia', 'sentidoVia', 'condicaoMetereologica', 'tipoPista', 'tracadoVia', 'usoSolo', 'idVeiculo',
            'tipoVeiculo', 'marca', 'anoFabricacaoVeiculo', 'tipoEnvolvido', 'estadoFisico', 'idade', 'sexo',
            'ilesos', 'feridosLeves', 'feridosGraves', 'mortos', 'latitude', 'longitude', 'regional', 'delegacia', 'uop'
        ]
        return df

    def rename_columns_ocorrencias(self, df):
        df.columns = [
            'idOcorrencias', 'data', 'diaSemana', 'horario', 'uf', 'br', 'km', 'municipio', 'causaAcidente',
            'tipoAcidente', 'classificacaoAcidente', 'faseDia', 'sentidoVia', 'condicaoMetereologica', 'tipoPista',
            'tracadoVia', 'usoSolo', 'pessoas', 'mortos', 'feridosLeves', 'feridosGraves', 'ilesos', 'ignorados',
            'feridos', 'veiculos', 'latitude', 'longitude', 'regional', 'delegacia', 'uop'
        ]
        return df

    def rename_columns_pessoas(self, df):
        df.columns = [
            'idPessoas', 'idOcorrencias', 'data', 'diaSemana', 'horario', 'uf', 'br', 'km', 'municipio',
            'causaAcidente', 'tipoAcidente', 'classificacaoAcidente', 'faseDia', 'sentidoVia', 'condicaoMetereologica',
            'tipoPista', 'tracadoVia', 'usoSolo', 'idVeiculo', 'tipoVeiculo', 'marca', 'anoFabricacao_veiculo',
            'tipoEnvolvido', 'estadoFisico', 'idade', 'sexo', 'ilesos', 'feridosLeves', 'feridosGraves', 'mortos',
            'latitude', 'longitude', 'regional', 'delegacia', 'uop'
        ]
        return df

    def process_and_transform_data(self, table_name, rename_func):
        # Carregar dados do SQL Server
        df = self.load_data(table_name)

        # Renomear colunas
        df = rename_func(df)

        # Limpar dados
        df = DataCleaner.clean_all(df)

        # Inserir os dados transformados de volta em DuckDB
        self.conn.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM df")
        transformed_df = self.conn.execute(f"SELECT * FROM {table_name}").df()

        return transformed_df

    def insert_data_to_sql_server(self, df, table_name, chunksize=500000):
        try:
            total_rows = len(df)
            for i, chunk in enumerate(range(0, total_rows, chunksize)):
                df_chunk = df.iloc[chunk:chunk + chunksize]
                df_chunk.to_sql(table_name, con=self.engine, schema='SILVER', if_exists='append', index=False)
                print(f"Chunk {i + 1} inserido com sucesso na tabela SILVER.{table_name}.")
        except Exception as e:
            print(f"Erro durante a inserção dos dados na tabela SILVER.{table_name}: {e}")

    def process_all_tables(self):
        # Processar e transformar dados da tabela OCORRENCIAS
        transformed_df = self.process_and_transform_data('OCORRENCIAS', self.rename_columns_ocorrencias)
        self.insert_data_to_sql_server(transformed_df, 'OCORRENCIAS')    

        # Processar e transformar dados da tabela PESSOAS
        transformed_df = self.process_and_transform_data('PESSOAS', self.rename_columns_pessoas)
        self.insert_data_to_sql_server(transformed_df, 'PESSOAS')

        # Processar e transformar dados da tabela ACIDENTES_TODAS_CAUSAS
        transformed_df = self.process_and_transform_data('ACIDENTES_TODAS_CAUSAS', self.rename_columns_acidentes_todas_causas)
        self.insert_data_to_sql_server(transformed_df, 'ACIDENTES_TODAS_CAUSAS')