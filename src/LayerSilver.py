import pandas as pd
import duckdb
from sqlalchemy import create_engine

class DataCleaner:
    """
    Classe estática responsável pela limpeza de dados.

    Métodos Estáticos:
        clean_sex_column(df): Substitui valores indesejados na coluna 'sexo' por None.
        clean_specific_values(df): Substitui valores indesejados na coluna 'anoFabricacaoVeiculo' por None.
        clean_all(df): Aplica todas as funções de limpeza no DataFrame.
    """

    @staticmethod
    def clean_sex_column(df):
        """
        Substitui valores indesejados na coluna 'sexo' por None.

        Parâmetros:
            df (pandas.DataFrame): O DataFrame a ser limpo.

        Retorna:
            pandas.DataFrame: O DataFrame com a coluna 'sexo' limpa.
        """
        if 'sexo' in df.columns:
            df['sexo'] = df['sexo'].replace(['Ignorado', 'Não Informado'], None)
        return df

    @staticmethod
    def clean_specific_values(df):
        """
        Substitui valores indesejados na coluna 'anoFabricacaoVeiculo' por None.

        Parâmetros:
            df (pandas.DataFrame): O DataFrame a ser limpo.

        Retorna:
            pandas.DataFrame: O DataFrame com a coluna 'anoFabricacaoVeiculo' limpa.
        """
        if 'anoFabricacaoVeiculo' in df.columns:
            df['anoFabricacaoVeiculo'] = df['anoFabricacaoVeiculo'].replace([1917, 1901, 1900, 0], None)
        return df

    @staticmethod
    def clean_all(df):
        """
        Aplica todas as funções de limpeza no DataFrame.

        Parâmetros:
            df (pandas.DataFrame): O DataFrame a ser limpo.

        Retorna:
            pandas.DataFrame: O DataFrame limpo.
        """
        df = DataCleaner.clean_sex_column(df)
        df = DataCleaner.clean_specific_values(df)
        return df


class LayerSilver:
    """
    Classe responsável por processar e transformar dados da camada BRONZE para a camada SILVER do banco de dados.

    Atributos:
        engine (sqlalchemy.engine.base.Engine): A engine de conexão com o banco de dados.
        conn (duckdb.DuckDBPyConnection): Conexão DuckDB em memória.
    """
    
    def __init__(self, connection_string):
        """
        Inicializa a classe LayerSilver com a string de conexão.

        Parâmetros:
            connection_string (str): A string de conexão para o banco de dados.
        """
        self.engine = create_engine(connection_string, fast_executemany=True)
        self.conn = duckdb.connect(':memory:')

    def load_data(self, table_name):
        """
        Carrega dados da tabela BRONZE especificada.

        Parâmetros:
            table_name (str): O nome da tabela BRONZE.

        Retorna:
            pandas.DataFrame: O DataFrame com os dados carregados.
        """
        query = f"SELECT * FROM BRONZE.{table_name}"
        df = pd.read_sql(query, self.engine)
        return df

    def process_and_transform_data(self, table_name):
        """
        Processa e transforma dados de uma tabela BRONZE específica.

        Parâmetros:
            table_name (str): O nome da tabela BRONZE.

        Retorna:
            pandas.DataFrame: O DataFrame transformado.
        """
        df = self.load_data(table_name)
        df = DataCleaner.clean_all(df)
        self.conn.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM df")
        transformed_df = self.conn.execute(f"SELECT * FROM {table_name}").df()
        return transformed_df

    def insert_data_to_sql_server(self, df, table_name, chunksize=500000):
        """
        Insere dados transformados na tabela SILVER do banco de dados.

        Parâmetros:
            df (pandas.DataFrame): O DataFrame a ser inserido.
            table_name (str): O nome da tabela SILVER.
            chunksize (int): O tamanho dos chunks para inserção de dados.
        """
        try:
            total_rows = len(df)
            for i, chunk in enumerate(range(0, total_rows, chunksize)):
                df_chunk = df.iloc[chunk:chunk + chunksize]
                df_chunk.to_sql(table_name, con=self.engine, schema='SILVER', if_exists='append', index=False)
                print(f"Chunk {i + 1} inserido com sucesso na tabela SILVER.{table_name}.")
        except Exception as e:
            print(f"Erro durante a inserção dos dados na tabela SILVER.{table_name}: {e}")

    def process_all_tables(self):
        """
        Processa e transforma dados de todas as tabelas BRONZE especificadas.
        """
        transformed_df = self.process_and_transform_data('PESSOAS')
        self.insert_data_to_sql_server(transformed_df, 'PESSOAS')        

        transformed_df = self.process_and_transform_data('OCORRENCIAS')
        self.insert_data_to_sql_server(transformed_df, 'OCORRENCIAS')    

        transformed_df = self.process_and_transform_data('ACIDENTES_TODAS_CAUSAS')
        self.insert_data_to_sql_server(transformed_df, 'ACIDENTES_TODAS_CAUSAS')
