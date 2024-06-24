import pandas as pd
from sqlalchemy import create_engine

class LayerGold:
    """
    Classe responsável por copiar dados da camada SILVER para a camada GOLD do banco de dados.

    Atributos:
        engine (sqlalchemy.engine.base.Engine): A engine de conexão com o banco de dados.
    """
    
    def __init__(self, connection_string):
        """
        Inicializa a classe LayerGold com a string de conexão.

        Parâmetros:
            connection_string (str): A string de conexão para o banco de dados.
        """
        self.engine = create_engine(connection_string, fast_executemany=True)

    def load_data(self, table_name):
        """
        Carrega dados da tabela SILVER especificada.

        Parâmetros:
            table_name (str): O nome da tabela SILVER.

        Retorna:
            pandas.DataFrame: O DataFrame com os dados carregados.
        """
        query = f"SELECT * FROM SILVER.{table_name}"
        df = pd.read_sql(query, self.engine)
        return df

    def insert_data_to_gold(self, df, table_name, chunksize=500000):
        """
        Insere dados na tabela GOLD do banco de dados.

        Parâmetros:
            df (pandas.DataFrame): O DataFrame a ser inserido.
            table_name (str): O nome da tabela GOLD.
            chunksize (int): O tamanho dos chunks para inserção de dados.
        """
        try:
            total_rows = len(df)
            for i, chunk in enumerate(range(0, total_rows, chunksize)):
                df_chunk = df.iloc[chunk:chunk + chunksize]
                df_chunk.to_sql(table_name, con=self.engine, schema='GOLD', if_exists='append', index=False)
                print(f"Chunk {i + 1} inserido com sucesso na tabela GOLD.{table_name}.")
        except Exception as e:
            print(f"Erro durante a inserção dos dados na tabela GOLD.{table_name}: {e}")

    def copy_all_tables(self):
        """
        Copia dados de todas as tabelas SILVER especificadas para as tabelas GOLD.
        """
        for table_name in ['PESSOAS', 'OCORRENCIAS', 'ACIDENTES_TODAS_CAUSAS']:
            df = self.load_data(table_name)
            self.insert_data_to_gold(df, table_name)