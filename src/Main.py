from sqlalchemy.engine import URL
from LayerBronze import LayerBronze

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

# Instanciar a classe RawLayer e processar os arquivos
raw_layer = LayerBronze(csv_directory, connection_string)
raw_layer.process_all_files()
