import os
from sqlalchemy.engine import URL
from LayerBronze import LayerBronze
from DataExtract import ExtractPRFOpenData, ExtractionComplete
from LayerSilver import LayerSilver
from LayerGold import LayerGold

def main():
    """
    Função principal para extrair, processar e carregar dados em várias camadas do banco de dados.
    
    Etapas:
    1. Extrair e organizar os dados a partir de um URL.
    2. Processar e carregar os dados na camada bronze.
    3. Processar e carregar os dados na camada silver.
    4. Copiar os dados da camada silver para a camada gold.
    """
    # Etapa 1: Extrair e organizar os dados
    url = "https://www.gov.br/prf/pt-br/acesso-a-informacao/dados-abertos/dados-abertos-da-prf"
    extractor = ExtractPRFOpenData(url)

    try:
        extractor.extract_csv_files()
    except ExtractionComplete:
        print("Finalizando extração de arquivos, continuando com as próximas etapas.")
    
    # Etapa 2: Processar e carregar os dados na camada bronze
    base_directory = os.path.join(os.getcwd(), 'database')
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

    layer_bronze = LayerBronze(base_directory, connection_string)
    layer_bronze.process_all_folders()

    # Etapa 3: Processar e carregar os dados na camada silver
    layer_silver = LayerSilver(connection_string)
    layer_silver.process_all_tables()

    # Etapa 4: Copiar os dados da camada silver para a camada gold
    layer_gold = LayerGold(connection_string)
    layer_gold.copy_all_tables()

if __name__ == '__main__':
    main()
