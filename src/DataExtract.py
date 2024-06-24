import os
import sys
import zipfile
import logging
import requests
from bs4 import BeautifulSoup

class Logging:
    """
    Classe responsável por configurar e fornecer o logger para o sistema de extração de dados.
    """

    def __init__(self):
        """
        Inicializa a classe Logging e configura o logger com um handler de arquivo.
        """
        log_directory = 'logging'
        log_file = os.path.join(log_directory, 'extractor.log')

        if not os.path.exists(log_directory):
            os.makedirs(log_directory)

        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

        file_handler = logging.FileHandler(log_file, mode='w', encoding='utf-8')
        file_handler.setLevel(logging.INFO)
        file_handler.setFormatter(formatter)

        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)
        self.logger.addHandler(file_handler)

    def get_logger(self):
        """
        Retorna o logger configurado.

        Retorno:
            logging.Logger: Logger configurado para a aplicação.
        """
        return self.logger


class ExtractionComplete(Exception):
    """
    Exceção personalizada para indicar que a extração dos arquivos está completa.
    """
    pass


class ExtractPRFOpenData:
    """
    Classe responsável por extrair e processar dados abertos da PRF.

    Atributos:
        url (str): URL da página de dados abertos da PRF.
        links (list): Lista de links para download dos arquivos CSV.
        logger (logging.Logger): Logger para registrar eventos da extração.
    """

    def __init__(self, url):
        """
        Inicializa a classe ExtractPRFOpenData com a URL da página de dados abertos.

        Parâmetros:
            url (str): URL da página de dados abertos da PRF.
        """
        self.url = url
        self.links = []
        self.logger = Logging().get_logger()
        self._get_links()
        self._create_directories()

    def _get_links(self):
        """
        Obtém os links para download dos arquivos CSV da página de dados abertos.
        """
        response = requests.get(self.url)
        if response.status_code == 200:
            soup = BeautifulSoup(response.text, 'html.parser')
            self.links = soup.find_all('a', title="Clique aqui para baixar")
            self.logger.info(f"Número de links encontrados: {len(self.links)}")

    def _create_directories(self):
        """
        Cria as pastas necessárias para armazenar os arquivos extraídos, caso não existam.
        """
        directories = ['database', 'database/ocorrencias', 'database/causas', 'database/pessoas']
        for directory in directories:
            if not os.path.exists(directory):
                os.makedirs(directory)
                self.logger.info(f"Pasta '{directory}' criada.")

    def download_and_extract_csv(self, link):
        """
        Baixa e extrai arquivos CSV de um link específico.

        Parâmetros:
            link (bs4.element.Tag): Elemento de link contendo a URL de download.
        """
        file_id = link['href'].split('/')[-3]
        download_link = f"https://drive.google.com/uc?id={file_id}&export=download"
        file_name_zip = f"arquivo_{file_id}.zip"
        self.logger.info(f"Baixando arquivo ZIP: {download_link}")

        r = requests.get(download_link, stream=True)
        with open(file_name_zip, "wb") as f:
            for chunk in r.iter_content(chunk_size=1024):
                if chunk:
                    f.write(chunk)

        extracted_files = []
        if os.path.exists(file_name_zip) and os.path.getsize(file_name_zip) > 0:
            with zipfile.ZipFile(file_name_zip, 'r') as zip_ref:
                extracted_files = zip_ref.namelist()
                zip_ref.extractall("database")
            self.logger.info(f"Arquivo '{file_name_zip}' extraído com sucesso para 'database'")
            os.remove(file_name_zip)
        else:
            self.logger.error(f"Erro ao baixar o arquivo ZIP '{file_name_zip}'!")

        self.rename_files(extracted_files)

    def rename_files(self, extracted_files):
        """
        Renomeia e organiza os arquivos extraídos.

        Parâmetros:
            extracted_files (list): Lista de nomes de arquivos extraídos.
        """
        for file in extracted_files:
            old_file_path = os.path.join("database", file)
            new_file_name, folder = self.generate_new_name_and_folder(file)
            if new_file_name and folder:
                new_file_path = os.path.join(folder, new_file_name)
                if old_file_path != new_file_path:
                    if os.path.exists(new_file_path):
                        os.remove(old_file_path)
                        self.logger.warning(f"Arquivo '{new_file_path}' já existe e será ignorado.")
                    else:
                        if not os.path.exists(folder):
                            os.makedirs(folder)
                        os.rename(old_file_path, new_file_path)
                        self.logger.info(f"Arquivo renomeado de '{old_file_path}' para '{new_file_path}'")

    def generate_new_name_and_folder(self, file_name):
        """
        Gera um novo nome e diretório para um arquivo com base em seu nome original.

        Parâmetros:
            file_name (str): Nome original do arquivo.

        Retorno:
            tuple: Novo nome do arquivo e diretório correspondente.
        """
        year = None
        new_name = None
        folder = None

        if "datatran" in file_name:
            year = file_name.split("datatran")[1][:4]
            new_name = f"{year}_ocorrencias.csv"
            folder = os.path.join("database", "ocorrencias")
        elif "acidentes" in file_name and "_todas_causas_tipos" in file_name:
            year = file_name.split("acidentes")[1][:4]
            new_name = f"{year}_causas.csv"
            folder = os.path.join("database", "causas")
        elif "acidentes" in file_name:
            year = file_name.split("acidentes")[1][:4]
            new_name = f"{year}_pessoas.csv"
            folder = os.path.join("database", "pessoas")

        if year and int(year) < 2017:
            os.remove(os.path.join("database", file_name))
            self.logger.info(f"Todos os arquivos necessários foram baixados!")
            raise ExtractionComplete

        return new_name, folder

    def extract_csv_files(self):
        """
        Extrai todos os arquivos CSV disponíveis nos links obtidos.
        """
        try:
            for link in self.links:
                self.download_and_extract_csv(link)
        except ExtractionComplete:
            self.logger.info("Finalizando extração de arquivos.")
