import os
import sys
import zipfile
import logging
import requests
from bs4 import BeautifulSoup

class Logging:
    def __init__(self):
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

        file_handler = logging.FileHandler(r'logging/extractor.log', mode='w', encoding='utf-8')
        file_handler.setLevel(logging.INFO)
        file_handler.setFormatter(formatter)

        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)
        self.logger.addHandler(file_handler)

    def get_logger(self):
        return self.logger


class ExtractionComplete(Exception):
    pass


class ExtractPRFOpenData:
    def __init__(self, url):
        self.url = url
        self.links = []
        self.logger = Logging().get_logger()
        self._get_links()

    def _get_links(self):
        response = requests.get(self.url)
        if response.status_code == 200:
            soup = BeautifulSoup(response.text, 'html.parser')
            self.links = soup.find_all('a', title="Clique aqui para baixar")
            self.logger.info(f"Número de links encontrados: {len(self.links)}")

    def download_and_extract_csv(self, link):
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
            self.logger.info(f"Arquivo '{file_name_zip}' extraído com sucesso para 'Database'")
            os.remove(file_name_zip)
        else:
            self.logger.error(f"Erro ao baixar o arquivo ZIP '{file_name_zip}'!")

        self.rename_files(extracted_files)

    def rename_files(self, extracted_files):
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
        year = None
        new_name = None
        folder = None

        if "datatran" in file_name:
            year = file_name.split("datatran")[1][:4]
            new_name = f"{year}_ocorrencias.csv"
            folder = r"C:\Users\Matheus Delcor\Documents\Code\Python\DataSynth\database\ocorrencias"
        elif "acidentes" in file_name and "_todas_causas_tipos" in file_name:
            year = file_name.split("acidentes")[1][:4]
            new_name = f"{year}_causas.csv"
            folder = r"C:\Users\Matheus Delcor\Documents\Code\Python\DataSynth\database\causas"
        elif "acidentes" in file_name:
            year = file_name.split("acidentes")[1][:4]
            new_name = f"{year}_pessoas.csv"
            folder = r"C:\Users\Matheus Delcor\Documents\Code\Python\DataSynth\database\pessoas"

        if year and int(year) < 2017:
            os.remove(os.path.join("database", file_name))
            self.logger.info(f"Todos os arquivos necessários foram baixados!")
            raise ExtractionComplete

        return new_name, folder

    def extract_csv_files(self):
        try:
            for link in self.links:
                self.download_and_extract_csv(link)
        except ExtractionComplete:
            self.logger.info("Finalizando extração de arquivos.")
