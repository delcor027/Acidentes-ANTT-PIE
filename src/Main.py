import os
import requests
from lxml import html
import pandas as pd

repositorio_csv = r'C:\Users\Matheus Delcor\Documents\Code\Python\DataSynth\database'

if not os.path.exists(repositorio_csv):
    os.makedirs(repositorio_csv)

url_antt = 'https://dados.antt.gov.br/dataset/acidentes-rodovias'

with requests.Session() as session:
    resposta = session.get(url_antt)

    if resposta.status_code == 200:
        pagina = html.fromstring(resposta.text)
        lista = pagina.xpath('//*[@id="dataset-resources"]/ul/li')

        dataframes = []  # Lista para armazenar os DataFrames

        for indice, lista_de_elemento in enumerate(lista, start=1):
            links_dos_arquivos = lista_de_elemento.xpath('./div/ul/li[2]/a/@href')
            concessionaria = lista_de_elemento.xpath('./a/@title')[0]
            concessionaria = concessionaria.replace("Demonstrativos de Acidentes - ", "")

            for arquivos in links_dos_arquivos:
                nome_do_arquivo = arquivos.split('/')[-1]
                if nome_do_arquivo.lower().endswith('.csv'):
                    resposta = session.get(arquivos)

                    if resposta.status_code == 200:
                        caminho_completo = os.path.join(repositorio_csv, nome_do_arquivo)
                        with open(caminho_completo, 'wb') as arquivo:
                            arquivo.write(resposta.content)

                        print(f'Arquivo {nome_do_arquivo} baixado com sucesso do índice {indice}.')

                        try:
                            df = pd.read_csv(caminho_completo, encoding='latin1', delimiter=';', dtype={'coluna2': str}, low_memory=False)
                        except UnicodeDecodeError:
                            df = pd.read_csv(caminho_completo, encoding='utf-8', delimiter=';', dtype={'coluna2': str}, low_memory=False)

                        df.insert(0, 'concessionaria', concessionaria)
                        dataframes.append(df)
                    else:
                        print(f'Falha ao baixar o arquivo {nome_do_arquivo} do índice {indice}.')
                else:
                    print(f'Arquivo {nome_do_arquivo} não é um arquivo CSV ou não atende às condições.')

        df_final = pd.concat(dataframes, ignore_index=True)
        df_final.to_csv(os.path.join(repositorio_csv, 'demostrativo_acidentes.csv'), encoding='latin1', index=False, sep=';')

    else:
        print(f'Falha ao acessar a página: {url_antt}')
