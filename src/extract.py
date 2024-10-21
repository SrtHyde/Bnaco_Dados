# Imports 
import requests
from requests import get
from pandas import DataFrame, to_numeric, concat
from bs4 import BeautifulSoup
from os import path, makedirs, getcwd, remove
from glob import glob
import sys
import zipfile
import io
from config.config import Config
from models.model import Model
import datetime
import os

class Extract:
    def __init__(self):

        self.config = Config()
        self.connection = self.config.db_conn_mysql
        self.engine = self.config.engine_conn_mysql
        self.dict_source = self.config.vars.source
        self.caminho_atual = getcwd()

        self.start_year = self.config.vars.start_year
        self.limit = self.config.vars.limit
        self.project_root = os.path.abspath(os.path.join(os.getcwd()))

    def get_links_prouni(self):
        url = f'{self.url}{self.title}'
        print(url)
        response = get(url)

        response = BeautifulSoup(response.text, 'html.parser')
        pages = [self.title] + list(set([f"{elem['href']}" for elem in response.find_all(
            "a", {"class": "hasTooltip pagenav"}, href=True) if elem['href']]))

        finaldata = DataFrame()

        for page in pages:
            url_name = f"{self.url}{page}"
            response = get(url_name)
            response = BeautifulSoup(response.text, 'html.parser')

            data = DataFrame(
                {"url": [f"{elem['href']}"
                         for elem in response.find_all("a", href=True)
                         if elem["href"].endswith(('.csv', '.zip'))]}
            ).assign(
                arquivo=lambda x: x.url.str.replace(".*/", "", regex=True),
                ano=lambda x: to_numeric(x.arquivo.str.findall("[0-9]+").str.join(""), errors="coerce"),
                source='Prouni'
            )

            finaldata = concat([finaldata, data], ignore_index=False)
        
        finaldata = finaldata[~finaldata['url'].str.contains('v3')].query(f"ano >= {self.start_year}")
        return finaldata

    def get_links_ibge(self):
        response = get(f'{self.url}')
        response = BeautifulSoup(response.text, 'html.parser')

        finaldata = DataFrame(
            {"url": [f"{elem['href']}"
                     for elem in response.find_all("a", href=True)
                     if elem["href"].endswith(('.csv', '.zip'))]}
        ).assign(
            arquivo=lambda x: x.url.str.replace(".*/", "", regex=True),
            ano=lambda x: to_numeric(x.arquivo.str.findall("[0-9]+").str.join(""), errors="coerce"),
            source='Ibge'
        )

        return finaldata

    def get_links(self, source):
        if source == 'prouni':
            finaldata = self.get_links_prouni()
        elif source == 'ibge':
            finaldata = self.get_links_ibge()
        else:
            raise ValueError("Source must be 'prouni' or 'ibge'")

        try:
            existing_files = self.connection.query(Model.DownloadRegistry.file_name).all()
            existing_files = [file[0] for file in existing_files]
        except Exception as e:
            print(f"Erro ao buscar arquivos existentes: {e}")
            existing_files = []

        if len(existing_files) > 0:
            finaldata = finaldata[~finaldata['arquivo'].isin(existing_files)]

        return finaldata

    def registry_download(self, file_name, status):
        new_registry = Model.DownloadRegistry(
            file_name=file_name,
            download_date=datetime.datetime.now(),
            status=status
        )
        self.connection.add(new_registry)
        self.connection.commit()

    def download(self, urls : dict):
       

        if len(urls) == 0:
            print("Nenhum arquivo novo encontrado para download")
            return None

        if not path.exists(self.data_dir):
            print(f"Criando pasta: {self.data_dir}")
            makedirs(self.data_dir)

        for doc in urls.to_dict('records'):
            file = doc["arquivo"]
            urlname = doc["url"]

            destfile = f'{self.data_dir}\{file}'

            try:
                response = requests.get(urlname, stream=True)
                response.raise_for_status()
                print(f"\nBaixando arquivo {file}")

                with open(destfile, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        f.write(chunk)
                print(f"Arquivo {file} baixado com sucesso")

                self.registry_download(file, 'downloaded')

                if file.lower().endswith('.zip'):

                    print(f"\nDescompactando arquivo ZIP")
        
                    
                    with zipfile.ZipFile(destfile, 'r') as zip_ref:

                        zip_ref.extractall(self.data_dir)

                    print("Removendo arquivo ZIP\n")
                    remove(destfile)

            except requests.RequestException as e:
                print(f"Erro ao baixar o arquivo {file}: {e}")

    def execute(self, source: str):
        self.source = self.dict_source[source]
        self.url = self.source['url']
        self.data_dir = f'{self.project_root}{self.source["data_dir"]}'
        self.title = self.source['title']

        urls = self.get_links(source)
        download = self.download(urls) 
        
        return download