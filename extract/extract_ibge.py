# Imports 
from requests import get
from pandas import DataFrame, to_numeric
from bs4 import BeautifulSoup
from os import path, system, remove, makedirs, getcwd
from glob import glob
from config.config import Config

class IbgeExtractor:
  def __init__(self):
    '''Define instância para extrair dados do IBGE'''
    self.config = Config()
    self.url = self.config.vars.ibge_url # adicionar self.config
    self.limit = self.config.vars.limit # adicionar self.config
    self.start_year = self.config.vars.start_year

    self.caminho_atual = getcwd()
    self.data_dir = f'{self.caminho_atual}{self.config.vars.data_dir_ibge}'

  def get_links(self):

    response = get(f'{self.url}')
    response = BeautifulSoup(response.text, 'html.parser')

    data = DataFrame(
                      {"url": [f"{elem['href']}"
                               for elem in response.find_all("a", href=True)
                              if elem["href"].endswith(('.csv', '.zip'))]}
                  ).assign(
                      arquivo=lambda x: x.url.str.replace(".*/", "", regex=True),
                      ano=lambda x: to_numeric(x.arquivo.str.findall("[0-9]+").str.join(""), errors="coerce"
                      ).astype(int),
                   )

    return data

  def download(self):
    urls = self.get_links()

    data = urls.query(f"ano >= {self.start_year}")
    print(data)

    if not path.exists(self.data_dir):
      print("Criando pasta")
      makedirs(self.data_dir)


    for doc in data.to_dict('records'):

      file = doc["arquivo"]
      urlname = doc["url"]
      year = doc["ano"]
      destfile = (f'{self.data_dir}\{file}')
      
      print(destfile)

      if destfile in glob(f"{self.data_dir}\*"):
        print("Arquivo já existe")
        continue

      os_cmd = (
        f"curl --limit-rate {self.limit}K "
        f"--insecure -C - {urlname} -o {destfile}"
        )
      system(os_cmd)
      if ".zip" in file.lower():
        print("Uncompress ZIP file")
        system(f"7z e {destfile} -O{self.data_dir}")
        print("Remove ZIP file")
        remove(destfile)
        

      else: 
        print('Arquivo já baixado')