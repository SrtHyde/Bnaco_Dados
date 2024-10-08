# Imports 
from requests import get
from pandas import DataFrame, to_numeric
from bs4 import BeautifulSoup
from os import path, system, remove, makedirs
from glob import glob

class IbgeExtractor:
  def __init__(self):
    '''Define instânncia para extrair dados do IBGE'''
    
    self.url = None # adicionar self.config
    self.limit = None # adicionar self.config

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

  def download(self,url, data_dir, start_year):
    self.url = url
    urls = self.get_links()

    data = urls.query(f"ano >= {start_year}")
    print(data)

    if not path.exists(data_dir):
      print("Criando pasta")
      makedirs(data_dir)


    for doc in data.to_dict('records'):

      file = doc["arquivo"]
      urlname = doc["url"]
      year = doc["ano"]
      destfile = (f'{data_dir}/{file}')

      if destfile in glob(f"{data_dir}/*"):
        print("Arquivo já existe")
        continue

      os_cmd = (
            f"wget --limit-rate {self.limit}k "
            f"--no-check-certificate -c {urlname} -O {destfile}")
      system(os_cmd)

      if ".zip" in file.lower():
        print("Uncompress ZIP file")
        system(f"7z e {destfile} -O{data_dir}")
        print("Remove ZIP file")
        remove(destfile)