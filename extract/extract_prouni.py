# Imports 
from requests import get
from pandas import DataFrame, to_numeric, concat
from bs4 import BeautifulSoup
from os import path, system, remove, makedirs
from glob import glob


class ProuniExtractor:
  def __init__(self):
    self.url = None # adicionar self.config
    self.start_year = None # adicionar self.config
    self.title = None # adicionar self.config
    self.limit = None # adicionar self.config
    self.data_dir = None # adicionar self.config


  def get_links(self):

    response = get(f'{self.url}{self.title}')
    response = BeautifulSoup(response.text, 'html.parser')
    pages = [self.title] + list(set([f"{elem['href']}" for elem in response.find_all(
        "a", {"class": "hasTooltip pagenav"}, href=True) if elem['href']]))

    finaldata = DataFrame()

    for page in pages:
      url_name= f"{self.url}{page}"
      response = get(url_name)
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

      finaldata = concat([finaldata, data], ignore_index=False)
    finaldata = finaldata[~finaldata['url'].str.contains('v3')]

    finaldata = finaldata.copy().query(f"ano >= {self.start_year}")
    return finaldata


  def download(self):

    urls = self.get_links()

    if not path.exists(self.data_dir):
      print("Criando pasta")
      makedirs(self.data_dir)


    for doc in urls.to_dict('records'):
      print(doc)
      file = doc["arquivo"]
      urlname = doc["url"]
      year = doc["ano"]
      destfile = (f'{self.data_dir}/{file}')
      if destfile in glob(f"{self.data_dir}/*"):
        print("Arquivo já existe")
        continue

      os_cmd = (
            f"wget --limit-rate {self.limit}k "
            f"--no-check-certificate -c {urlname} -O {destfile}")
      system(os_cmd)
      if ".zip" in file.lower():
        print("Uncompress ZIP file")
        system(f"7z e {destfile} -O{self.data_dir}")
        print("Remove ZIP file")
        remove(destfile)


