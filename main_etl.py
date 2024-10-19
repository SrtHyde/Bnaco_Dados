from config.config import Config
from src.extract_components.extract_ibge import IbgeExtractor
from src.extract_components.extract_prouni import ProuniExtractor
from src.transform import Transform
from models.model import Model
from src.load import Load
class MainETL:
     def __init__(self):
       
         self.model = Model()
         self.transform = Transform()
         self.config = Config()
         self.load = Load()
         self.extract_prouni = ProuniExtractor()
         self.extract_ibge = IbgeExtractor()
         self.data_dir_ibge = self.config.vars.data_dir_ibge
         self.data_dir_prouni = self.config.vars.data_dir_prouni
     def extract_data(self):
         try:
             self.extract_prouni.download() 
             self.extract_ibge.download() 
         except Exception as e:
             print(f"Erro durante extração de dados: {e}")
     def transform_data(self):
         try:
             table = self.transform.execute(self.data_dir_prouni, self.data_dir_ibge)
             print(table)
         except Exception as e:
             print(f"Erro durante transformação de dados: {e}")
         return table
     def load_data(self, table):
         try:
           
             self.load.execute_load(table)
         except Exception as e:
             print(f"Erro durante carga de dados: {e}")
   
     def execute(self):
         self.model.execute()
         self.extract_data()
         self.transformed_tables = self.transform_data()
         self.load_data(self.transformed_tables)
if __name__ == "__main__":
     main = MainETL()
     main.execute()
#config = Config()
#data_dir_ibge = config.vars.data_dir_ibge
#data_dir_prouni = config.vars.data_dir_prouni
#
#extract_prouni = ProuniExtractor()
#extract_prouni.download()
#
#extract_ibge = IbgeExtractor()
#extract_ibge.download()
#
#trasform = Transform()
#trasform.transform(data_dir_prouni, data_dir_ibge)