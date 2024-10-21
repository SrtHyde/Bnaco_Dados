from config.config import Config

from src.extract import Extract
from src.transform import Transform
from models.model import Model
from src.load import Load

class MainETL:
     def __init__(self):
     
         self.model = Model()
         self.transform = Transform()
         self.config = Config()
         self.load = Load()
         self.extract = Extract()
         self.dict_source = self.config.vars.source

     def extract_data(self):
         
         try:
  
            for source in self.dict_source:
                self.extract.execute(source)
            return
         except Exception as e:
             print(f"Erro durante extração de dados: {e}")

     def transform_data(self):
         try:
             table = self.transform.execute()

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

         extract = self.extract_data()

         self.transformed_tables = self.transform.execute()
         self.load_data(self.transformed_tables)

         
if __name__ == "__main__":
    main = MainETL()
    main.execute()
    print('\nProcesso finalizado!!!!')
