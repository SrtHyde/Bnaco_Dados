from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm.exc import NoResultFound
from models.model import Model
from config.config import Config

class Load:
    def __init__(self):
        self.config = Config()
        self.connection = self.config.db_conn_mysql

    def load_data(self, tables):
        cont=1
        for table_name, df in tables.items():
            print(f"\nProcessando tabela {table_name}, {cont}/{len(tables)}")
            model_class = getattr(Model, table_name.capitalize())
            rows_added = 0
            rows_skipped = 0
            cont+=1
            for _, row in df.iterrows():
                try:
                    with self.connection.begin() as transaction:
                        # Verifica se o registro já existe
                        existing = self.connection.query(model_class).filter_by(**row.to_dict()).first()
                        
                        if existing is None:
                            obj = model_class(**row.to_dict())
                            self.connection.add(obj)
                            rows_added += 1
                        else:
                            rows_skipped += 1
                except Exception as e:
                    print(f"Erro ao processar linha na tabela {table_name}: {str(e)}")

            print(f"Tabela {table_name} processada. Linhas adicionadas: {rows_added}, Linhas ignoradas: {rows_skipped}")
    
    
    def execute_load(self, tables):
        # Definindo a ordem de inserção para respeitar as dependências entre as tabelas
        table_order = [
            "regiao", "uf", "municipio", "instituicao", 
            "campus", "turno", "modalidade", "bolsa", "curso",  
            "aluno"]
        
        
        ordered_tables = {table: tables[table] for table in table_order if table in tables}



        self.load_data(ordered_tables)