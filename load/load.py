from sqlalchemy.orm import Session
from models.model import Model  
from config.config import Config
class Load:


    def __init__(self, connection_function):
        self.config = Config()
        self.connection = self.cofig.db_create_connection()

    def load_data(self, tables):
        with self.connection() as session:
            try:
                for table_name, df in tables.items():
                    model_class = getattr(Model, table_name.capitalize())
                    for _, row in df.iterrows():
                        obj = model_class(**row.to_dict())
                        session.add(obj)
                    session.commit()
                    print(f"Dados inseridos com sucesso na tabela {table_name}")
            except Exception as e:
                session.rollback()
                print(f"Erro ao inserir dados: {str(e)}")

    def execute_load(self, tables):
        # Definindo a ordem de inserção para respeitar as dependências entre as tabelas
        table_order = [
            "regiao", "uf", "municipio", "instituicao", "campus", "turno",
            "curso", "modalidade", "tipo_bolsa", "beneficiario"
        ]
        
        ordered_tables = {table: tables[table] for table in table_order if table in tables}
        self.load_data(ordered_tables)