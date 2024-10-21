import dataclasses
import os
from yaml import load
from yaml.loader import SafeLoader
from sqlalchemy.exc import OperationalError
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import OperationalError
from sqlalchemy.ext.automap import automap_base

@dataclasses.dataclass
class Variables:
    """ Variables dataclass """
    source: dict
    database: str
    db_host: str
    db_user: str
    db_password: str
    start_year: int
    limit: int

class Config:
    """ Configuration interface """

    def __init__(self) -> None:
        """ Load instance variables """
        env_data = self._load_yaml('env.yaml')
        secrets_data = self._load_yaml('secrets.yaml')
        
        self.vars = Variables(
            database=env_data.get("database"),
            source=env_data.get("source"),  
            db_host=secrets_data.get("db_host"),
            db_user=secrets_data.get("db_user"),
            db_password=secrets_data.get("db_password"),
            start_year=env_data.get("start_year"),
            limit=env_data.get('limit'),
        )

        connection_string = f"mysql+pymysql://{self.vars.db_user}:{self.vars.db_password}@{self.vars.db_host}"
        engine = create_engine(connection_string)
        session = sessionmaker(bind=engine)()
        dbname = self.vars.database
        engine_url = str(engine.url)

                
        #if engine_url.find(dbname) == -1:
        #    print(engine_url.find(dbname))
        #    session, engine = self.create_db_connection( engine, session, connection_string)
        with engine.connect() as connection:
            result = connection.execute(text(f"SHOW DATABASES LIKE '{dbname}'"))
            db_exists = result.fetchone() is not None
        if not db_exists:
            print(f"Banco de dados '{dbname}' não encontrado. Criando o banco de dados...")
            session, engine = self.create_db_connection(engine, session, connection_string)
        else:
            print(f"Banco de dados '{dbname}' encontrado.")
            connection_string = f"mysql+pymysql://{self.vars.db_user}:{self.vars.db_password}@{self.vars.db_host}/{dbname}"
            engine = create_engine(connection_string)
            session = sessionmaker(bind=engine)()

        
        self.db_conn_mysql = session
        self.engine_conn_mysql = engine

    def _load_yaml(self, filename):
        """Load YAML file"""
        with open(os.path.join(os.path.dirname(os.path.abspath(__file__)), filename), encoding='utf-8') as file:
            return load(file, Loader=SafeLoader)

    def create_db_connection(self, engine, session, connection_string):
        """    
        Se o banco de dados não existir, ele será criado.
        """

        # Tena criar o banco de dados se não existir
        try:
            with engine.connect() as conn:
                conn.execute(text(f"CREATE DATABASE IF NOT EXISTS {self.vars.database}"))
        except OperationalError as e:
            print(f"Erro ao criar o banco de dados: {e}")
            return None

        # Agora, conecta-se ao banco de dados específico
        connection_string_with_db = f"{connection_string}/{self.vars.database}"
        engine = create_engine(connection_string_with_db)
        session = sessionmaker(bind=engine)()

        try:
            # Configurar o automap
            automap = automap_base()
            automap.prepare(engine, reflect=True)
            self.automap = automap
            # Armazenar conexões
            self.db_conn_mysql = session
            self.engine_conn_mysql = engine

            print(f"Conectado com sucesso ao banco de dados '{self.vars.database}'")

            return session, engine
        except OperationalError as e:
            print(f"Erro ao conectar ao banco de dados '{self.vars.database}': {e}")
            return None
        
