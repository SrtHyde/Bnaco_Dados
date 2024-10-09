import dataclasses
from os.path import join, dirname, abspath
from yaml import load, safe_load
from yaml.loader import SafeLoader
import mysql.connector
from mysql.connector import Error
from sqlalchemy.exc import OperationalError
from sqlalchemy import create_engine, text
import pymysql
from os import system
@dataclasses.dataclass
class Variables:
    """ Variables dataclass """
    ibge_url: str
    prouni_url: str
    files_dir: str
    data_dir_ibge: str
    data_dir_prouni: dict
    database: str
    db_host: str
    db_user: str
    db_password: str
    start_year: int
    limit: int
    title: str

class Config:
    """ Configuration interface """

    def __init__(self) -> None:
        """ Load instance variables """
        env_data = self._load_yaml('env.yaml')
        secrets_data = self._load_yaml('secrets.yaml')
        
        self.vars = Variables(
            ibge_url=env_data.get("url_ibge"),
            prouni_url=env_data.get("url_prouni"),
            database=env_data.get("database"),
            files_dir=env_data.get("resultados_dir"),
            data_dir_ibge=env_data.get("data_dir_ibge"),
            data_dir_prouni=env_data.get("data_dir_prouni"),
            db_host=secrets_data.get("db_host"),
            db_user=secrets_data.get("db_user"),
            db_password=secrets_data.get("db_password"),
            start_year=env_data.get("start_year"),
            limit=env_data.get('limit'),
            title=env_data.get('title')
        )

    def _load_yaml(self, filename):
        """Load YAML file"""
        with open(join(dirname(abspath(__file__)), filename), encoding='utf-8') as file:
            return load(file, Loader=SafeLoader)

    def create_db_connection(self):
        """
        Cria uma conexão com o banco de dados MySQL.
        Se o banco de dados não existir, ele será criado.
        """

        connection_string = f"mysql+pymysql://{self.vars.db_user}:{self.vars.db_password}@{self.vars.db_host}"
        engine = create_engine(connection_string)

        # Tente criar o banco de dados se não existir
        try:
            with engine.connect() as conn:
                conn.execute(text(f"CREATE DATABASE IF NOT EXISTS {self.vars.database}"))
                print(f"Banco de dados '{self.vars.database}' criado ou já existe.")
        except OperationalError as e:
            print(f"Erro ao criar o banco de dados: {e}")
            return None

        # Agora, conecte-se ao banco de dados específico
        connection_string_with_db = f"mysql+pymysql://{self.vars.db_user}:{self.vars.db_password}@{self.vars.db_host}"
        engine_with_db = create_engine(connection_string_with_db)

        try:
            with engine_with_db.connect() as conn:
                print(f"Conectado com sucesso ao banco de dados '{self.vars.database}'")
                return conn  # Retorna a conexão para uso posterior
        except OperationalError as e:
            print(f"Erro ao conectar ao banco de dados '{self.vars.database}': {e}")
            return None

       
