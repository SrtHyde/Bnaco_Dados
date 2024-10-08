import dataclasses
from os.path import join, dirname, abspath
from yaml import load, safe_load
from yaml.loader import SafeLoader
import mysql.connector
from mysql.connector import Error

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
            db_password=secrets_data.get("db_password")
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


        try:
            import mysql.connector
            from mysql.connector import Error

            # Primeiro, conecte-se ao MySQL sem especificar um banco de dados
            connection = mysql.connector.connect(
                host=self.vars.db_host,
                user=self.vars.db_user,
                password=self.vars.db_password
            )

            if connection.is_connected():
                cursor = connection.cursor()
                
                # Tente criar o banco de dados se ele não existir
                cursor.execute(f"CREATE DATABASE IF NOT EXISTS {self.vars.database}")
                
                # Feche a conexão inicial
                cursor.close()
                connection.close()

                # Agora, conecte-se ao banco de dados específico
                connection = mysql.connector.connect(
                    host=self.vars.db_host,
                    user=self.vars.db_user,
                    password=self.vars.db_password,
                    database=self.vars.database
                )

                if connection.is_connected():
                    print(f"Conectado com sucesso ao banco de dados {self.vars.database}")
                    return connection

        except Error as e:
            print(f"Erro ao conectar ao MySQL: {e}")
            return None
        

