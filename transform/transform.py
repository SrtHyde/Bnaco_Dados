from requests import get
from pandas import DataFrame, to_numeric, read_csv, concat, to_datetime, Series, read_excel
from unidecode import unidecode
from bs4 import BeautifulSoup
from os import path, system, remove, makedirs
from glob import glob
from re import sub, search


class Transform:
    def __init__(self, data_dir_ibge, data_dir_prouni):
        """Inicializa a classe com os diretórios de dados do IBGE e Prouni."""
        self.data_dir_ibge = data_dir_ibge
        self.data_dir_prouni = data_dir_prouni
        self.prouni_data = None  # Inicializa a variável que armazenará os dados do Prouni
        self.table_ibge = None   # Inicializa a variável que armazenará os dados do IBGE

    def ibge(self, data):
        """Transforma os dados do IBGE."""
        map = {
            'UF': 'cod_uf',
            'Nome_UF': 'nome_uf',
            'Região Geográfica Intermediária': 'regiao_intermediaria',
            'Nome Região Geográfica Intermediária': 'nome_regiao_intermediaria',
            'Região Geográfica Imediata': 'regiao_imediata',
            'Nome Região Geográfica Imediata': 'nome_regiao_imediata',
            'Mesorregião Geográfica': 'mesorregiao_geografica',
            'Nome_Mesorregião': 'nome_mesorregiao',
            'Microrregião Geográfica': 'microrregiao_geografica',
            'Nome_Microrregião': 'nome_microrregiao',
            'Município': 'municipio',
            'Código Município Completo': 'cod_mundv',
            'Nome_Município': 'nome_municipio'
        }
        ufs = {
            'Acre': 'AC', 'Alagoas': 'AL', 'Amapá': 'AP', 'Amazonas': 'AM', 'Bahia': 'BA', 'Ceará': 'CE',
            'Distrito Federal': 'DF', 'Espírito Santo': 'ES', 'Goiás': 'GO', 'Maranhão': 'MA', 'Mato Grosso': 'MT',
            'Mato Grosso do Sul': 'MS', 'Minas Gerais': 'MG', 'Pará': 'PA', 'Paraíba': 'PB', 'Paraná': 'PR',
            'Pernambuco': 'PE', 'Piauí': 'PI', 'Rio de Janeiro': 'RJ', 'Rio Grande do Norte': 'RN', 'Rio Grande do Sul': 'RS',
            'Rondônia': 'RO', 'Roraima': 'RR', 'Santa Catarina': 'SC', 'São Paulo': 'SP', 'Sergipe': 'SE', 'Tocantins': 'TO'
        }

        regioes = {
            1: 'Norte', 2: 'Nordeste', 3: 'Sudeste', 4: 'Sul', 5: 'Centro-Oeste'
        }

        # Renomeando colunas e aplicando transformações
        data = (
            data.rename(columns=map)
            .assign(
                sg_uf=lambda df: df['nome_uf'].map(ufs),
                cod_regiao=lambda df: df['cod_uf'].astype(str).str[1:].astype(int),
                nome_regiao=lambda df: df['cod_regiao'].map(regioes),
                nome_municipio=lambda df: df['nome_municipio'].str.upper().map(unidecode)
            )
            [['cod_mundv', 'nome_municipio', 'cod_uf', 'sg_uf', 'cod_regiao', 'nome_regiao']]
        )

        data['nome_regiao'] = data['nome_regiao'].str.upper()

        return data

    def transform_instituicao(self):
        """Cria a tabela de instituições a partir dos dados do Prouni."""
        instituicao = self.prouni_data[['CODIGO_EMEC_IES_BOLSA', 'NOME_IES_BOLSA']].drop_duplicates()
        return instituicao

    def transform_campus(self):
        """Cria a tabela de campus mapeando as cidades e regiões do IBGE."""
        map_campus = self.prouni_data['CAMPUS'].unique()
        dict_campus = {key: value + 1 for value, key in enumerate(map_campus)}
        self.prouni_data['cod_campus'] = self.prouni_data['CAMPUS'].map(dict_campus)
 
        campus = self.prouni_data.merge(self.table_ibge,
                                        left_on=['MUNICIPIO', 'REGIAO_BENEFICIARIO'],
                                        right_on=['nome_municipio', 'nome_regiao']
                                   )[['cod_campus', 'cod_mundv', 'CAMPUS', 'CODIGO_EMEC_IES_BOLSA']]
        
        return campus

    def transform_turno(self):
        """Cria a tabela de turnos a partir dos dados do Prouni."""
        map_turno = self.prouni_data['NOME_TURNO_CURSO_BOLSA'].unique()
        dict_turno = {key: value + 1 for value, key in enumerate(map_turno)}
        self.prouni_data['cod_turno'] = self.prouni_data['NOME_TURNO_CURSO_BOLSA'].map(dict_turno)
        
        turno = self.prouni_data[['cod_turno', 'NOME_TURNO_CURSO_BOLSA']].drop_duplicates()
        return turno

    def transform_curso(self):
        """Cria a tabela de cursos a partir dos dados do Prouni."""
        map_curso = self.prouni_data['NOME_CURSO_BOLSA'].unique()
        dict_curso = {key: value + 1 for value, key in enumerate(map_curso)}
        self.prouni_data['cod_curso'] = self.prouni_data['NOME_CURSO_BOLSA'].map(dict_curso)
        
        curso = self.prouni_data[['cod_curso', 'NOME_CURSO_BOLSA']].drop_duplicates()
        return curso

    def transform_modalidade(self):
        """Cria a tabela de modalidades de ensino a partir dos dados do Prouni."""
        map_modalidade = self.prouni_data['MODALIDADE_ENSINO_BOLSA'].unique()
        dict_modalidade = {key: value + 1 for value, key in enumerate(map_modalidade)}
        self.prouni_data['cod_modalidade'] = self.prouni_data['MODALIDADE_ENSINO_BOLSA'].map(dict_modalidade)
        
        modalidade = self.prouni_data[['cod_modalidade', 'MODALIDADE_ENSINO_BOLSA']].drop_duplicates()
        return modalidade

    def transform_tipo_bolsa(self):
        """Cria a tabela de tipos de bolsas a partir dos dados do Prouni."""
        map_bolsa = self.prouni_data['TIPO_BOLSA'].unique()
        dict_bolsa = {key: value + 1 for value, key in enumerate(map_bolsa)}
        self.prouni_data['cod_tipo_bolsa'] = self.prouni_data['TIPO_BOLSA'].map(dict_bolsa)
        
        bolsa = self.prouni_data[['cod_tipo_bolsa', 'TIPO_BOLSA']].drop_duplicates()
        return bolsa

    def transform_beneficiario(self):
        """Cria a tabela de beneficiários e une com os dados do IBGE."""
        self.prouni_data['cod_beneficiario'] = range(1, len(self.prouni_data) + 1)
        
        beneficiario = self.prouni_data.merge(self.table_ibge,
                                              left_on=['MUNICIPIO', 'UF_BENEFICIARIO'],
                                              right_on=['nome_municipio', 'sg_uf']
                                              )[['cod_beneficiario', 'cod_mundv', 'cod_tipo_bolsa', 'cod_curso',
                                                 'cod_modalidade', 'cod_campus', 'CPF_BENEFICIARIO', 'SEXO_BENEFICIARIO',
                                                 'RACA_BENEFICIARIO', 'DATA_NASCIMENTO', 'BENEFICIARIO_DEFICIENTE_FISICO']]
        return beneficiario

    def transform_uf(self):
        """Cria a tabela de UF (Unidades Federativas) a partir dos dados do IBGE."""
        uf = self.table_ibge[['cod_uf', 'sg_uf']].drop_duplicates()
        return uf

    def transform_regiao(self):
        """Cria a tabela de regiões a partir dos dados do IBGE."""
        regiao = self.table_ibge[['cod_regiao', 'nome_regiao']].drop_duplicates()
        return regiao

    def transform_municipio(self):
        """Cria a tabela de municípios a partir dos dados do IBGE."""
        municipio = self.table_ibge[['cod_mundv', 'nome_municipio']].drop_duplicates()
        return municipio

    def transform(self):
        """Método principal para transformar todos os dados."""
        tables = {
            "regiao": self.transform_regiao(),
            "uf": self.transform_uf(),
            "municipio": self.transform_municipio(),
            "instituicao": self.transform_instituicao(),
            "campus": self.transform_campus(),
            "turno": self.transform_turno(),
            "curso": self.transform_curso(),
            "modalidade": self.transform_modalidade(),
            "tipo_bolsa": self.transform_tipo_bolsa(),
            "beneficiario": self.transform_beneficiario(),
        }

        return tables
