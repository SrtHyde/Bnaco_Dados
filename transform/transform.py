from pandas import DataFrame, read_csv, Series, read_excel
from unidecode import unidecode
from glob import glob
from re import sub
from config.config import Config

class Transform:
    def __init__(self):
        """Inicializa a classe com os diretórios de dados do IBGE e Prouni."""
        self.config = Config()
        self.data_dir_ibge = self.config.vars.data_dir_ibge
        self.data_dir_prouni = self.config.vars.data_dir_prouni
        self.prouni_data = None  # Inicializa a variável que armazenará os dados do Prouni
        self.table_ibge = None   # Inicializa a variável que armazenará os dados do IBGE

    def check_files(self):

        files_prouni  = glob(f"{self.data_dir_prouni}*/*.csv")
        files_ibge = glob(f"{self.data_dir_ibge}*/*.xls")
        
        if len(files_ibge) == 0  :
                raise FileNotFoundError(f"Not found files to IBGE")
        if len(files_prouni) == 0  :
                raise FileNotFoundError(f"Not found files to PROUNI")
        
        file_ibge = files_ibge[0]

        return files_prouni, file_ibge
    
    def __clean_prouni(self, data):
        """Limpa os dados do Prouni"""
        data.columns = [sub(r'[^\w_]', '', coluna).replace('ï', '') for coluna in data.columns]
        data = data.sort_index(axis=1)
        data = data.dropna(subset=['ANO_CONCESSAO_BOLSA'])

        if 'CAMPUS' not in data.columns:
            data['campus'] = Series(['Sem informação'] * len(data))
            data['municipio_ies'] = Series(['Sem informação'] * len(data))

        map_columns = {
            'ANO_CONCESSAO_BOLSA': 'ANO',
            'DT_NASCIMENTO_BENEFICIARIO': 'DATA_NASCIMENTO',
            'SIGLA_UF_BENEFICIARIO_BOLSA': 'UF',
            'CPF_BENEFICIARIO_BOLSA': 'CPF',
            'RACA_BENEFICIARIO_BOLSA': 'RACA',
            'REGIAO_BENEFICIARIO_BOLSA': 'REGIAO',
            'SEXO_BENEFICIARIO_BOLSA': 'SEXO',
            'MUNICIPIO_BENEFICIARIO_BOLSA': 'MUNICIPIO_BENEFICIARIO',
            'MODALIDADE_ENSINO_BOLSA': 'MODALIDADE_ENSINO',
            'NOME_TURNO_CURSO_BOLSA': 'TURNO',
            'NOME_CURSO_BOLSA': 'CURSO',
            'BENEFICIARIO_DEFICIENTE_FISICO': 'DEFICIENTE_FISICO',
            'CPF_BENEFICIARIO': 'CPF',
            'UF_BENEFICIARIO': 'UF_BENEFICIARIO',
            'RACA_BENEFICIARIO': 'RACA',
            'REGIAO_BENEFICIARIO': 'REGIAO',
            'SEXO_BENEFICIARIO': 'SEXO',
            'MUNICIPIO': 'MUNICIPIO_IES',
            'CODIGO_EMEC_IES_BOLSA': 'COD_EMEC',
            'NOME_IES_BOLSA': 'NOME_IES'
        }

        map_regions = {
            "Norte": 1,
            "Nordeste": 2,
            "Sudeste": 3,
            "Sul": 4,
            "Centro-Oeste": 5
        }

        data = data.rename(columns=map_columns)
        data.columns = [sub(r'[^\w_]', '', coluna).replace(' ', '_').lower() for coluna in data.columns]

        data = data.apply(lambda x: x.str.title() if x.dtype == 'object' else x).assign(
            ano=lambda x: x.ano.astype(int),
            tipo_bolsa=lambda x: x.tipo_bolsa.replace({'Bolsa Parcial 50%': 'Parcial', 'Bolsa Integral': 'Integral', 'Bolsa Parcial 25%': 'Parcial'}),
            modalidade_ensino=lambda x: x.modalidade_ensino.replace({'Educação A Distância': 'EAD'}),
            sexo=lambda x: x.sexo.replace({'Feminino': 'F', 'Masculino': 'M'}),
            deficiente_fisico=lambda x: x.deficiente_fisico.replace({'S': 'Sim', 'N': 'Não', 'M': 'Não'}),
            raca=lambda x: x.raca.replace({'Ind¡gena': 'Indígena', 'Ind¡Gena': 'Indígena'}),
            cod_regiao=lambda x: x.regiao.replace(map_regions),
        )
        return data

    def __clean_ibge(self, data):
        """Limpa os dados do IBGE."""
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
        instituicao = self.prouni_data[['cod_emec', 'nome_ies']].drop_duplicates()
        return instituicao

    def transform_campus(self):
        """Cria a tabela de campus mapeando as cidades e regiões do IBGE."""
        map_campus = self.prouni_data['campus'].unique()
        dict_campus = {key: value + 1 for value, key in enumerate(map_campus)}
        self.prouni_data['cod_campus'] = self.prouni_data['campus'].map(dict_campus)
 
        campus = self.prouni_data.merge(self.table_ibge,
                                        left_on=['municipio_ies', 'cod_regiao'],
                                        right_on=['nome_municipio', 'cod_regiao']
                                   )[['cod_campus', 'cod_mundv', 'campus', 'cod_emec']]
        
        return campus

    def transform_turno(self):
        """Cria a tabela de turnos a partir dos dados do Prouni."""
        map_turno = self.prouni_data['turno'].unique()
        dict_turno = {key: value + 1 for value, key in enumerate(map_turno)}
        self.prouni_data['cod_turno'] = self.prouni_data['turno'].map(dict_turno)
        
        turno = self.prouni_data[['cod_turno', 'turno']].drop_duplicates()
        return turno

    def transform_curso(self):
        """Cria a tabela de cursos a partir dos dados do Prouni."""
        map_curso = self.prouni_data['curso'].unique()
        dict_curso = {key: value + 1 for value, key in enumerate(map_curso)}
        self.prouni_data['cod_curso'] = self.prouni_data['curso'].map(dict_curso)
        
        curso = self.prouni_data[['cod_curso', 'curso']].drop_duplicates()
        return curso

    def transform_modalidade(self):
        """Cria a tabela de modalidades de ensino a partir dos dados do Prouni."""
        map_modalidade = self.prouni_data['modalidade_ensino'].unique()
        dict_modalidade = {key: value + 1 for value, key in enumerate(map_modalidade)}
        self.prouni_data['cod_modalidade'] = self.prouni_data['modalidade_ensino'].map(dict_modalidade)
        
        modalidade = self.prouni_data[['cod_modalidade', 'modalidade_ensino']].drop_duplicates()
        return modalidade

    def transform_tipo_bolsa(self):
        """Cria a tabela de tipos de bolsas a partir dos dados do Prouni."""
        map_bolsa = self.prouni_data['tipo_bolsa'].unique()
        dict_bolsa = {key: value + 1 for value, key in enumerate(map_bolsa)}
        self.prouni_data['cod_tipo_bolsa'] = self.prouni_data['tipo_bolsa'].map(dict_bolsa)
        
        bolsa = self.prouni_data[['cod_tipo_bolsa', 'tipo_bolsa']].drop_duplicates()
        return bolsa

    def transform_beneficiario(self):
        """Cria a tabela de beneficiários e une com os dados do IBGE."""
        self.prouni_data['cod_beneficiario'] = range(1, len(self.prouni_data) + 1)
        
        beneficiario = self.prouni_data.merge(self.table_ibge,
                                              left_on=['municipio_beneficiario', 'uf_beneficiario'],
                                              right_on=['nome_municipio', 'sg_uf']
                                              )[['cod_beneficiario', 'cod_mundv', 'cod_tipo_bolsa', 'cod_curso',
                                                 'cod_modalidade', 'cod_turno','cod_campus', 'cpf', 'sexo',
                                                 'raca', 'data_nascimento', 'deficiente_fisico']]
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
        prouni_files, ibge_file = self.check_files()
        try:
            print('Lendo Arquivo Ibge ...')
            data_ibge = read_excel(f'{ibge_file}', skiprows=6, engine='xlrd' )

            if len(data_ibge) == 0:
                raise FileNotFoundError(f"Not found data to {ibge_file}")
            
            self.table_ibge = self.__clean_ibge(data_ibge)

        except Exception as error:
            raise OSError(error) from error
        
        qtd = len(prouni_files)
        for cont, file in enumerate(prouni_files):
            try:

                print(f'Processando Prouni {file} - ({cont}/{qtd})...')
                data_prouni = read_csv(f'{file}', sep=';', encoding='latin1' )

                if len(data_prouni) == 0:
                    raise FileNotFoundError(f"Not found data to {file}")

                self.prouni_data = self.__clean_prouni(data_prouni)

            except Exception as error:
                raise OSError(error) from error
        
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
        print(tables)
        exit()
        return tables


