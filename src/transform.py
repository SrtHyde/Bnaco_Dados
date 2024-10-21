from pandas import DataFrame, read_csv, read_excel, concat, to_datetime
from unidecode import unidecode
from glob import glob
from re import sub
from typing import List, Dict, Tuple
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.config import Config

class Transform:
    def __init__(self):
        self.config = Config()
        self.dict_source = self.config.vars.source
        self.project_root = os.path.abspath(os.path.join(os.getcwd()))
        self.prouni_data = None
        self.ibge_data = None
        self.merged_data = None

    def check_files(self) -> Tuple[List[str], str]:
        for source in self.dict_source.keys():
            dict_source = self.dict_source[source]
            data_dir = f"{self.project_root}{dict_source['data_dir']}"
            type_file = dict_source['type']

            files_dir = glob(os.path.join(data_dir, type_file))

            if source == 'ibge':
                files_ibge = files_dir
            if source == 'prouni':
                files_prouni = files_dir
     

        if  len(files_ibge) == 0:
            raise FileNotFoundError("Arquivos não encontrados para IBGE")
        if len(files_prouni) == 0:
            raise FileNotFoundError("Arquivos não encontrados para PROUNI")
        
        return files_prouni, files_ibge[1]
    
    def clean_prouni(self, data: DataFrame) -> DataFrame:
        data.columns = [sub(r'[^\w_]', '', coluna).replace('ï', '') for coluna in data.columns]

        data = data.sort_index(axis=1).dropna(subset=['ANO_CONCESSAO_BOLSA'])

        ufs = {
            'AC': 12, 'AL': 27, 'AP': 16, 'AM': 13, 'BA': 29,
            'CE': 23, 'DF': 53, 'ES': 32, 'GO': 52,
            'MA': 21, 'MT': 51, 'MS': 50, 'MG': 31,
            'PA': 15, 'PB': 25, 'PR': 41, 'PE': 26, 'PI': 22,
            'RJ': 33, 'RN': 24, 'RS': 43,
            'RO': 11, 'RR': 14, 'SC': 42, 'SP': 35,
            'SE': 28, 'TO': 17
            }

        if 'CAMPUS' not in data.columns:
            data['campus'] = 'Sem informação'
            data['municipio_ies'] = 'Sem informação'

        map_columns = {
            'ANO_CONCESSAO_BOLSA': 'ANO', 'DT_NASCIMENTO_BENEFICIARIO': 'DATA_NASCIMENTO',
            'SIGLA_UF_BENEFICIARIO_BOLSA': 'SG_UF', 'UF_BENEFICIARIO': 'SG_UF' ,'CPF_BENEFICIARIO_BOLSA': 'CPF', 'RACA_BENEFICIARIO_BOLSA': 'RACA',
            'REGIAO_BENEFICIARIO_BOLSA': 'REGIAO', 'SEXO_BENEFICIARIO_BOLSA': 'SEXO',  "MUNICIPIO_BENEFICIARIO": "MUNICIPIO_ALUNO",
            'MUNICIPIO_BENEFICIARIO_BOLSA': 'MUNICIPIO_ALUNO', 'MODALIDADE_ENSINO_BOLSA': 'MODALIDADE',
            'MODALIDADE_ENSINO': 'MODALIDADE', 'NOME_TURNO_CURSO_BOLSA': 'TURNO', 'NOME_CURSO_BOLSA': 'NOME_CURSO',
            'NOME_TURNO_CURSO_BOLSA': 'TURNO', 
            'BENEFICIARIO_DEFICIENTE_FISICO': 'DEFICIENTE_FISICO', 'CPF_BENEFICIARIO': 'CPF',
            'RACA_BENEFICIARIO': 'RACA', 'REGIAO_BENEFICIARIO': 'NOME_REGIAO',
            'SEXO_BENEFICIARIO': 'SEXO', 'MUNICIPIO': 'MUNICIPIO_IES',
            'CODIGO_EMEC_IES_BOLSA': 'COD_EMEC', 'NOME_IES_BOLSA': 'NOME_IES',
            'CAMPUS': 'CAMPUS',
        }

        map_regions = {
            "Norte": 1, "Nordeste": 2, "Sudeste": 3, "Sul": 4, "Centro-Oeste": 5
        }

        data = data.rename(columns=map_columns)
        data.columns = [sub(r'[^\w_]', '', coluna).replace(' ', '_').lower() for coluna in data.columns]
        
        data = data.apply(lambda x: x.str.title() if x.dtype == 'object' else x).assign(
            ano=lambda x: x.ano.astype(int),
            tipo_bolsa=lambda x: x.tipo_bolsa.replace({'Bolsa Parcial 50%': 'Parcial', 'Bolsa Integral': 'Integral', 'Bolsa Parcial 25%': 'Parcial'}),
            modalidade=lambda x: x.modalidade.replace({'Educação A Distância': 'EAD'}),
            sexo=lambda x: x.sexo.replace({'Feminino': 'F', 'Masculino': 'M'}),
            deficiente_fisico=lambda x: x.deficiente_fisico.replace({'S': 'Sim', 'N': 'Não', 'M': 'Não'}),
            raca=lambda x: x.raca.replace({'Ind¡gena': 'Indígena', 'Ind¡Gena': 'Indígena'}),
            cod_regiao=lambda x: x.nome_regiao.map(map_regions),
            municipio_ies=lambda x: x.municipio_ies.str.upper().map(unidecode),
            municipio_aluno=lambda x: x.municipio_aluno.str.upper().map(unidecode),
            sg_uf = lambda x: x.sg_uf.str.upper().map(unidecode),
            cod_uf_aluno=lambda x: x.sg_uf.map(ufs),
            
        )
        data['data_nascimento'] = to_datetime(data['data_nascimento'], format='%d/%m/%Y', errors='coerce'
                                              ).dt.strftime('%Y-%m-%d')
        data['municipio_aluno'] = data['municipio_aluno'].str.replace(r'D[\s\'\"`]+', "D'", regex=True
                                                                    ).replace({"BELFORD'ROXO": "BELFORD ROXO",
                                                                                "SUD'MENNUCCI": "SUD MENNUCCI",
                                                                                 "DAVID'CANABARRO": "DAVID CANABARRO",
                                                                                 "MOGI-GUACU": "MOGI GUACU",
                                                                                 "PARATI":'PARATY',
                                                                                'EMBU': 'EMBU DAS ARTES',
                                                                                'DONA EUSEBIA': 'DONA EUZEBIA',
                                                                                'SANTANA DO LIVRAMENTO': "SANT'ANA DO LIVRAMENTO",
                                                                                'ITAMARACA': 'ILHA DE ITAMARACA',
                                                                                'LAGOA DO ITAENGA': 'LAGOA DE ITAENGA',
                                                                                'SANTA ISABEL DO PARA': 'SANTA IZABEL DO PARA',
                                                                                'TRAJANO DE MORAIS': 'TRAJANO DE MORAES',
                                                                                'SANTO ANTONIO DO LEVERGER': 'SANTO ANTONIO DE LEVERGER',
                                                                                'ITAPAGE': 'ITAPAJE',
                                                                                'PICARRAS': 'BALNEARIO PICARRAS',
                                                                                'IGUARACI': 'IGUARACY',
                                                                                'BELEM DE SAO FRANCISCO': 'BELEM DO SAO FRANCISCO',
                                                                                'TOCOS DO MOGI': 'TOCOS DO MOJI',
                                                                                'SAO VALERIO DA NATIVIDADE': 'SAO VALERIO',
                                                                                'SAO MIGUEL DE TOUROS': 'SAO MIGUEL DO GOSTOSO',
                                                                                'SAO THOME DAS LETRAS': 'SAO TOME DAS LETRAS',
                                                                                'ITABIRINHA DE MANTENA': 'ITABIRINHA',
                                                                                'BRASOPOLIS': 'BRAZOPOLIS',
                                                                                'GOVERNADOR LOMANTO JUNIOR': 'NOVA IBIA',
                                                                                'FORTALEZA DO TABOCAO': 'TABOCAO',
                                                                                'ELDORADO DOS CARAJAS': 'ELDORADO DO CARAJAS',
                                                                                'AUGUSTO SEVERO': 'CAMPO GRANDE',
                                                                                'COUTO DE MAGALHAES': 'COUTO MAGALHAES',
                                                                                'SAO BENTO DE POMBAL': 'SAO BENTO',
                                                                                'CAMPO DE SANTANA': 'TACIMA',
                                                                                'CUVERLANDIA': 'CURVELANDIA',
                                                                                'POXOREO': 'POXOREU',
                                                                                'CHIAPETA': 'CHIAPETTA',
                                                                                'IPAUCU': 'IPAUSSU',
                                                                                'SAO LUIS DO PARAITINGA': 'SAO LUIZ DO PARAITINGA',
                                                                                'MUQUEM DE SAO FRANCISCO': 'MUQUEM DO SAO FRANCISCO',
                                                                                'BATAIPORA': 'BATAYPORA',
                                                                                'SAO DOMINGOS DE POMBAL': 'SAO DOMINGOS',
                                                                                'AMPARO DE SAO FRANCISCO': 'AMPARO DO SAO FRANCISCO',
                                                                                'BELA VISTA DO CAROBA': 'BELA VISTA DA CAROBA',
                                                                                'SANTA TERESINHA': 'SANTA TEREZINHA'})
        mapping = {
            ('SANTAREM', 25): 'JOCA CLAUDINO',
            ('PRESIDENTE JUSCELINO', 24): 'SERRA CAIADA',
            ('PRESIDENTE CASTELO BRANCO', 42): 'PRESIDENTE CASTELLO BRANCO',
            ('SANTA TEREZINHA', 25): 'SANTA TERESINHA'
            }

        data['municipio_aluno'] = data.apply(
            lambda row: mapping.get((row['municipio_aluno'], row['cod_uf_aluno']), row['municipio_aluno']),
            axis=1
        )

        data['municipio_ies'] = data['municipio_ies'].str.replace('-', ' ')
        data['municipio_ies'] = data['municipio_ies'].str.replace(r'D[\s\'\"`]+', "D'", regex=True
                                                                    ).replace({"BELFORD'ROXO": "BELFORD ROXO",
                                                                                "SUD'MENNUCCI": "SUD MENNUCCI",
                                                                                 "DAVID'CANABARRO": "DAVID CANABARRO",
                                                                                 "MOGI-GUACU": "MOGI GUACU",
                                                                                 "PARATI":'PARATY',
                                                                                'EMBU': 'EMBU DAS ARTES',
                                                                                'DONA EUSEBIA': 'DONA EUZEBIA',
                                                                                'SANTANA DO LIVRAMENTO': "SANT'ANA DO LIVRAMENTO",
                                                                                'ITAMARACA': 'ILHA DE ITAMARACA',
                                                                                'LAGOA DO ITAENGA': 'LAGOA DE ITAENGA',
                                                                                'SANTA ISABEL DO PARA': 'SANTA IZABEL DO PARA',
                                                                                'TRAJANO DE MORAIS': 'TRAJANO DE MORAES',
                                                                                'SANTO ANTONIO DO LEVERGER': 'SANTO ANTONIO DE LEVERGER',
                                                                                'ITAPAGE': 'ITAPAJE',
                                                                                'PICARRAS': 'BALNEARIO PICARRAS',
                                                                                'IGUARACI': 'IGUARACY',
                                                                                'BELEM DE SAO FRANCISCO': 'BELEM DO SAO FRANCISCO',
                                                                                'TOCOS DO MOGI': 'TOCOS DO MOJI',
                                                                                'SAO VALERIO DA NATIVIDADE': 'SAO VALERIO',
                                                                                'SAO MIGUEL DE TOUROS': 'SAO MIGUEL DO GOSTOSO',
                                                                                'SAO THOME DAS LETRAS': 'SAO TOME DAS LETRAS',
                                                                                'ITABIRINHA DE MANTENA': 'ITABIRINHA',
                                                                                'BRASOPOLIS': 'BRAZOPOLIS',
                                                                                'GOVERNADOR LOMANTO JUNIOR': 'NOVA IBIA',
                                                                                'FORTALEZA DO TABOCAO': 'TABOCAO',
                                                                                'ELDORADO DOS CARAJAS': 'ELDORADO DO CARAJAS',
                                                                                'AUGUSTO SEVERO': 'CAMPO GRANDE',
                                                                                'COUTO DE MAGALHAES': 'COUTO MAGALHAES',
                                                                                'SAO BENTO DE POMBAL': 'SAO BENTO',
                                                                                'CAMPO DE SANTANA': 'TACIMA',
                                                                                'CUVERLANDIA': 'CURVELANDIA',
                                                                                'POXOREO': 'POXOREU',
                                                                                'CHIAPETA': 'CHIAPETTA',
                                                                                'IPAUCU': 'IPAUSSU',
                                                                                'SAO LUIS DO PARAITINGA': 'SAO LUIZ DO PARAITINGA',
                                                                                'MUQUEM DE SAO FRANCISCO': 'MUQUEM DO SAO FRANCISCO',
                                                                                'BATAIPORA': 'BATAYPORA',
                                                                                'SAO DOMINGOS DE POMBAL': 'SAO DOMINGOS',
                                                                                'AMPARO DE SAO FRANCISCO': 'AMPARO DO SAO FRANCISCO',
                                                                                'BELA VISTA DO CAROBA': 'BELA VISTA DA CAROBA',
                                                                                'SANTA TERESINHA': 'SANTA TEREZINHA'})



        data['municipio_aluno'] = data['municipio_aluno'].str.replace('-', ' ')
        return data

    def clean_ibge(self, data: DataFrame) -> DataFrame:
        map_columns = {
            'UF': 'cod_uf', 'Nome_UF': 'nome_uf',
            'Código Município Completo': 'cod_mundv',
            'Nome_Município': 'nome_municipio',
        }
        ufs = {
            12: 'AC', 27: 'AL', 16: 'AP', 13: 'AM', 29: 'BA',
            23: 'CE', 53: 'DF', 32: 'ES', 52: 'GO',
            21: 'MA', 51: 'MT', 50: 'MS', 31: 'MG',
            15: 'PA', 25: 'PB', 41: 'PR', 26: 'PE', 22: 'PI',
            33: 'RJ', 24: 'RN', 43: 'RS',
            11: 'RO', 14: 'RR', 42: 'SC', 35: 'SP',
            28: 'SE', 17: 'TO'
        }
        regioes = {
            1: 'NORTE', 2: 'NORDESTE', 3: 'SUDESTE', 4: 'SUL', 5: 'CENTRO-OESTE'
        }
        data = data.rename(columns=map_columns).assign(
                sg_uf=lambda df: df['cod_uf'].map(ufs),
                cod_regiao=lambda df: df['cod_uf'].astype(str).str[0].astype(int),
                nome_regiao=lambda df: df['cod_regiao'].map(regioes),
                nome_municipio=lambda df: df['nome_municipio'].str.replace('-', ' ').str.upper().map(unidecode)
            )[['cod_mundv', 'nome_municipio', 'cod_uf', 'sg_uf', 'cod_regiao', 'nome_regiao']
             ].assign().drop_duplicates()


        return data

    def generate_codes(self, data: DataFrame) -> DataFrame:

        def create_code_map(column: str) -> Dict[str, int]:
            unique_values = data[column].unique()
            return {key: value + 1 for value, key in enumerate(unique_values)}

        code_columns = ['turno', 'nome_curso', 'modalidade', 'tipo_bolsa']
        
        for column in code_columns:
            code_map = create_code_map(column)
            new_column = f'cod_{column.replace("_ensino", "").replace("nome_", "")}'
            data[new_column] = data[column].map(code_map)

        data['cod_ies'] = data.groupby(['cod_emec', 'nome_ies']).ngroup() + 1
        data['cod_curso'] = data.groupby(['nome_curso', 'cod_ies', 'cod_turno', 'cod_modalidade']).ngroup() + 1
        data['cod_campus'] = data.groupby(['cod_ies', 'campus', 'cod_mundv_campus']).ngroup() + 1
        data['cod_aluno'] = range(1, len(data) + 1)

        return data

    

    def transform(self) -> Dict[str, DataFrame]:

        prouni_files, ibge_file = self.check_files()
        
        try:
            print(f'\n Processando {ibge_file} ...')
            data_ibge = read_excel(ibge_file, skiprows=6, engine='xlrd')
            print('Processado\n')
            if data_ibge.empty:
                raise FileNotFoundError(f"Dados não encontrados no arquivo: {ibge_file}")
            self.ibge_data = self.clean_ibge(data_ibge)
        except Exception as error:
            raise OSError(f"Erro ao processar arquivo do IBGE: {error}") from error

        all_prouni_data = DataFrame()
        for file in prouni_files:
            try:

                print(f'Processando {file}...')
                data_prouni = read_csv(file, sep=';', encoding='iso-8859-1')
                if data_prouni.empty:
                    print(f"Dados não encontrados no arquivo: {file}")
                    continue
                cleaned_data = self.clean_prouni(data_prouni)
                all_prouni_data = concat( [all_prouni_data, cleaned_data], ignore_index=True)
                
            except Exception as error:
                print(f"Erro ao processar {file}: {error}")
        
        if  len(all_prouni_data) == 0:
            raise ValueError("Não foram encontrados dados válidos para o Prouni")

        self.prouni_data = all_prouni_data

        
       # Realizando os merges com sufixos para evitar conflitos
        self.merged_data = self.prouni_data.merge(
            self.ibge_data,
            left_on=['municipio_ies'],
            right_on=['nome_municipio'],
            how='left',
            suffixes=('_campus', '_ibge')
        ).drop(columns=['sg_uf_ibge', 'nome_regiao_ibge', 'nome_municipio']
        ).rename(columns={'cod_mundv': 'cod_mundv_campus', 
                          'cod_uf': 'cod_uf_campus', 
                          'cod_regiao': 'cod_regiao_campus',
                          })
        
        self.merged_data = self.merged_data.merge(
            self.ibge_data,
            left_on=['municipio_aluno', 'cod_uf_aluno'],
            right_on=['nome_municipio', 'cod_uf'],
            suffixes=('_prouni', '_ibge'),
            how='left'
            ).dropna(subset=['nome_curso']).rename(columns={'cod_mundv': 'cod_mundv_aluno', 
                          'cod_regiao': 'cod_regiao_aluno',
                          'sg_uf': 'sg_uf_aluno',
                          }).drop(columns=['nome_municipio', 'cod_uf'])
        # Agora vamos renomear as colunas duplicadas ou conflitantes para algo mais claro:
        


        self.merged_data = self.generate_codes(self.merged_data)

        regiao=  self.ibge_data[['cod_regiao', 'nome_regiao']].drop_duplicates()
        uf  = self.ibge_data[['cod_uf', 'sg_uf', 'cod_regiao']].drop_duplicates()
        municipio= self.ibge_data[['cod_mundv', 'nome_municipio', 'cod_uf']].drop_duplicates()
        instituicao= self.merged_data[['cod_ies','cod_emec', 'nome_ies']].drop_duplicates()
        campus= self.merged_data[['cod_campus', 'cod_mundv_campus', 'campus', 'cod_ies']].drop_duplicates()
        turno= self.merged_data[['cod_turno', 'turno']].drop_duplicates()
        curso =self.merged_data[['cod_curso', 'nome_curso', 'cod_turno', 'cod_modalidade', 'cod_ies']].drop_duplicates()
        modalidade = self.merged_data[['cod_modalidade', 'modalidade']].drop_duplicates()
        bolsa = self.merged_data[['cod_tipo_bolsa', 'tipo_bolsa']].drop_duplicates()
        aluno = self.merged_data[['cod_aluno', 'cod_mundv_aluno', 'cod_tipo_bolsa', 'cod_curso',
                                                            'cod_campus', 'cpf', 'sexo', 'raca', 'data_nascimento', 
                                                            'deficiente_fisico']].drop_duplicates()
        
        tables = {
            "regiao": regiao,
            "uf": uf,
            "municipio":municipio,
            "instituicao": instituicao,
            "campus": campus,
            "turno": turno,
            "curso": curso,
            "modalidade": modalidade,
            "bolsa": bolsa,
            "aluno": aluno,
        }
        
        return tables

    def execute(self) -> Dict[str, DataFrame]:

        self.tables = self.transform()

        return self.tables