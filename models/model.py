from sqlalchemy import Column, Integer, String, Date, ForeignKey, DateTime
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base
from config.config import Config

Base = declarative_base()


class Model:
    def __init__(self) -> None:
        self.config = Config()
        self.engine = self.config.engine_conn_mysql
    class DownloadRegistry(Base):
        __tablename__ = 'download_registry'
        id = Column(Integer, primary_key=True)
        file_name = Column(String(200))
        download_date = Column(DateTime)
        status = Column(String(200))

    class Regiao(Base):
        __tablename__ = 'regiao'
        cod_regiao = Column(Integer, primary_key=True, autoincrement=True)
        nome_regiao = Column(String(200), nullable=False)
        uf = relationship("Uf", back_populates="regiao")

    class Uf(Base):
        __tablename__ = 'uf'
        cod_uf = Column(Integer, primary_key=True, autoincrement=True)
        sg_uf = Column(String(200), nullable=False)

        #Chave estrangeira
        cod_regiao = Column(Integer, ForeignKey('regiao.cod_regiao'))

        # Relacionamentos
        regiao = relationship("Regiao", back_populates="uf")
        municipios = relationship("Municipio", back_populates="uf")

    class Municipio(Base):
        __tablename__ = 'municipio'
        cod_mundv = Column(Integer, primary_key=True, autoincrement=True)
        nome_municipio = Column(String(200), nullable=False)
        #Chave estrangeira
        cod_uf = Column(Integer, ForeignKey('uf.cod_uf'))

        # Relacionamentos
        uf = relationship("Uf", back_populates="municipios")
        campus = relationship("Campus", back_populates="municipio")
    class Instituicao(Base):
        __tablename__ = 'instituicao'
        cod_ies = Column(Integer, primary_key=True, autoincrement=True)
        cod_emec = Column(Integer, nullable=False)
        nome_ies = Column(String(200), nullable=False)

        # Relacionamentos
        campus = relationship("Campus", back_populates="instituicao")
        curso = relationship("Curso", back_populates="instituicao")   
    class Campus(Base):
        __tablename__ = 'campus'
        cod_campus = Column(Integer, primary_key=True, autoincrement=True)
        cod_mundv_campus = Column(Integer, ForeignKey('municipio.cod_mundv'))

        # Chave estrangeira
        cod_ies = Column(Integer, ForeignKey('instituicao.cod_ies'))

        campus= Column(String(200), nullable=False)

        # Relacionamentos
        instituicao = relationship("Instituicao", back_populates="campus")
        municipio = relationship("Municipio", back_populates="campus")
        aluno = relationship("Aluno", back_populates="campus")
    class Turno(Base):
        __tablename__ = 'turno'
        cod_turno = Column(Integer, primary_key=True, autoincrement=True)
        turno = Column(String(200), nullable=False)

        # Relacionamento
        curso = relationship("Curso", back_populates="turno")

    class Modalidade(Base):
        __tablename__ = 'modalidade'
        cod_modalidade = Column(Integer, primary_key=True, autoincrement=True)
        modalidade = Column(String(200), nullable=False)

        curso = relationship("Curso", back_populates="modalidade")

    class Curso(Base):
        __tablename__ = 'curso'
        cod_curso = Column(Integer, primary_key=True, autoincrement=True)
        nome_curso = Column(String(200), nullable=False)

        #Chaves Estrangeiras
        cod_ies = Column(Integer, ForeignKey('instituicao.cod_ies'))
        cod_modalidade = Column(Integer, ForeignKey('modalidade.cod_modalidade'))
        cod_turno = Column(Integer, ForeignKey('turno.cod_turno'))

        # Relacionamentos
        instituicao = relationship("Instituicao", back_populates="curso")
        modalidade = relationship("Modalidade", back_populates="curso")
        turno = relationship("Turno", back_populates="curso")
        aluno = relationship("Aluno", back_populates="curso")
    class Bolsa(Base):
        __tablename__ = 'bolsa'
        cod_tipo_bolsa = Column(Integer, primary_key=True, autoincrement=True)
        tipo_bolsa = Column(String(200), nullable=False)

        # Relacionamento
        aluno = relationship("Aluno", back_populates="bolsa")
    class Aluno(Base):
        __tablename__ = 'aluno'
        cod_aluno = Column(Integer, primary_key=True, autoincrement=True)
        ano_processo_seletivo = Column(Integer)
        cod_mundv_aluno = Column(Integer, ForeignKey('municipio.cod_mundv'))
        cod_tipo_bolsa = Column(Integer, ForeignKey('bolsa.cod_tipo_bolsa'))
        cod_curso = Column(Integer, ForeignKey('curso.cod_curso'))
        cod_campus = Column(Integer, ForeignKey('campus.cod_campus'))
        
        cpf = Column(String(200), nullable=False)
        sexo = Column(String(200))
        raca = Column(String(200))
        data_nascimento = Column(Date)
        deficiente_fisico = Column(String(200))


        bolsa = relationship("Bolsa", back_populates="aluno")
        curso = relationship("Curso", back_populates="aluno")
        campus = relationship("Campus", back_populates="aluno")
        bolsa = relationship("Bolsa", back_populates="aluno")

    def execute(self, drop=False):
            engine = self.config.engine_conn_mysql
            if drop:
                Base.metadata.drop_all(engine)
            Base.metadata.create_all(engine)
