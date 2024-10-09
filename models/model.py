from sqlalchemy import Column, Integer, String, Date, ForeignKey
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class Model:
    class Instituicao(Base):
        __tablename__ = 'instituicao'

        cod_emec = Column(Integer, primary_key=True, autoincrement=True)
        nome_ies = Column(String, nullable=False)

        campus = relationship("Model.Campus", back_populates="instituicao")

    class Campus(Base):
        __tablename__ = 'campus'

        cod_campus = Column(Integer, primary_key=True, autoincrement=True)
        cod_mundv = Column(Integer, ForeignKey('municipio.cod_mundv'))
        cod_emec = Column(Integer, ForeignKey('instituicao.cod_emec'))
        campus_nome = Column(String, nullable=False)

        instituicao = relationship("Model.Instituicao", back_populates="campus")
        municipio = relationship("Model.Municipio", back_populates="campus")
        beneficiario = relationship("Model.Beneficiario", back_populates="campus")

    class Turno(Base):
        __tablename__ = 'turno'

        cod_turno = Column(Integer, primary_key=True, autoincrement=True)
        nome_turno = Column(String, nullable=False)

        beneficiario = relationship("Model.Beneficiario", back_populates="turno")

    class Curso(Base):
        __tablename__ = 'curso'

        cod_curso = Column(Integer, primary_key=True, autoincrement=True)
        nome_curso = Column(String, nullable=False)

        beneficiario = relationship("Model.Beneficiario", back_populates="curso")

    class Modalidade(Base):
        __tablename__ = 'modalidade'

        cod_modalidade = Column(Integer, primary_key=True, autoincrement=True)
        nome_modalidade = Column(String, nullable=False)

        beneficiario = relationship("Model.Beneficiario", back_populates="modalidade")

    class Bolsa(Base):
        __tablename__ = 'bolsa'

        cod_tipo_bolsa = Column(Integer, primary_key=True, autoincrement=True)
        nome_tipo_bolsa = Column(String, nullable=False)

        beneficiario = relationship("Model.Beneficiario", back_populates="bolsa")

    class Beneficiario(Base):
        __tablename__ = 'beneficiario'

        cod_beneficiario = Column(Integer, primary_key=True, autoincrement=True)
        cod_mundv = Column(Integer, ForeignKey('municipio.cod_mundv'))
        cod_tipo_bolsa = Column(Integer, ForeignKey('bolsa.cod_tipo_bolsa'))
        cod_curso = Column(Integer, ForeignKey('curso.cod_curso'))
        cod_modalidade = Column(Integer, ForeignKey('modalidade.cod_modalidade'))
        cod_campus = Column(Integer, ForeignKey('campus.cod_campus'))
        cod_turno = Column(Integer, ForeignKey('turno.cod_turno'))

        cpf = Column(String, nullable=False)
        sexo = Column(String)
        raca = Column(String)
        data_nascimento = Column(Date)
        deficiente_fisico = Column(String)

        bolsa = relationship("Model.Bolsa", back_populates="beneficiario")
        curso = relationship("Model.Curso", back_populates="beneficiario")
        campus = relationship("Model.Campus", back_populates="beneficiario")
        modalidade = relationship("Model.Modalidade", back_populates="beneficiario")
        turno = relationship("Model.Turno", back_populates="beneficiario")

    class UF(Base):
        __tablename__ = 'uf'

        cod_uf = Column(Integer, primary_key=True, autoincrement=True)
        sigla = Column(String, nullable=False)
        cod_regiao = Column(Integer, ForeignKey('regiao.cod_regiao'))

        regiao = relationship("Model.Regiao", back_populates="ufs")
        municipios = relationship("Model.Municipio", back_populates="uf")

    class Regiao(Base):
        __tablename__ = 'regiao'

        cod_regiao = Column(Integer, primary_key=True, autoincrement=True)
        nome_regiao = Column(String, nullable=False)

        ufs = relationship("Model.UF", back_populates="regiao")

    class Municipio(Base):
        __tablename__ = 'municipio'

        cod_mundv = Column(Integer, primary_key=True, autoincrement=True)
        nome_municipio = Column(String, nullable=False)
        cod_uf = Column(Integer, ForeignKey('uf.cod_uf'))

        uf = relationship("Model.UF", back_populates="municipios")
        campus = relationship("Model.Campus", back_populates="municipio")