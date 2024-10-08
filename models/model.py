from sqlalchemy import Column, Integer, String, ForeignKey
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class Instituicao(Base):
    __tablename__ = 'instituicao'

    codigo_emec_ies_bolsa = Column(Integer, primary_key=True)
    nome_ies_bolsa = Column(String, nullable=False)
    
    campus = relationship("Model.Campus", back_populates="instituicao")

class Campus(Base):
    __tablename__ = 'campus'

    cod_campus = Column(Integer, primary_key=True)
    cod_mundv = Column(Integer, ForeignKey('municipio.cod_mundv'))
    campus_nome = Column(String, nullable=False)
    codigo_emec_ies_bolsa = Column(Integer, ForeignKey('instituicao.codigo_emec_ies_bolsa'))

    instituicao = relationship("Model.Instituicao", back_populates="campus")
    municipio = relationship("Model.Municipio", back_populates="campus")

class Turno(Base):
    __tablename__ = 'turno'

    cod_turno = Column(Integer, primary_key=True)
    nome_turno = Column(String, nullable=False)

class Curso(Base):
    __tablename__ = 'curso'

    cod_curso = Column(Integer, primary_key=True)
    nome_curso = Column(String, nullable=False)

class Modalidade(Base):
    __tablename__ = 'modalidade'

    cod_modalidade = Column(Integer, primary_key=True)
    nome_modalidade = Column(String, nullable=False)

class Bolsa(Base):
    __tablename__ = 'bolsa'

    cod_tipo_bolsa = Column(Integer, primary_key=True)
    nome_tipo_bolsa = Column(String, nullable=False)

    beneficiario = relationship("Model.Beneficiario", back_populates="bolsa")

class Beneficiario(Base):
    __tablename__ = 'beneficiario'

    cod_beneficiario = Column(Integer, primary_key=True)
    cod_mundv = Column(Integer, ForeignKey('municipio.cod_mundv'))
    cod_tipo_bolsa = Column(Integer, ForeignKey('bolsa.cod_tipo_bolsa'))
    cod_curso = Column(Integer, ForeignKey('curso.cod_curso'))
    cod_modalidade = Column(Integer, ForeignKey('modalidade.cod_modalidade'))
    cod_campus = Column(Integer, ForeignKey('campus.cod_campus'))
    cpf = Column(String, nullable=False)
    sexo = Column(String)
    raca = Column(String)
    data_nascimento = Column(String)
    beneficiario_deficiente_fisico = Column(String)

    bolsa = relationship("Model.Bolsa", back_populates="beneficiario")
    curso = relationship("Model.Curso", back_populates="beneficiario")
    campus = relationship("Model.Campus", back_populates="beneficiario")
    modalidade = relationship("Model.Modalidade", back_populates="beneficiario")

class UF(Base):
    __tablename__ = 'uf'

    cod_uf = Column(Integer, primary_key=True)
    sigla = Column(String, nullable=False)

class Regiao(Base):
    __tablename__ = 'regiao'

    cod_regiao = Column(Integer, primary_key=True)
    nome_regiao = Column(String, nullable=False)

class Municipio(Base):
    __tablename__ = 'municipio'

    cod_mundv = Column(Integer, primary_key=True)
    nome_municipio = Column(String, nullable=False)

    campus = relationship("Model.Campus", back_populates="municipio")