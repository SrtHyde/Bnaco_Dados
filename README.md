
# Projeto de ETL para Dados do ProUni
Este projeto foi desenvolvido para extrair, transformar e carregar dados (ETL) relacionados ao programa ProUni, utilizando fontes oficiais do ProUni e dados auxiliares do IBGE. Abaixo está o detalhamento das etapas de Extract, Transform, e Load.

## Etapas do Processo
### 1. Extract (Extração)
A etapa de extração coleta dados diretamente do site oficial do ProUni, complementando as informações com dados do IBGE. Nesta etapa, utilizamos as bibliotecas:

- requests: para realizar requisições HTTP aos sites e acessar as fontes de dados.
- BeautifulSoup: para analisar e extrair o conteúdo HTML necessário.
### 2. Transform (Transformação)
Nesta etapa, os dados extraídos passam por um processo de transformação para limpeza e padronização, tornando-os adequados para carga no banco de dados. As principais bibliotecas utilizadas foram:

- pandas: para manipulação e transformação dos dados em DataFrames, facilitando a organização e análise.
- unidecode: para remover acentuações e caracteres especiais, garantindo consistência na base.
### 3. Load (Carga)
Os dados transformados são carregados em um banco de dados MySQL. A modelagem do banco foi realizada com **SQLAlchemy**, levando em consideração a estrutura dos dados do ProUni de 2020. A modelagem inclui definições de tabelas, tipos de dados e restrições necessárias para garantir a integridade dos dados.

## Configuração do Banco de Dados
O banco de dados foi modelado para armazenar eficientemente os dados do ProUni, proporcionando uma estrutura robusta para consultas e análises futuras.
O modelo criado pode ser vizualizado na imagem a seguir:

![Diagrama Lógico do banco de dados](images\Diagrama_ER_PROUNI.png)