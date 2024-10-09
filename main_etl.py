from config.config import Config
from extract.extract_ibge import IbgeExtractor
from extract.extract_prouni import ProuniExtractor
from transform.transform import Transform

extract_prouni = ProuniExtractor()
extract_prouni.download()

extract_ibge = IbgeExtractor()
extract_ibge.download()

transform = Transform()

table = transform.transform()
print(table)
