from os.path import join, dirname, abspath
from yaml import load
from yaml.loader import SafeLoader

class Config:
    
    def __init__(self) -> None:
        """Load instance variables"""
        data = {}
        with open(
            join(dirname(abspath(__file__)), "env.yaml"), encoding="utf-8"
        ) as file:
            data = load(file, Loader=SafeLoader)
        file.close()
        for key, value in data.items():
            self.vars.update({key: value})