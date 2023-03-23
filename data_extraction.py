import yaml

class DataExtractor:
    def __init__(self) -> None:
        pass

    def read_db_creds(self, cred_file):
        with open(cred_file, 'r') as f:
            credentials = yaml.safe_load(f)
            return credentials
    
extractor = DataExtractor()
file = 'db_creds.yaml'
dic = extractor.read_db_creds(file)
print(dic)
