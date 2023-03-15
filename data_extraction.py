import yaml

class DataExtractor:
    def __init__(self) -> None:
        pass

    def read_db_creds(self, cred_file):
        credentials = yaml.safe_load(cred_file)
        return credentials
    
extractor = DataExtractor()
file = open('db_creds.yaml', 'r')
dic = extractor.read_db_creds(file)
print(dic)