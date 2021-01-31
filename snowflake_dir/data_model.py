from snowflake_dir.snowflake_connect import SnowflakeConnection
import yaml

class DBModel():
    def __init__(self):
        self.conn = SnowflakeConnection(**self.get_db_credentials()).get_conn()

    def get_db_credentials(self):
        file_loc = '../config/snowflake_credentials.yml'
        with open(file_loc) as db_credentials:
            credentials = yaml.load(db_credentials, Loader=yaml.FullLoader)
            return credentials

if __name__ == '__main__':
    DBModel()

