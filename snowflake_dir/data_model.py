from snowflake_dir.snowflake_connect import SnowflakeConnection
import yaml
from snowflake_dir.sql_statements import SQLGenerator


class DBModel():
    def __init__(self):
        self.conn = SnowflakeConnection(**self.get_db_credentials()).get_conn()
        self.sql = SQLGenerator()

    def get_db_credentials(self):
        file_loc = '../config/snowflake_credentials.yml'
        with open(file_loc) as db_credentials:
            credentials = yaml.load(db_credentials, Loader=yaml.FullLoader)
            return credentials

    def execute_sql(self):
        self.conn.cursor().execute(self.sql.create_stage())
        self.conn.cursor().execute(self.sql.cust_table())
        self.conn.cursor().execute(self.sql.cust_file_format())
        self.conn.cursor().execute(self.sql.copy_into_customers())
        self.conn.cursor().execute(self.sql.product_table())
        self.conn.cursor().execute(self.sql.copy_into_product())
        self.conn.cursor().execute(self.sql.tran_file_format())
        self.conn.cursor().execute(self.sql.transactions_table())
        self.conn.cursor().execute(self.sql.copy_into_transactions())
        self.conn.cursor().execute(self.sql.transaction_data_view())
        self.conn.cursor().execute(self.sql.final_view())
        print("finish")


if __name__ == '__main__':
    dbmod = DBModel()
    dbmod.execute_sql()

