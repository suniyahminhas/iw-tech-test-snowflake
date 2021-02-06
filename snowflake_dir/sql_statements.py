import yaml

class SQLGenerator():
    def __init__(self): #, **kwargs):
        pass
        # self.aws = kwargs.get("aws_bucket")
        # self.customers_table_name = kwargs.get("customers_table").get('table_name')
        # self.customers_table_cols = kwargs.get("customers_table").get('table_cols')
        # print(self.aws, self.customers_table_name)


    def create_stage(self):
        return f"""CREATE STAGE MY_BUCKET
        URL = 's3://iw2021purchases'
        CREDENTIALS =
        (AWS_KEY_ID = 'AKIAWBJZDLJM4VMJ4K7Q'
        AWS_SECRET_KEY = 'GX7Xwuj6J9cTPqSraRdEQIwsUqC7Lo5/w3FbrRSj');"""

    def cust_table(self):
        return f"""CREATE OR REPLACE TABLE CUSTOMERS (
                        customer_id VARCHAR(10),
                        loyalty_score INTEGER);"""

    def cust_file_format(self):
        return f"""CREATE OR REPLACE FILE FORMAT CUSTOMER_FILE_FORMAT
                    TYPE = 'CSV' SKIP_HEADER=1;"""

    def copy_into_customers(self):
        return f"""COPY INTO CUSTOMERS
            FROM @MY_BUCKET//
            FILES = ('customers.csv')
            FILE_FORMAT = (FORMAT_NAME = CUSTOMER_FILE_FORMAT);"""

    def product_table(self):
        return f"""CREATE OR REPLACE TABLE PRODUCTS (
                product_id VARCHAR(10),
                product_description VARCHAR(250),
                product_category VARCHAR(250));"""

    def copy_into_product(self):
        return f"""COPY INTO products
                FROM @MY_BUCKET//
                FILES = ('products.csv')
                FILE_FORMAT = (FORMAT_NAME = CUSTOMER_FILE_FORMAT);"""


    def tran_file_format(self):
        return f"""CREATE FILE FORMAT TRANSACTIONS_FILE_FORMAT TYPE = 'JSON';"""

    def transactions_table(self):
        return f"""CREATE OR REPLACE TABLE TRANSACTIONS (
                transaction VARIANT);"""

    def copy_into_transactions(self):
        return f"""COPY INTO TRANSACTIONS
            FROM @MY_BUCKET/transactions
            FILE_FORMAT = (FORMAT_NAME = TRANSACTIONS_FILE_FORMAT);"""


    def transaction_data_view(self):
        return f"""CREATE OR REPLACE VIEW TRANSACTION_DATA AS
                SELECT product_id, customer_id, count(product_id) AS purchase_count
                FROM (
                SELECT value:product_id::VARCHAR AS PRODUCT_ID, transaction:customer_id::STRING as customer_id
                from TRANSACTIONS,
                LATERAL FLATTEN
                (input => transaction:basket)
                )
                GROUP BY (product_id, customer_id);"""

    def final_view(self):
        return f"""CREATE OR REPLACE VIEW FINAL_VIEW AS
                SELECT A.product_id, A.customer_id, A.purchase_count, B.loyalty_score, C.product_category
                FROM TRANSACTION_DATA A
                LEFT JOIN
                CUSTOMERS B
                on A.customer_id = B.customer_id
                LEFT JOIN
                PRODUCTS C
                on A.product_id = C.product_id;"""


if __name__ == '__main__':
    pass
    # def get_db_credentials():
    #     file_loc = '../config/object_details.yml'
    #     with open(file_loc) as db_credentials:
    #         credentials = yaml.load(db_credentials, Loader=yaml.FullLoader)
    #         print(credentials)
    #         return credentials
    # SQLGenerator(**get_db_credentials())