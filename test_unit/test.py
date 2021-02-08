import unittest
from unittest import TestCase
from unittest.mock import MagicMock
from snowflake_dir.data_objects import Stage, Table, File_Format, Pipe, Integration

class TestDataObjects(TestCase):

    def setUp(self):
        self.conn = MagicMock()

    def test_stage_create_ddl(self):
        ddl = Stage(conn=self.conn).create_ddl({'name': 'MY_BUCKET', 'bucket': 'suniminhas20210206'})
        self.assertEqual(ddl, f"""CREATE OR REPLACE STAGE MY_BUCKET
        URL = 's3://suniminhas20210206'
        CREDENTIALS =
        (AWS_KEY_ID = 'AKIAWBJZDLJM4VMJ4K7Q'
        AWS_SECRET_KEY = 'GX7Xwuj6J9cTPqSraRdEQIwsUqC7Lo5/w3FbrRSj');""")

    def test_table_create_ddl(self):
        ddl = Table(conn=self.conn).create_ddl({'name': 'CUSTOMERS',
            'file_format': 'csv_file_format',
            'bucket_path': '/customers',
            'table_cols': {'customer_id': 'VARCHAR(10)', 'loyalty_score': 'INTEGER'}})
        self.assertEqual(ddl, f"""CREATE OR REPLACE TABLE CUSTOMERS (\n   customer_id VARCHAR(10),\n   loyalty_score INTEGER);""")

    def test_pipe_create_ddl(self):
        ddl = Pipe(conn=self.conn).create_ddl(({'name': 'CUSTOMERS',
            'file_format': 'csv_file_format',
            'bucket_path': '/customers',
            'table_cols': {'customer_id': 'VARCHAR(10)', 'loyalty_score': 'INTEGER'}}, "file_format", "stage_name"))
        self.assertEqual(ddl, f"""create or replace pipe CUSTOMERS_PIPE auto_ingest = true as
                    COPY INTO CUSTOMERS
                    FROM @stage_name//customers
                    FILE_FORMAT = (FORMAT_NAME = file_format);""")

    def test_ff_create_ddl(self):
        ddl = File_Format(conn=self.conn).create_ddl({'name': 'CSV_FILE_FORMAT', 'type': 'CSV', 'skip_header': 1})
        self.assertEqual(ddl, f"""CREATE OR REPLACE FILE FORMAT CSV_FILE_FORMAT
                    TYPE = 'CSV' SKIP_HEADER = 1;""")

    def test_integration_create_ddl(self):
        ddl = Integration(conn=self.conn).create_ddl(({'name': 'iw2021', 'arn': 'arn:aws:iam::415120972377:role/snowflake_role2'}, "bucket_name"))
        self.assertEqual(ddl, f"""CREATE STORAGE INTEGRATION iw2021
                      TYPE = EXTERNAL_STAGE
                      STORAGE_PROVIDER = S3
                      ENABLED = TRUE
                      STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::415120972377:role/snowflake_role2'
                      STORAGE_ALLOWED_LOCATIONS = ('s3://bucket_name');""")

























# integration = {'integration': {'name': 'iw2021', 'arn': 'arn:aws:iam::415120972377:role/snowflake_role2'}}
# stage = {'stage': {'name': 'MY_BUCKET', 'bucket': 'suniminhas20210206'}}
# file_format = {'file_format': {'csv_file_format': {'name': 'CSV_FILE_FORMAT', 'type': 'CSV', 'skip_header': 1},
#                                'json_file_format': {'name': 'JSON_FILE_FORMAT', 'type': 'JSON', 'skip_header': 0}}}
# table_pipe = {'table': {'customers_table': {'name': 'CUSTOMERS',
#                                             'file_format': 'csv_file_format',
#                                             'bucket_path': '/customers',
#                                             'table_cols': {'customer_id': 'VARCHAR(10)', 'loyalty_score': 'INTEGER'}},
#                         'product_table': {'name': 'PRODUCTS',
#                                           'file_format': 'csv_file_format',
#                                           'bucket_path': '/products',
#                                           'table_cols': {'product_id': 'VARCHAR(10)',
#                                                          'product_description': 'VARCHAR(250)',
#                                                          'product_category': 'VARCHAR(250)'}}}},
# view = {'view': {'transaction_view': {'name': 'TRANSACTION_DATA', 'definition': 'defA'},
#                  'final_view': {'name': 'FINAL_VIEW', 'definition': 'defB'}}}