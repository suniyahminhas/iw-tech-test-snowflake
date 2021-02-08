import pandas as pd
from snowflake.connector.errors import BadRequest
import logging

logger = logging.getLogger(__name__)


class DBObject():
    def __init__(self, conn, object):
        self.conn = conn
        self.object = object

    def create_object(self, properties):
        try:
            self.conn.cursor().execute(self.create_ddl(properties))
            logger.info(f'Successfully created instance of {self.object} in Snowflake')
        except BadRequest as err:
            logger.error(f'Bad Request for {self.object} {err}')

    def drop_object(self, name):
        try:
            self.conn.cursor().execute(f'DROP {self.object} {name};')
            logger.info(f'Successfully dropped instance of {self.object} in Snowflake')
        except BadRequest as err:
            logger.error(f'Bad Request for {self.object} {err}')


class Stage(DBObject):
    def __init__(self, conn):
        DBObject.__init__(self, conn=conn, object="STAGE")

    def create_ddl(self, properties):
        return f"""CREATE OR REPLACE STAGE {properties.get('name')}
        URL = 's3://{properties.get('bucket')}'
        CREDENTIALS =
        (AWS_KEY_ID = 'AKIAWBJZDLJM4VMJ4K7Q'
        AWS_SECRET_KEY = 'GX7Xwuj6J9cTPqSraRdEQIwsUqC7Lo5/w3FbrRSj');"""


class Table(DBObject):
    def __init__(self, conn):
        DBObject.__init__(self, conn=conn, object="TABLE")

    def create_ddl(self, properties):
        ddl = f"""CREATE OR REPLACE TABLE {properties.get('name')} ("""
        for col, data_typ in properties.get('table_cols').items():
            ddl += f"\n   {col} {data_typ},"
        ddl = ddl[:-1] + ");"
        return ddl


class File_Format(DBObject):
    def __init__(self, conn):
        DBObject.__init__(self, conn=conn, object="FILE FORMAT")

    def create_ddl(self, properties):
        skip_header = f" SKIP_HEADER = {properties.get('skip_header')}" if properties.get('skip_header') >= 1 else ""
        return f"""CREATE OR REPLACE FILE FORMAT {properties.get('name')}
                    TYPE = '{properties.get('type')}'{skip_header};"""


class Pipe(DBObject):
    def __init__(self, conn):
        DBObject.__init__(self, conn=conn, object="PIPE")

    def create_ddl(self, properties):
        table_properties, file_format_name, stage_name = properties
        return f"""create or replace pipe {table_properties.get('name')}_PIPE auto_ingest = true as
                    COPY INTO {table_properties.get('name')}
                    FROM @{stage_name}/{table_properties.get('bucket_path')}
                    FILE_FORMAT = (FORMAT_NAME = {file_format_name});"""

    def get_arn(self, properties):
        df = pd.read_sql(f'SHOW PIPES IN SCHEMA {properties.get("schema")};', self.conn)
        return df["notification_channel"][0]


class View(DBObject):
    def __init__(self, conn):
        DBObject.__init__(self, conn=conn, object="VIEW")

    def create_ddl(self, properties):
        return f"""create or replace view {properties.get('name')} as\n{properties.get('definition')};"""


class Integration(DBObject):
    def __init__(self, conn):
        DBObject.__init__(self, conn=conn, object="STORAGE INTEGRATION")

    def create_ddl(self, properties):
        int_properties, bucket = properties
        return f"""CREATE STORAGE INTEGRATION {int_properties.get('name')}
                      TYPE = EXTERNAL_STAGE
                      STORAGE_PROVIDER = S3
                      ENABLED = TRUE
                      STORAGE_AWS_ROLE_ARN = '{int_properties.get('arn')}'
                      STORAGE_ALLOWED_LOCATIONS = ('s3://{bucket}');"""

    def get_integration_props(self, properties):
        df = pd.read_sql(f"DESC INTEGRATION {properties.get('name')};", self.conn)
        df = df.set_index("property")
        return df["property_value"]["STORAGE_AWS_EXTERNAL_ID"], df["property_value"]["STORAGE_AWS_IAM_USER_ARN"]
