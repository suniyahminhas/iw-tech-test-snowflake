class DBObject():
    def __init__(self, conn):
        self.conn = conn

    def create_object(self, properties):
        self.conn.cursor().execute(self.create_ddl(properties))

    def create_ddl(self, properties):
        pass


class Stage(DBObject):
    def __init__(self, conn):
        DBObject.__init__(self, conn=conn)

    def create_ddl(self, properties):
        return f"""CREATE OR REPLACE STAGE {properties.get('name')}
        URL = 's3://{properties.get('bucket')}'
        CREDENTIALS =
        (AWS_KEY_ID = 'AKIAWBJZDLJM4VMJ4K7Q'
        AWS_SECRET_KEY = 'GX7Xwuj6J9cTPqSraRdEQIwsUqC7Lo5/w3FbrRSj');"""


class Table(DBObject):
    def __init__(self, conn):
        DBObject.__init__(self, conn=conn)

    def create_ddl(self, properties):
        ddl = f"""CREATE OR REPLACE TABLE {properties.get('name')} ("""
        for col, data_typ in properties.get('table_cols').items():
            ddl += f"\n   {col} {data_typ},"
        ddl = ddl[:-1] + ");"
        return ddl


class File_Format(DBObject):
    def __init__(self, conn):
        DBObject.__init__(self, conn=conn)

    def create_ddl(self, properties):
        skip_header = f" SKIP_HEADER = {properties.get('skip_header')}" if properties.get('skip_header') >= 1 else ""
        return f"""CREATE OR REPLACE FILE FORMAT {properties.get('name')}
                    TYPE = '{properties.get('type')}'{skip_header};"""


class Pipe(DBObject):
    def __init__(self, conn):
        DBObject.__init__(self, conn=conn)

    def create_ddl(self, properties):
        table_properties, file_format_name, stage_name = properties
        return f"""create or replace pipe {table_properties.get('name')}_PIPE auto_ingest = true as
                    COPY INTO {table_properties.get('name')}
                    FROM @{stage_name}/{table_properties.get('bucket_path')}
                    FILE_FORMAT = (FORMAT_NAME = {file_format_name});"""

class View(DBObject):
    def __init__(self, conn):
        DBObject.__init__(self, conn=conn)

    def create_ddl(self, properties):
        return f"""create or replace view {properties.get('name')} as\n{properties.get('definition')};"""


if __name__ == '__main__':
    pass
