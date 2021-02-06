from snowflake_dir.snowflake_connect import SnowflakeConnection
import yaml
import infra.aws_pys3 as aws
from snowflake_dir.data_objects import Stage, Table, File_Format, Pipe, View


class DBModel():
    def __init__(self):
        self.conn = SnowflakeConnection(**self.get_db_credentials()).get_conn()
        self.object_properties = self.get_obj_details()

    def get_db_credentials(self):
        file_loc = '../config/snowflake_credentials.yml'
        with open(file_loc) as db_credentials:
            credentials = yaml.load(db_credentials, Loader=yaml.FullLoader)
            return credentials

    def get_obj_details(self):
        file_loc = '../config/object_details.yml'
        with open(file_loc) as obj_details:
            credentials = yaml.load(obj_details, Loader=yaml.FullLoader)
            return credentials

    def execute_sql(self):
        self.create_stage()
        self.create_file_format()
        self.create_tables_and_pipes()
        self.create_views()

    def create_stage(self):
        stage_properties = self.object_properties.get('stage')
        Stage(self.conn).create_object(stage_properties)

    def create_file_format(self):
        for file_format, file_format_properties in self.object_properties.get('file_format').items():
            File_Format(self.conn).create_object(file_format_properties)
            print(f'{file_format} created')

    def create_tables_and_pipes(self):
        for table, table_properties in self.object_properties.get('table').items():
            Table(self.conn).create_object(table_properties)
            print(f'{table} created')

            table_file_format = table_properties.get('file_format')
            pipe_properties = (table_properties,
                               self.object_properties.get('file_format').get(table_file_format).get('name'),
                               self.object_properties.get('stage').get('name'))
            Pipe(self.conn).create_object(pipe_properties)
            print(f'{table}_PIPE created')

    def create_views(self):
        for view, view_properties in self.object_properties.get('view').items():
            View(self.conn).create_object(view_properties)
            print(f'{view} created')


if __name__ == '__main__':
    dbmod = DBModel()
    dbmod.execute_sql()
    aws.main()
