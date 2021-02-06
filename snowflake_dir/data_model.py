from snowflake_dir.snowflake_connect import SnowflakeConnection
import yaml
from infra.aws_pys3 import AWS_Connect
from snowflake_dir.data_objects import Stage, Table, File_Format, Pipe, View, Integration


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

    def create_integration(self):
        integration_properties = (self.object_properties.get('integration'),
                                  self.object_properties.get('stage').get('bucket'))
        Integration(self.conn).create_object(integration_properties)
        print(f'Storage Integration created')

        return Integration(self.conn).get_integration_props(self.object_properties.get('integration'))

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

    def get_pipe_arn(self):
        return Pipe(self.conn).get_arn(self.get_db_credentials())


if __name__ == '__main__':
    dbmod = DBModel()
    aws = AWS_Connect()
    # aws.create_bucket()
    # STORAGE_AWS_EXTERNAL_ID, STORAGE_AWS_IAM_USER_ARN = dbmod.create_integration()
    # aws.update_trust(STORAGE_AWS_EXTERNAL_ID, STORAGE_AWS_IAM_USER_ARN)
    # dbmod.execute_sql()
    arn = dbmod.get_pipe_arn()
    aws.notifications(arn)
    aws.upload_directory()



