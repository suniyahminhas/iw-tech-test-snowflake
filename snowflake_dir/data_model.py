from snowflake_dir.snowflake_connect import SnowflakeConnection
import yaml
from snowflake_dir.data_objects import Stage, Table, File_Format, Pipe, View, Integration
import logging

logger = logging.getLogger(__name__)

class DBModel():
    def __init__(self, object_properties):
        self.conn = SnowflakeConnection(**self.get_db_credentials()).get_conn()
        self.object_properties = object_properties
        logger.info("DBModel class initialised, Snowflake connection set")

    def get_db_credentials(self):
        file_loc = 'config/snowflake_credentials.yml'
        try:
            with open(file_loc) as db_credentials:
                credentials = yaml.load(db_credentials, Loader=yaml.FullLoader)
                logger.info("snowflake credentials obtained")
                return credentials
        except FileNotFoundError:
            logger.error("snowflake_credentials.yml file does not exist")

    def create_integration(self):
        try:
            integration_properties = (self.object_properties.get('integration'),
                                      self.object_properties.get('stage').get('bucket'))
            Integration(self.conn).create_object(integration_properties)
            logger.info(f'Storage Integration created')
            return Integration(self.conn).get_integration_props(self.object_properties.get('integration'))
        except ValueError:
            logger.info(f'Storage Integration not created successfully: check properties in object_details')

    def execute_sql(self):
        self.create_stage()
        self.create_file_format()
        self.create_tables_and_pipes()
        self.create_views()

    def create_stage(self):
        try:
            stage_properties = self.object_properties.get('stage')
            Stage(self.conn).create_object(stage_properties)
            logger.info(f'Stage created')
        except ValueError:
            logger.info(f'Stage not created successfully: check properties in object_details')

    def create_file_format(self):
        for file_format, file_format_properties in self.object_properties.get('file_format').items():
            try:
                File_Format(self.conn).create_object(file_format_properties)
                logger.info(f'{file_format} File Format created')
            except ValueError:
                logger.info(f'{file_format} File Format not created successfully: check properties in object_details')

    def create_tables_and_pipes(self):
        for table, table_properties in self.object_properties.get('table').items():
            try:
                Table(self.conn).create_object(table_properties)
                logger.info(f'{table} created')
            except ValueError:
                logger.info(f'{table} table not created successfully: check properties in object_details')

            table_file_format = table_properties.get('file_format')
            pipe_properties = (table_properties,
                               self.object_properties.get('file_format').get(table_file_format).get('name'),
                               self.object_properties.get('stage').get('name'))
            try:
                Pipe(self.conn).create_object(pipe_properties)
                logger.info(f'Pipe for table {table} created')
            except ValueError:
                logger.info(f'Pipe for table {table} not created successfully: check properties in object_details')

    def create_views(self):
        for view, view_properties in self.object_properties.get('view').items():
            try:
                View(self.conn).create_object(view_properties)
                logger.info(f'{view} created')
            except ValueError:
                logger.info(f'View {view} not created successfully: check properties in object_details')


    def get_pipe_arn(self):
        try:
            return Pipe(self.conn).get_arn(self.get_db_credentials())
        except ValueError:
            logger.info(f'Pipe arn detail not obtained')

    def drop_objects(self):
        self.drop_views()
        self.drop_table_pipes()
        self.drop_file_format()
        self.drop_stage()
        self.drop_integration()

    def drop_views(self):
        for view, view_properties in self.object_properties.get('view').items():
            view_name = view_properties.get('name')
            View(self.conn).drop_object(name = view_name)

    def drop_table_pipes(self):
        for table, table_properties in self.object_properties.get('table').items():
            Table(self.conn).drop_object(table_properties.get('name'))
            Pipe(self.conn).drop_object(table_properties.get('name')+'_PIPE')

    def drop_file_format(self):
        for file_format, file_format_properties in self.object_properties.get('file_format').items():
            File_Format(self.conn).drop_object(file_format_properties.get('name'))

    def drop_stage(self):
        stage_name = self.object_properties.get('stage').get('name')
        Stage(self.conn).drop_object(stage_name)

    def drop_integration(self):
        integration_name = self.object_properties.get('integration').get('name')
        Integration(self.conn).drop_object(integration_name)




