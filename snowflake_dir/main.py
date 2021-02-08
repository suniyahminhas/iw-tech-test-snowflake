from infra.aws_pys3 import AWS_Connect
from snowflake_dir.data_model import DBModel
import yaml
import logging
import datetime

# mode = "CREATE" to create model, mode = "DELETE" to delete model
mode = "DELETE"

open(f'logs/logfile', 'w').close()
logging.basicConfig(filename=f'logs/logfile.log', level=logging.INFO,
                    format="%(asctime)s :: %(name)s :: %(levelname)s :: %(message)s")
logger = logging.getLogger(__name__)


def get_obj_details():
    """
    load object details in dictionary format from yml file in config
    """
    file_loc = 'config/object_details.yml'
    try:
        with open(file_loc) as obj_details:
            credentials = yaml.load(obj_details, Loader=yaml.FullLoader)
            logger.info("object details successfully read and loaded")
            return credentials
    except FileNotFoundError:
        logger.error("object_details.yml file does not exist")


db_mod = DBModel(get_obj_details())
aws = AWS_Connect(get_obj_details())


def create_all():
    """
    create an S3 bucket
    create table, file formats, pipes, storage integrations and views  within snowflake
    secure a trust relationships between S3 and Snowflake
    Set up event notifications on S3 bucket to Snowflake Pipes
    Ingest data into Bucket
    """
    aws.create_bucket()
    storage_aws_external_id, storage_aws_iam_user_arn = db_mod.create_integration()
    aws.update_trust(storage_aws_external_id, storage_aws_iam_user_arn)
    db_mod.execute_sql()
    arn = db_mod.get_pipe_arn()
    aws.notifications(arn)
    aws.upload_directory()


if __name__ == '__main__':
    logger.info('iw_tech_test begin')
    if mode == "CREATE":
        create_all()
    if mode == "DELETE":
        db_mod.drop_objects()
        aws.delete_bucket()
