from infra.aws_pys3 import AWS_Connect
from snowflake_dir.data_model import DBModel



if __name__ == '__main__':
    dbmod = DBModel()
    aws = AWS_Connect()
    aws.create_bucket()
    STORAGE_AWS_EXTERNAL_ID, STORAGE_AWS_IAM_USER_ARN = dbmod.create_integration()
    aws.update_trust(STORAGE_AWS_EXTERNAL_ID, STORAGE_AWS_IAM_USER_ARN)
    dbmod.execute_sql()
    arn = dbmod.get_pipe_arn()
    aws.notifications(arn)
    aws.upload_directory()