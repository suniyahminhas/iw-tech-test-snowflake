import os
import boto3
from botocore.exceptions import ClientError


class AWS_Connect():
    def __init__(self, object_properties):
        """entry point"""
        self.access = os.environ['ACCESS_KEY']
        self.secret = os.environ['SECRET_KEY']
        self.s3 = boto3.resource('s3', aws_access_key_id=self.access, aws_secret_access_key=self.secret)
        self.local_folder = object_properties.get('aws').get('data_path')#'../input_data/starter/'
        self.walks = os.walk(self.local_folder)
        self.bucket_name = object_properties.get('stage').get('bucket') #'suniminhas20210206'
        self.role_name = object_properties.get('aws').get('role_for_trust') #'snowflake_role2'


    def notifications(self, arn):
        bucket_notification = self.s3.BucketNotification(self.bucket_name)
        response = bucket_notification.put(NotificationConfiguration={
            'QueueConfigurations': [
                {
                    'Id': 'snowflake',
                    'QueueArn': arn,
                    'Events': ['s3:ObjectCreated:*'],
                },
            ]
        })


    def update_trust(self, STORAGE_AWS_EXTERNAL_ID, STORAGE_AWS_IAM_USER_ARN):
        client = boto3.client('iam', aws_access_key_id=self.access, aws_secret_access_key=self.secret)
        response = client.update_assume_role_policy(
            RoleName=self.role_name,
            PolicyDocument='''{
          "Statement": [
            {
              "Sid": "",
              "Effect": "Allow",
              "Principal": {
                "AWS": "''' + STORAGE_AWS_IAM_USER_ARN + '''"
              },
              "Action": "sts:AssumeRole",
              "Condition": {
                "StringEquals": {
                  "sts:ExternalId": "''' + STORAGE_AWS_EXTERNAL_ID + '''"
                }
              }
            }
          ]
        }'''
            )


    def create_bucket(self):
        try:
            bucket = self.s3.create_bucket(Bucket= self.bucket_name)
        except ClientError as ce:
            print('error', ce) # log me


    def upload_directory(self):
        try:
            my_bucket = self.s3.Bucket(self.bucket_name)
            for path, subdirs, files in os.walk(self.local_folder):
                path = path.replace("\\","/")
                directory_name = path.replace(self.local_folder,"")
                for file in files:
                    my_bucket.upload_file(os.path.join(path, file), directory_name+'/'+file)
        except Exception as err:
            print(err)

    def delete_bucket(self):
        bucket = self.s3.Bucket(self.bucket_name)
        for key in bucket.objects.all():
            key.delete()
        bucket.delete()

if __name__ == '__main__':
    connect = AWS_Connect()
    connect.create_bucket()



