"""Usage: Add bucket name and credentials
          script.py <source folder> <s3 destination folder >"""

import os
import boto3
from botocore.exceptions import NoCredentialsError, ClientError

# ACCESS_KEY = 'AWS_ACCESS_KEY_ID'
# SECRET_KEY = 'AWS_SECRET_ACCESS_KEY'
# AWS_DEFAULT_REGION = 'AWS_DEFAULT_REGION'
# bucket_name = 'iw2021purchases'


class AWS_Connect():
    def __init__(self):
        """entry point"""
        self.access = 'AKIAWBJZDLJM4VMJ4K7Q' #os.environ['ACCESS_KEY']
        self.secret = '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' #os.environ['SECRET_KEY']
        self.s3 = boto3.resource('s3', aws_access_key_id=self.access, aws_secret_access_key=self.secret)
        self.local_folder = '../input_data/starter/'
        self.s3_folder = 'infinity_works'
        self.walks = os.walk(self.local_folder)
        self.bucket_name = 'suniminhas20210206'
        self.role_name = 'snowflake_role2'

        # create_bucket(bucket_name, s3)
        # do this after pipes have been implemented and you have arn code
        # notifications(s3, 'arn:aws:sqs:us-east-1:270302263326:sf-snowpipe-AIDAT532FHAPF73NNMW7U-J4G4DZckgFAYLfTwFfOmHg')
        # upload_directory(s3)
        # delete_bucket(bucket_name, s3)
        # update_trust()

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
        # response = client.get_role(RoleName=self.role_name)
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
#         response = client.update_assume_role_policy(
#             RoleName=self.role_name,
#             PolicyDocument="""{
#   "Statement": [
#     {
#       "Sid": "",
#       "Effect": "Allow",
#       "Principal": {
#         "AWS": "arn:aws:iam::270302263326:user/ahpp-s-iest2181"
#       },
#       "Action": "sts:AssumeRole",
#       "Condition": {
#         "StringEquals": {
#           "sts:ExternalId": "XK87678_SFCRole=2_kK+LRUMZoT6bUoXdw8DMMo42ocQ="
#         }
#       }
#     }
#   ]
# }"""
#        )

        # trust_policy = response['Role']['AssumeRolePolicyDocument']
        # # change effect to `Deny`
        # trust_policy['Statement'][0]['Condition']['StringEquals']["sts:ExternalId"] = STORAGE_AWS_EXTERNAL_ID
        # # change principle to '123456'
        # trust_policy['Statement'][0]['Principal']['AWS'] = STORAGE_AWS_IAM_USER_ARN


    def create_bucket(self):
        try:
            location = {'LocationConstraint': 'eu-west-2'}
            bucket = self.s3.create_bucket(Bucket= self.bucket_name) #, CreateBucketConfiguration=location)
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
        bucket= self.s3.Bucket(self.bucket_name)
        for key in bucket.objects.all():
            key.delete()
        bucket.delete()

if __name__ == '__main__':
    connect = AWS_Connect()
    connect.create_bucket()









# #!/usr/bin/env python3
#
# # -*-coding:utf-8 -*-
#
# """Transfer data to amazon S3"""
# import os
# import boto3
# from botocore.exceptions import ClientError
#
# ACCESS_KEY = 'AKIAWBJZDLJM4VMJ4K7Q'
# SECRET_KEY = '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'
#
# PRI_BUCKET_NAME = 'sm30012021test'
# TRANSIENT_BUCKET_NAME = 'suni2021'
# DIR = '../input_data/starter'
# F1 = 'customers.csv'
#
# # make env variables
#

#

# # def upload_file(bucket, directory, file, s3, s3path=None):
# #     file_path = directory + '/' + file
# #     remote_path = s3path
# #     if remote_path is None:
# #         remote_path = file
# #     try:
# #         s3.Bucket(bucket).upload_file(file_path, remote_path)
# #     except ClientError as ce:
# #         print('error', ce)
#
# def upload_directory(bucket, path, s3):
#     for root,dirs,files in os.walk(path):
#         print(root, dirs, files)
#         for file in files:
#             print(file)
#             try:
#                 s3.Bucket(bucket).upload_file(os.path.join(root,file), file)
#             except ClientError as ce:
#                 print('error', ce)
#
# def delete_files(bucket, keys, s3):
#     objects = []
#     for key in keys:
#         object.append({'Key': key})
#     try:
#         s3.Bucket(bucket).delete_objects(Delete = {'Objects': objects})
#     except ClientError as ce:
#         print('error', ce)
#
# if __name__ == '__main__':
#     main()


"""For file names"""
# # def upload_directory(s3):
# #     for source, dirs, files in walks:
# #         print('Directory: ' + source)
# #         for filename in files:
# #             # construct the full local path
# #             local_file = os.path.join(source, filename)
# #             # construct the full Dropbox path
# #             relative_path = os.path.relpath(local_file, local_folder)
# #             s3_file = os.path.join(s3_folder, relative_path)
# #             # Invoke upload function
# #             upload_to_aws(bucket_name, local_file, s3_file, s3)
#
# # Function to upload to s3
# # def upload_to_aws(bucket, local_file, s3_file, s3):
# #     """local_file, s3_file can be paths"""
# #     print('  Uploading ' +local_file + ' as ' + bucket + '/' +s3_file)
# #     try:
# #         s3.upload_file(local_file, bucket, s3_file)
# #         print('  '+s3_file + ": Upload Successful")
# #         print('  ---------')
# #         return True
# #     except NoCredentialsError:
# #         print("Credentials not available")
# #         return False




#     response = client.create_policy(
#         PolicyName='snowflake_new',
#         PolicyDocument="""../config/policy.json""",
#         Description='snowflake policy'
#     )
# #     response = client.create_role(
# #         RoleName='snowflake_new',
# #         AssumeRolePolicyDocument="""{
# #     "Version": "2012-10-17",
# #     "Statement": [
# #         {
# #             "Effect": "Allow",
# #             "Action": [
# #                 "s3:GetObject",
# #                 "s3:GetObjectVersion"
# #             ],
# #             "Resource": "arn:aws:s3:::<bucket>/<prefix>/*"
# #         },
# #         {
# #             "Effect": "Allow",
# #             "Action": "s3:ListBucket",
# #             "Resource": "arn:aws:s3:::iw2021purchases",
# #             "Condition": {
# #                 "StringLike": {
# #                     "s3:prefix": [
# #                         "*"
# #                     ]
# #                 }
# #             }
# #         }
# #     ]
# # }"""
#    # )
    # response = iam.get_role(RoleName='mysnowflakerole')
    # trust_policy = response['Role']['AssumeRolePolicyDocument']
    # print(trust_policy)
    # # change effect to `Deny`
    # trust_policy['Statement'][0]['Condition']['StringEquals']["sts:ExternalId"] = 'XK87678_SFCRole=2_+mAL9UHtbHDsIzqjpmdYOcX4058=' #STORAGE_AWS_EXTERNAL_ID '
    # # change principle to '123456'
    # trust_policy['Statement'][0]['Principal']['AWS'] = 'arn:aws:iam::270302263326:user/ahpp-s-iest2181' #'STORAGE_AWS_IAM_USER_ARN'
    # print(trust_policy)

