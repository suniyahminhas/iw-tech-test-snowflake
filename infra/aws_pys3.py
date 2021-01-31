#!/usr/bin/python
"""Usage: Add bucket name and credentials
          script.py <source folder> <s3 destination folder >"""

import os
import boto3
from botocore.exceptions import NoCredentialsError, ClientError

ACCESS_KEY = 'AWS_ACCESS_KEY_ID'
SECRET_KEY = 'AWS_SECRET_ACCESS_KEY'
AWS_DEFAULT_REGION = 'AWS_DEFAULT_REGION'
bucket_name = 'iw2021purchases'
local_folder = '../input_data/starter/'
s3_folder = 'infinity_works'
walks = os.walk(local_folder)



def main():
    """entry point"""
    access = os.environ['ACCESS_KEY']
    secret = os.environ['SECRET_KEY']
    s3 = boto3.resource('s3', aws_access_key_id=access, aws_secret_access_key=secret)

    create_bucket(bucket_name, s3)
    upload_directory(s3)
    # delete_bucket(bucket_name, s3)


def create_bucket(name, s3):
    try:
        location = {'LocationConstraint': 'eu-west-2'}
        bucket = s3.create_bucket(Bucket= name) #, CreateBucketConfiguration=location)
    except ClientError as ce:
        print('error', ce) # log me


def upload_directory(s3):
    try:
        my_bucket = s3.Bucket(bucket_name)
        for path, subdirs, files in os.walk(local_folder):
            path = path.replace("\\","/")
            directory_name = path.replace(local_folder,"")
            for file in files:
                my_bucket.upload_file(os.path.join(path, file), directory_name+'/'+file)
    except Exception as err:
        print(err)

def delete_bucket(bucket, s3):
    bucket= s3.Bucket(bucket)
    for key in bucket.objects.all():
        key.delete()
    bucket.delete()

if __name__ == '__main__':
    main()








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