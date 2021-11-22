import boto3
import os
import json

class S3():

    def __init__(self):
        
        self.aws_secret_access_key = os.environ["AWS_SECRET_KEY_1"]
        self.aws_access_key_id = os.environ["AWS_ACCESS_KEY_1"]
        self.s3_client =  boto3.client('s3',aws_access_key_id = self.aws_access_key_id ,aws_secret_access_key = self.aws_secret_access_key)


    def upload_File(self, bucket, uploadFile, uploadPath):

        with open(uploadFile, "rb") as f:
            self.s3_client.upload_fileobj(f, bucket, uploadPath)


    def upload(self, bucket, result, uploadPath):
        
        uploadByteStream = bytes(json.dumps(result).encode('UTF-8'))

        self.s3_client.put_object(Bucket = bucket, Key = uploadPath, Body = uploadByteStream)


    def download_s3_file(bucket, fileName, LocalFileName):
        s2_client =  boto3.client('s3',aws_access_key_id = os.environ["AWS_ACCESS_KEY_1"] ,aws_secret_access_key = os.environ["AWS_SECRET_KEY_1"])
        s2_client.download_file(Bucket = bucket,Key = fileName, FileName = LocalFileName)






            