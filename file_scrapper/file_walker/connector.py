import os
from abc import ABCMeta, abstractmethod

import boto3
from botocore.exceptions import NoCredentialsError
from google.cloud import storage


class BaseConnector(metaclass=ABCMeta):
    """
    Abstract class to set the connection over cloud
    """

    @abstractmethod
    def connect_to_client(self):
        pass

    @abstractmethod
    def upload_to_resource(self, *args, **kwargs):
        pass


class AWSConnector(BaseConnector):
    def connect_to_client(self):
        """
        Connect to GCS using project id
        """
        client = None
        try:
            access_key = os.getenv('AWS_ACCESS_KEY')
            secret_key = os.getenv('AWS_SECRET_KEY')
            client = boto3.client(
                's3',
                aws_access_key_id=access_key,
                aws_secret_access_key=secret_key
            )
            print("AWS connected successfully")
        except NoCredentialsError:
            print("Something wrong with Credentials")
        except Exception as e:
            print(f"Something Went Wrong:{e}")

        return client

    def upload_to_resource(self, file_path: str, bucket_name: str, filename: str, client: boto3.client):
        """
        upload the files to GCS
        """
        try:
            client.upload_file(filename, bucket_name, file_path)
            print("File Uploaded successful")
        except NoCredentialsError:
            print("AWS credentials not found")
        except Exception as e:
            print(f"Something Went Wrong:{e}")


class GCSConnector(BaseConnector):
    def connect_to_client(self):
        """
        Connect to GCS using project id
        """
        client = None
        try:
            client = storage.Client()
            print("GCS Connected Successfully")
        except Exception as e:
            print(f"Error connecting to GCS: {str(e)}")

        return client

    def upload_to_resource(self, file_path: str, bucket_name: str, filename: str, client: storage.Client):
        """
        upload the files to GCS
        """
        try:
            bucket = client.bucket(bucket_name)
            blob = bucket.blob(filename)
            blob.upload_from_filename(file_path)
            print("File Uploaded successful")
        except Exception as e:
            print(f"Error uploading to GCS: {str(e)}")
