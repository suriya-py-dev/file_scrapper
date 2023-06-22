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
    def connect_to_aws_client(self):
        pass

    @abstractmethod
    def upload_to_aws_resource(self, *args, **kwargs):
        pass

    @abstractmethod
    def connect_to_gcs_client(self):
        pass

    @abstractmethod
    def upload_to_gcs_resource(self, *args, **kwargs):
        pass


class Connector(BaseConnector):
    def __init__(self):
        self.gcs_client = None
        self.aws_client = None

    def connect_to_aws_client(self):
        """
        Connect to GCS using project id
        """
        try:
            access_key = os.getenv('AWS_ACCESS_KEY')
            secret_key = os.getenv('AWS_SECRET_KEY')
            self.aws_client = boto3.client(
                's3',
                aws_access_key_id=access_key,
                aws_secret_access_key=secret_key
            )
            print("AWS connected successfully")
        except NoCredentialsError:
            print("Something wrong with Credentials")
        except Exception as e:
            print(f"Something Went Wrong:{e}")

    def upload_to_aws_resource(self, file_path: str, bucket_name: str):
        """
        upload the files to GCS
        """
        try:
            self.aws_client.upload_file(file_path, bucket_name, file_path)
            print("File Uploaded successful")
        except NoCredentialsError:
            print("AWS credentials not found")
        except Exception as e:
            print(f"Something Went Wrong:{e}")

    def connect_to_gcs_client(self):
        """
        Connect to GCS using project id
        """
        try:
            gcs_project_id = os.getenv('GCS_PROJECT_ID')
            self.gcs_client = storage.Client(project=gcs_project_id)
            print("GCS Connected Successfully")
        except Exception as e:
            print(f"Error connecting to GCS: {str(e)}")

    def upload_to_gcs_resource(self, file_path: str, bucket_name: str):
        """
        upload the files to GCS
        """
        try:
            bucket = self.gcs_client.bucket(bucket_name)
            blob = bucket.blob(file_path)
            blob.upload_from_filename(file_path)
            print("File Uploaded successful")
        except Exception as e:
            print(f"Error uploading to GCS: {str(e)}")
