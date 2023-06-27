"""
Code to load all the files in the folder
 and upload to cloud storage
"""
import os

from file_scrapper.file_walker.connector import GCSConnector, AWSConnector


class MultipleCloudExist(Exception):
    def __init__(self, message):
        self.message = message


class BucketNameNotFound(Exception):
    def __init__(self, name):
        self.message = f"{name} is Missed"


class CredNotFound(Exception):
    def __init__(self, message):
        self.message = message


class FileWalker:
    """
    Code to load all the files in the folder
    and upload to cloud storage
    """
    media_list = ["mp3", "mp4", "mpeg4", "wmv", "3gp", "webm"]
    document_list = ["doc", "docx", "csv", "pd", "txt"]
    image_list = ["jpg", "png", "svg", "webp"]

    def __init__(self):
        self.gcs = None
        self.aws = None

    def _validate_gcs(self, bucket_name: str):
        """Validate GCS cred and bucket name provided or not"""
        gcs_cred = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')

        if not gcs_cred:
            message = "Please add GOOGLE_APPLICATION_CREDENTIALS env variable"
            raise CredNotFound(message)

        if bucket_name is None:
            name = "GCS Bucket Name"
            raise BucketNameNotFound(name)

    def _validate_aws(self, bucket_name: str):
        """Validate AWS cred and bucket name provided or not"""
        access_key = os.getenv('AWS_ACCESS_KEY')
        secret_key = os.getenv('AWS_SECRET_KEY')

        if not access_key or not secret_key:
            message = "One of the key is missed: AWS_ACCESS_KEY or AWS_SECRET_KEY"
            raise CredNotFound(message)

        if bucket_name is None:
            name = "AWS Bucket Name"
            raise BucketNameNotFound(name)

    def walk(self, path_to_walk: str, gcs_bucket_name=None,
             aws_bucket_name=None, all_to_gcs=False,
             all_to_aws=False):
        """
        Walk over all the folders and load the files to cloud storages
        festive-bloom-390715
        """
        if all_to_aws and all_to_gcs:
            message = "Can't upload all data to both cloud storage"
            raise MultipleCloudExist(message)

        # validate only aws is enabled
        if all_to_aws:
            self._validate_aws(aws_bucket_name)

        # validate only gcs is enabled
        if all_to_gcs:
            self._validate_gcs(gcs_bucket_name)

        # validate both enabled
        if not all_to_gcs and not all_to_aws:
            self._validate_aws(aws_bucket_name)
            self._validate_gcs(gcs_bucket_name)

        gcs = GCSConnector().connect_to_client()
        aws = AWSConnector().connect_to_client()
        aws_client = aws.connect_to_client()
        gcs_client = gcs.connect_to_client()

        for root, dirs, files in os.walk(path_to_walk):
            for file in files:
                file_path = os.path.join(root, file)
                file_ext = os.path.splitext(file)[1].lower()
                file_ext = file_ext.replace(".", "")
                if all_to_aws:
                    aws.upload_to_aws_resource(file_path, aws_bucket_name, file, aws_client)
                elif all_to_gcs:
                    gcs.upload_to_gcs_resource(file_path, gcs_bucket_name, file, gcs_client)
                elif file_ext in self.media_list:
                    aws.upload_to_aws_resource(file_path, aws_bucket_name, file, aws_client)
                elif file_ext in self.document_list:
                    aws.upload_to_aws_resource(file_path, aws_bucket_name, file, aws_client)
                elif file_ext in self.image_list:
                    gcs.upload_to_gcs_resource(file_path, gcs_bucket_name, file, gcs_client)
                else:
                    print(f"Skipped {file_path}")
