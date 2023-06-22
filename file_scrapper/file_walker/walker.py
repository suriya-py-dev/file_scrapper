"""
Code to load all the files in the folder
 and upload to cloud storage
"""
import os

from file_scrapper.file_walker.connector import Connector


class MultipleCloudExist(Exception):
    def __init__(self, message):
        self.message = message


class BucketNameNotFound(Exception):
    def __init__(self, name):
        self.message = f"{name} is Missed"


class FileWalker(Connector):
    """
    Code to load all the files in the folder
    and upload to cloud storage
    """
    media_list = ["mp3", "mp4", "mpeg4", "wmv", "3gp", "webm"]
    document_list = ["doc", "docx", "csv", "pd"]
    image_list = ["jpg", "png", "svg", "webp"]

    def _pre_run(self):
        self.connect_to_aws_client()
        self.connect_to_gcs_client()

    def walk(self, path_to_walk: str, gcs_bucket_name=None,
             aws_bucket_name=None, all_to_gcs=False,
             all_to_aws=False):
        """
        Walk over all the folders and load the files to cloud storages
        """
        if all_to_aws and all_to_gcs:
            message = "Can't upload all data to both cloud storage"
            raise MultipleCloudExist(message)

        if all_to_aws and aws_bucket_name is None:
            name = "AWS Bucket Name"
            raise BucketNameNotFound(name)

        if all_to_gcs and gcs_bucket_name is None:
            name = "GCS Bucket Name"
            raise BucketNameNotFound(name)

        self._pre_run()

        for root, dirs, files in os.walk(path_to_walk):
            for file in files:
                file_path = os.path.join(root, file)
                file_ext = os.path.splitext(file)[1].lower()
                file_ext = file_ext.replace(".", "")
                if all_to_aws:
                    self.upload_to_aws_resource(file_path, aws_bucket_name)
                elif all_to_gcs:
                    self.upload_to_gcs_resource(file_path, gcs_bucket_name)
                elif file_ext in self.media_list:
                    self.upload_to_aws_resource(file_path, aws_bucket_name)
                elif file_ext in self.document_list:
                    self.upload_to_aws_resource(file_path, aws_bucket_name)
                elif file_ext in self.image_list:
                    self.upload_to_gcs_resource(file_path, gcs_bucket_name)
                else:
                    print(f"Skipped {file_path}")
