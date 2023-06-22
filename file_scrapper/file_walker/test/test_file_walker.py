import pytest

from file_scrapper.file_walker.walker import FileWalker, MultipleCloudExist


class TestFileWalker:
    def test_get_client(self, mocker):
        """Test the AWS and GCS client is connected """
        mocked_s3_client = mocker.patch('boto3.client')
        mocked_gcs_client = mocker.patch('google.cloud.storage.Client')
        walker = FileWalker()
        assert walker.aws_client is None
        assert walker.gcs_client is None
        walker._pre_run()
        assert walker.aws_client is not None
        assert walker.gcs_client is not None

    def test_multiple_cloud_exist_error(self):
        """Test Multiple cloud enabled"""
        with pytest.raises(MultipleCloudExist) as exception_info:
            walker = FileWalker().walk("", "", "", all_to_gcs=True, all_to_aws=True)
        error_message = "Can't upload all data to both cloud storage"

        assert str(exception_info.value) == error_message

    def test_files_to_walk(self, tmpdir, mocker):
        """Test the dir files uploaded to cloud or not"""
        # Create temporary directory structure with test files
        root_dir = tmpdir.mkdir('test_files')
        image_dir = root_dir.mkdir('images')
        media_dir = root_dir.mkdir('media')
        document_dir = root_dir.mkdir('documents')

        # Create test files in the respective directories
        image_file1 = image_dir.join('image1.jpg')
        image_file1.write('Content of image1.jpg')

        image_file2 = image_dir.join('image2.png')
        image_file2.write('Content of image2.png')

        media_file1 = media_dir.join('media1.mp4')
        media_file1.write('Content of media1.mp4')

        document_file1 = document_dir.join('document1.docx')
        document_file1.write('Content of document1.docx')
        mocked_s3_client = mocker.patch('boto3.client')
        mocked_gcs_client = mocker.patch('google.cloud.storage.Client')

        FileWalker().walk(str(root_dir), "GCS-BUCKET", "AWS-BUCKET")

        cred = {'aws_access_key_id': None, 'aws_secret_access_key': None}
        mocked_s3_client.assert_called_once_with('s3', **cred)
        mocked_gcs_client.assert_called_once()

