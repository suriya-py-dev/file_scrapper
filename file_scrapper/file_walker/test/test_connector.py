from file_scrapper.file_walker.connector import Connector


class TestConnector:
    """
    Test the connector, which is
     connects to s3 bucket and GCS
    """
    def test_collect_to_aws_client(self, mocker):
        """Test AWS is connected to client"""
        mocked_s3_client = mocker.patch('boto3.client')
        cntr = Connector()
        assert cntr.aws_client is None
        cntr.connect_to_aws_client()
        assert cntr.aws_client is not None

    def test_collect_to_gcs_client(self, mocker):
        """Test GCS is connected to its client"""
        mocked_gcs_client = mocker.patch('google.cloud.storage.Client')
        cntr = Connector()
        assert cntr.gcs_client is None
        cntr.connect_to_gcs_client()
        assert cntr.gcs_client is not None

    def test_upload_to_gcs_resource(self, tmpdir, mocker):
        """Test the GCS client is uploaded the file to cloud"""
        root_dir = tmpdir.mkdir('test_files')
        image_dir = root_dir.mkdir('images')
        image_file1 = image_dir.join('image1.jpg')
        image_file1.write('Content of image1.jpg')
        mocked_gcs_client = mocker.patch('google.cloud.storage.Client')
        cntr = Connector()
        cntr.connect_to_gcs_client()
        cntr.upload_to_gcs_resource(image_file1.dirname, "GCS-BUCKET")
        mocked_gcs_client.assert_called_once()

    def test_upload_to_aws_resource(self, tmpdir, mocker):
        """Test the AWS client is uploaded the file to cloud"""
        root_dir = tmpdir.mkdir('test_files')
        media_dir = root_dir.mkdir('media')
        media_file1 = media_dir.join('media1.mp4')
        media_file1.write('Content of media1.mp4')
        mocked_aws_client = mocker.patch('boto3.client')
        cntr = Connector()
        cntr.connect_to_aws_client()
        cntr.upload_to_aws_resource(media_file1.dirname, "AWS-BUCKET")
        mocked_aws_client.assert_called_once()