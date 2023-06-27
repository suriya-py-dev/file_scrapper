from file_scrapper.file_walker.connector import GCSConnector, AWSConnector


class TestConnector:
    """
    Test the connector, which is
     connects to s3 bucket and GCS
    """
    def test_collect_to_aws_client(self, mocker):
        """Test AWS is connected to client"""
        mocked_s3_client = mocker.patch('boto3.client')
        aws_client = AWSConnector().connect_to_client()
        assert aws_client is not None

    def test_collect_to_gcs_client(self, mocker):
        """Test GCS is connected to its client"""
        mocked_gcs_client = mocker.patch('google.cloud.storage.Client')
        gcs_client = GCSConnector().connect_to_client()
        assert gcs_client is not None

    def test_upload_to_gcs_resource(self, tmpdir, mocker):
        """Test the GCS client is uploaded the file to cloud"""
        root_dir = tmpdir.mkdir('test_files')
        image_dir = root_dir.mkdir('images')
        image_file1 = image_dir.join('image1.jpg')
        image_file1.write('Content of image1.jpg')
        mocked_gcs_client = mocker.patch('google.cloud.storage.Client')
        cntr = GCSConnector()
        client = cntr.connect_to_client()
        cntr.upload_to_resource(image_file1.dirname, "GCS-BUCKET", "client", client)
        mocked_gcs_client.assert_called_once()

    def test_upload_to_aws_resource(self, tmpdir, mocker):
        """Test the AWS client is uploaded the file to cloud"""
        root_dir = tmpdir.mkdir('test_files')
        media_dir = root_dir.mkdir('media')
        media_file1 = media_dir.join('media1.mp4')
        media_file1.write('Content of media1.mp4')
        mocked_aws_client = mocker.patch('boto3.client')
        cntr = AWSConnector()
        client = cntr.connect_to_client()
        cntr.upload_to_resource(media_file1.dirname, "AWS-BUCKET", "client", client)
        mocked_aws_client.assert_called_once()
