import unittest
import boto3
from botocore.exceptions import ClientError
from unittest.mock import MagicMock, patch

from domain.aws_actions.aws_actions import file_in_s3_bucket, find_valid_s3_prefix_dict


class TestFileInS3Bucket(unittest.TestCase):
    @patch.object(boto3, "resource", return_value=MagicMock())
    def test_file_exists(self, mock_boto3_resource):
        file_name_sns = "example_file.txt"
        s3_bucket = mock_boto3_resource.return_value
        s3_bucket_object = MagicMock()
        s3_bucket.Object.return_value = s3_bucket_object
        bucket_name = "test_bucket"

        result = file_in_s3_bucket(file_name_sns, bucket=s3_bucket, bucket_name=bucket_name)
        self.assertTrue(result)
        s3_bucket.Object.assert_called_with(bucket_name, file_name_sns)
        s3_bucket_object.load.assert_called_once()

    @patch.object(boto3, "resource", return_value=MagicMock())
    def test_file_does_not_exist(self, mock_boto3_resource):
        file_name_sns = "non_existent_file.txt"
        s3_bucket = mock_boto3_resource.return_value
        s3_bucket_object = MagicMock()
        s3_bucket.Object.return_value = s3_bucket_object
        s3_bucket_object.load.side_effect = ClientError({"Error": {"Code": "404"}}, "load_object")
        bucket_name = "test_bucket"

        result = file_in_s3_bucket(file_name_sns, bucket=s3_bucket, bucket_name=bucket_name)
        self.assertFalse(result)
        s3_bucket.Object.assert_called_with(bucket_name, file_name_sns)
        s3_bucket_object.load.assert_called_once()


class TestS3Prefix(unittest.TestCase):

    @patch('domain.aws_actions.aws_actions.file_in_s3_bucket')
    def test_valid_s3_prefix(self, mock_file_in_s3_bucket):
        mock_file_in_s3_bucket.return_value = True
        prefix_dict = {'prefix-name': 'my-prefix', 'file-format': '.txt'}
        result = find_valid_s3_prefix_dict('file.txt', prefix_dict)
        self.assertEqual(result, prefix_dict)

    @patch('domain.aws_actions.aws_actions.file_in_s3_bucket')
    def test_invalid_s3_prefix(self, mock_file_in_s3_bucket):
        mock_file_in_s3_bucket.return_value = False
        prefix_dict = {'prefix-name': 'my-prefix', 'file-format': '.txt'}
        result = find_valid_s3_prefix_dict('file.txt', prefix_dict)
        self.assertIsNone(result)


if __name__ == '__main__':
    unittest.main()
