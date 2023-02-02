import os
import boto3 as boto3
from botocore.exceptions import ClientError


def find_valid_s3_prefix_dict(filename, prefix_dict):
    prefix_name = prefix_dict['prefix-name']
    configured_name = f"{prefix_name}/{filename}"
    if file_in_s3_bucket(configured_name):
        return prefix_dict
    else:
        pass


def file_in_s3_bucket(file_name_sns) -> bool:
    """
    Checks if specified file exists in s3 bucket

    :param:
        file_name_sns: file that needs to be checked

    :return:
        bool: status
    """
    s3_bucket = boto3.resource("s3")
    s3_bucket_name = os.environ.get("S3_BUCKET_NAME")
    try:
        s3_bucket.Object(s3_bucket_name, f"{file_name_sns}.xml").load()
    except ClientError:
        return False
    return True
