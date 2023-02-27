import os
import boto3 as boto3
from botocore.exceptions import ClientError
from aws_lambda_powertools import Logger

logger = Logger()
s3_bucket = boto3.resource("s3")
s3_bucket_name = os.environ.get("S3_BUCKET_NAME")
appconfig = boto3.client('appconfigdata')


def find_valid_s3_prefix_dict(filename, prefix_dict):
    """
    Searches for a file with the given `filename` in an S3 bucket, using a prefix defined in `prefix_dict`.

    :param:
        filename: The name of the file to search for in S3.
        prefix_dict: A dictionary containing the S3 prefix to use for the search. The prefix must include the
            directory name and the file format, with placeholders for the filename and file extension, respectively.

    :return:
        If a file with the specified name is found in the S3 bucket, returns the original `prefix_dict`. Otherwise,
        returns `None`.
    """
    logger.info(f"looking for a file: {filename} with prefix: {prefix_dict}")
    prefix_name = prefix_dict['prefix-name']
    prefix_extension = prefix_dict['file-format']
    configured_name = f"{prefix_name}/{filename}{prefix_extension}"
    if file_in_s3_bucket(configured_name):
        return prefix_dict
    else:
        pass


def file_in_s3_bucket(file_name_sns, bucket=s3_bucket, bucket_name=s3_bucket_name) -> bool:
    """
    Checks if specified file exists in s3 bucket

    :param:
        file_name_sns: file that needs to be checked

    :return:
        bool: status
    """

    logger.info(f"Searching for {file_name_sns} in {bucket_name}")
    try:
        bucket.Object(bucket_name, f"{file_name_sns}").load()
    except ClientError:
        logger.error(f"Could not find File {file_name_sns}")
        return False
    logger.info(f"File has been found: {file_name_sns}")
    return True


def get_latest_configuration():
    """
    This function gathers latest configuration in AWS App config

    :return:
        decoded configuration
    """

    token = appconfig.start_configuration_session(
        ApplicationIdentifier=os.environ.get('APP_CONFIG_APP_ID'),
        EnvironmentIdentifier=os.environ.get('APP_ENVIRONMENT'),
        ConfigurationProfileIdentifier=os.environ.get('APP_CONFIG_PREFIXES_ID'),
        RequiredMinimumPollIntervalInSeconds=20
    )['InitialConfigurationToken']

    response = appconfig.get_latest_configuration(
        ConfigurationToken=token
    )
    return response['Configuration'].read().decode('utf-8')
