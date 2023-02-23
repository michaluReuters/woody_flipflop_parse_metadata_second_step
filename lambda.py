import boto3
import json
import os
from aws_lambda_powertools import Logger
from domain.aws_actions.aws_actions import find_valid_s3_prefix_dict

logger = Logger()
appconfig = boto3.client('appconfig')
client = boto3.client('events')
LAMBDA_NAME = os.environ["AWS_LAMBDA_FUNCTION_NAME"]


@logger.inject_lambda_context(log_event=True)
def handler(event, context):
    dict_event = event['detail']
    name = dict_event['name']

    configuration_prefixes = appconfig.get_hosted_configuration_version(
        ApplicationId=os.environ.get('APP_CONFIG_APP_ID'),
        ConfigurationProfileId=os.environ.get('APP_CONFIG_PREFIXES_ID'),
        VersionNumber=int(os.environ.get('APP_CONFIG_PREFIXES_VERSION'))
    )['Content'].read().decode('utf-8')

    prefixes = json.loads(configuration_prefixes)

    try:
        valid_prefix_data_not_cleared = [find_valid_s3_prefix_dict(name, prefix) for prefix in prefixes]
        valid_prefix_data = [i for i in valid_prefix_data_not_cleared if i is not None]
        valid_prefix = valid_prefix_data[0]['prefix-name']
        valid_file_format = valid_prefix_data[0]['file-format']

        dict_event['prefix'] = valid_prefix

        response_path = valid_file_format.replace('.', '')

        data_str = json.dumps(dict_event)
        entry = {
            'Source': f'{LAMBDA_NAME}-{response_path}-complete',
            'Resources': [LAMBDA_NAME],
            'DetailType': 'metadata-step-complete',
            'Detail': data_str
        }
        logger.info(f"Completing step 2 with entry: {entry}")
        response = client.put_events(
            Entries=[entry, ]
        )
        logger.info(f"Step 2 completed. Response: {response}")
    except Exception as exc:
        logger.info(f"FILE NOT FOUND! exception is: {exc}")
