import boto3
import json
import os
from aws_lambda_powertools import Logger
from domain.aws_actions.aws_actions import find_valid_s3_prefix_dict

logger = Logger()
appconfig = boto3.client('appconfig')
client = boto3.client('events')


@logger.inject_lambda_context(log_event=True)
def handler(event, context):
    dict_event = event['message']['detail']
    name = dict_event['file_name']

    configuration_prefixes = appconfig.get_hosted_configuration_version(
        ApplicationId=os.environ.get('APP_CONFIG_APP_ID'),
        ConfigurationProfileId=os.environ.get('APP_CONFIG_PREFIXES_ID'),
        VersionNumber=int(os.environ.get('APP_CONFIG_PREFIXES_VERSION'))
    )['Content'].read().decode('utf-8')

    prefixes = json.loads(configuration_prefixes)

    try:
        valid_prefix_data = [find_valid_s3_prefix_dict(name, prefix) for prefix in prefixes]
        valid_prefix = valid_prefix_data[0]['prefix-name']
        valid_file_format = valid_prefix_data[0]['file-format']
        configured_file_name = f'{valid_prefix}/{name}{valid_file_format}'

        dict_event['metadata_path'] = configured_file_name

        response_path = valid_file_format.replace('.', '')

        data_str = json.dumps(dict_event)
        entry = {
            'Source': f'new-ppe-sonyhivemetadata-step2-{response_path}-complete',
            'Resources': ['new-ppe-sh-sonyhive-metadata-import-step2-lambda'],
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
