import json
import logging
import time
import traceback
from os import getenv

import boto3
from aws_assume_role_lib import assume_role

from .clients import ArchivesSpaceClient, CartographerClient
from .helpers import (ancestors_published, get_es_id, object_published,
                      valid_finding_aid_status, valid_id0)

logger = logging.getLogger()
logger.setLevel(getenv("LOGGING_LEVEL", logging.INFO))

VALID_OBJECT_STATUSES = ['updated', 'deleted']
VALID_OBJECT_TYPES = [
    'resource',
    'archival_object',
    'subject',
    'agent_person',
    'agent_corporate_entity',
    'agent_family',
    'arrangement_map_component']


class DataFetcher:
    """Fetches data."""

    def __init__(self, environment,
                 aws_region,
                 dynamodb_role_arn,
                 sns_role_arn,
                 sns_topic,
                 ssm_role_arn,
                 source_system,
                 object_status,
                 object_type):
        self.service_name = 'data_fetch'
        self.aws_region = aws_region
        self.dynamodb_role_arn = dynamodb_role_arn
        self.sns_role_arn = sns_role_arn
        self.sns_topic = sns_topic
        self.ssm_role_arn = ssm_role_arn
        self.source_system = source_system
        self.object_status = object_status
        self.object_type = object_type
        self.environment = environment
        if object_status not in VALID_OBJECT_STATUSES:
            raise Exception(f'Requested object status {object_status} is not one of {" ".join(VALID_OBJECT_STATUSES)}')
        if object_type not in VALID_OBJECT_TYPES:
            raise Exception(f'Requested object type {object_type} is not one of {" ".join(VALID_OBJECT_TYPES)}.')
        self.config = self.get_config()
        self.session_token_key = f"AS_SESSION_TOKEN_{object_type.upper()}_{object_status.upper()}"

    def fetch(self):
        """Main method, which calls all other methods."""
        if not self.is_running(self.object_status, self.object_type):
            logging.info(f"Fetching {self.object_status} {self.object_type} from {self.source_system}.")
            try:
                start_time = int(time.time())
                self.set_is_running(self.object_status, self.object_type)
                last_run = self.get_last_run_time(self.object_status, self.object_type)

                if self.config.get(self.session_token_key):
                    previous_client = ArchivesSpaceClient(
                        baseurl=self.config['AS_BASEURL'],
                        session_token=self.config[self.session_token_key],
                        repo=self.config['AS_REPO'])
                    previous_client.log_out()

                client = ArchivesSpaceClient(
                    baseurl=self.config['AS_BASEURL'],
                    username=self.config['AS_USERNAME'],
                    password=self.config['AS_PASSWORD'],
                    repo=self.config['AS_REPO'])
                self.update_session_token(client.get_session_token())

                if self.source_system == 'cartographer':
                    client = CartographerClient(
                        baseurl=self.config['CARTOGRAPHER_BASEURL'],
                        health_check_path=self.config['CARTOGRAPHER_HEALTH_CHECK_PATH'])

                if self.object_status == 'updated':
                    fetched_ids = client.get_updated_identifiers(self.object_type, last_run)
                    for obj in client.resolve_identifiers(fetched_ids, self.object_type):
                        uri = obj.get('uri') if obj.get('uri') else obj.get('archivesspace_uri')
                        if self.is_exportable(obj):
                            self.send_data_to_sns(obj)
                            logging.debug(f"Sent updated data to merger for {self.object_type} {uri}")
                        else:
                            self.send_delete_request(obj)
                            logging.debug(f"Sent delete request for {self.object_type} {uri}")
                else:
                    fetched_ids = client.get_deleted_identifiers(self.object_type, last_run)
                    for to_delete in fetched_ids:
                        self.send_delete_request({"uri": to_delete})
                self.send_success_message()
                self.set_last_run_time(self.object_status, self.object_type, start_time)
            except Exception as e:
                logging.error(e)
                self.send_failure_message(e)
            self.set_is_running(self.object_status, self.object_type, status=False)
            logging.info(f"Fetch of {self.object_status} {self.object_type} is complete.")
        else:
            logging.info(f"Fetch for {self.object_status} {self.object_type} is already running.")

    def get_client_with_role(self, resource, role_arn):
        """Gets Boto3 client which authenticates with a specific IAM role."""
        session = boto3.Session(region_name=self.aws_region)
        assumed_role_session = assume_role(session, role_arn)
        return assumed_role_session.client(resource)

    def get_config(self):
        """Fetch config values from Parameter Store.

        Args:
            ssm_parameter_path (str): Path to parameters

        Returns:
            configuration (dict): all parameters found at the supplied path.
        """
        ssm_parameter_path = f"/{self.environment}/{self.service_name}"
        configuration = {}
        ssm_client = self.get_client_with_role('ssm', getenv('SSM_ROLE_ARN'))
        try:
            paginator = ssm_client.get_paginator('get_parameters_by_path')
            response_iterator = paginator.paginate(Path=ssm_parameter_path)
            for page in response_iterator:
                for entry in page['Parameters']:
                    param_path_array = entry.get('Name').split("/")
                    section_position = len(param_path_array) - 1
                    section_name = param_path_array[section_position]
                    configuration[section_name] = entry.get('Value')
        except BaseException:
            logging.error("Encountered an error loading config from SSM.")
            traceback.print_exc()
        finally:
            return configuration

    def update_session_token(self, session_token):
        ssm_client = self.get_client_with_role('ssm', self.ssm_role_arn)
        ssm_client.put_parameter(
            Name=f"/{self.environment}/{self.service_name}/{self.session_token_key}",
            Value=session_token,
            Type="String",
            Overwrite=True)

    def get_last_run_time(self, object_status, object_type):
        """Fetches last run time."""
        client = self.get_client_with_role('dynamodb', self.dynamodb_role_arn)
        try:
            response = client.get_item(
                TableName=self.config['DYNAMODB_TABLE'],
                Key={'ObjectStatus': {'S': object_status},
                     'ObjectType': {'S': object_type}})
            return int(response['Item']['LastRunTime']['N'])
        except KeyError:  # No matching item found
            return 0

    def set_last_run_time(self, object_status, object_type, timestamp):
        """Sets last run time."""
        client = self.get_client_with_role('dynamodb', self.dynamodb_role_arn)
        client.update_item(
            TableName=self.config['DYNAMODB_TABLE'],
            Key={'ObjectStatus': {'S': object_status},
                 'ObjectType': {'S': object_type}},
            UpdateExpression="set #r = :t",
            ExpressionAttributeNames={
                '#r': 'LastRunTime'
            },
            ExpressionAttributeValues={
                ':t': {'N': str(timestamp)}
            })

    def is_running(self, object_status, object_type):
        """Determines if an export job for a given object status and type is already running."""
        client = self.get_client_with_role('dynamodb', self.dynamodb_role_arn)
        try:
            response = client.get_item(
                TableName=self.config['DYNAMODB_TABLE'],
                Key={'ObjectStatus': {'S': object_status},
                     'ObjectType': {'S': object_type}})
            return response['Item']['IsRunning']['BOOL']
        except KeyError:  # No matching item found
            return False

    def set_is_running(self, object_status, object_type, status=True):
        """Set the status of a fetch for a given object status and type."""
        client = self.get_client_with_role('dynamodb', self.dynamodb_role_arn)
        client.update_item(
            TableName=self.config['DYNAMODB_TABLE'],
            Key={'ObjectStatus': {'S': object_status},
                 'ObjectType': {'S': object_type}},
            UpdateExpression="set #i = :s",
            ExpressionAttributeNames={
                '#i': 'IsRunning'
            },
            ExpressionAttributeValues={
                ':s': {'BOOL': status}
            })

    def is_exportable(self, obj):
        """Determines whether the object can be exported.

        All methods called here should explicitly return True if the object can
        be exported or False if it cannot be exported.
        """
        return bool(all([
            object_published(obj),
            ancestors_published(obj),
            valid_id0(obj, self.config['VALID_ID0_PREFIXES']),
            valid_finding_aid_status(obj, self.config['RESTRICTED_FINDING_AID_STATUSES'])
        ]))

    def send_data_to_sns(self, data):
        """Sends fetched data to SNS topic."""
        client = self.get_client_with_role('sns', self.sns_role_arn)
        es_id = get_es_id(data)
        client.publish(
            TopicArn=self.sns_topic,
            MessageGroupId=f'{self.service_name}-{es_id}',
            MessageDeduplicationId=f'{self.service_name}-{es_id}-success',
            Message=json.dumps(data, default=str),
            MessageAttributes={
                'service': {
                    'DataType': 'String',
                    'StringValue': self.service_name,
                },
                'requested_action': {
                    'DataType': 'String',
                    'StringValue': 'merge',
                },
                'object_type': {
                    'DataType': 'String',
                    'StringValue': self.object_type,
                },
                'session_token_key': {
                    'DataType': 'String',
                    'StringValue': self.session_token_key,
                }
            })

    def send_delete_request(self, data):
        """Sends delete request to SNS topic."""
        client = self.get_client_with_role('sns', self.sns_role_arn)
        es_id = get_es_id(data)

        client.publish(
            TopicArn=self.sns_topic,
            MessageGroupId=f'{self.service_name}-{es_id}',
            MessageDeduplicationId=f'{self.service_name}-{es_id}-delete',
            Message=es_id,
            MessageAttributes={
                'service': {
                    'DataType': 'String',
                    'StringValue': self.service_name,
                },
                'requested_action': {
                    'DataType': 'String',
                    'StringValue': 'delete',
                },
                'es_id': {
                    'DataType': 'String',
                    'StringValue': es_id,
                },
                'object_type': {
                    'DataType': 'String',
                    'StringValue': self.object_type,
                }
            })

    def send_success_message(self):
        """Sends a message to an SNS topic when a fetch completes successfully."""
        client = self.get_client_with_role('sns', self.sns_role_arn)
        client.publish(
            TopicArn=self.sns_topic,
            MessageGroupId=f'{self.service_name}-{self.object_status}-{self.object_type}',
            MessageDeduplicationId=f'{self.service_name}-{self.object_status}-{self.object_type}-success',
            Message=f'Fetch for {self.object_status} {self.object_type} completed successfully',
            MessageAttributes={
                'service': {
                    'DataType': 'String',
                    'StringValue': self.service_name,
                },
                'object_status': {
                    'DataType': 'String',
                    'StringValue': self.object_status,
                },
                'object_type': {
                    'DataType': 'String',
                    'StringValue': self.object_type,
                },
                'outcome': {
                    'DataType': 'String',
                    'StringValue': 'SUCCESS',
                },
                'message': {
                    'DataType': 'String',
                    'StringValue': f'Fetch for {self.object_status} {self.object_type} completed successfully'
                }
            })

    def send_failure_message(self, exception):
        """Sends an error message to an SNS topic."""
        client = self.get_client_with_role('sns', self.sns_role_arn)
        tb = ''.join(traceback.format_exception(exception)[:-1])
        client.publish(
            TopicArn=self.sns_topic,
            MessageGroupId=f'{self.service_name}-{self.object_type}-{self.object_status}',
            MessageDeduplicationId=f'{self.service_name}-{self.object_type}-{self.object_status}-failure',
            Message=tb,
            MessageAttributes={
                'service': {
                    'DataType': 'String',
                    'StringValue': self.service_name,
                },
                'object_status': {
                    'DataType': 'String',
                    'StringValue': self.object_status,
                },
                'object_type': {
                    'DataType': 'String',
                    'StringValue': self.object_type,
                },
                'outcome': {
                    'DataType': 'String',
                    'StringValue': 'FAILURE',
                },
                'message': {
                    'DataType': 'String',
                    'StringValue': str(exception),
                }
            })


if __name__ == '__main__':
    environment = getenv('ENVIRONMENT')
    dynamodb_role_arn = getenv('DYNAMODB_ROLE_ARN')
    aws_region = getenv('AWS_REGION')
    sns_role_arn = getenv('SNS_ROLE_ARN')
    sns_topic = getenv('SNS_TOPIC')
    ssm_role_arn = getenv('SSM_ROLE_ARN')
    source_system = getenv('SOURCE_SYSTEM')
    object_status = getenv('OBJECT_STATUS')
    object_type = getenv('OBJECT_TYPE')
    DataFetcher(
        environment,
        aws_region,
        dynamodb_role_arn,
        sns_role_arn,
        sns_topic,
        ssm_role_arn,
        source_system,
        object_status,
        object_type).fetch()
