import json
from unittest import TestCase
from unittest.mock import ANY, call, patch

import boto3
import pytest
from moto import mock_aws
from moto.core import DEFAULT_ACCOUNT_ID

from src.fetch_data import DataFetcher

DEFAULT_ARGS = [
    'dev',
    'us-east-1',
    f'arn:aws:iam::{DEFAULT_ACCOUNT_ID}:role/dynamodb-role',
    f'arn:aws:iam::{DEFAULT_ACCOUNT_ID}:role/sns-role',
    'sns-topic',
    f'arn:aws:iam::{DEFAULT_ACCOUNT_ID}:role/ssm-role',
    'archivesspace',
    'updated',
    'archival_object']
DEFAULT_CONFIG = {
    "AS_BASEURL": "https://as.rockarch.org/api",
    "AS_USERNAME": "admin",
    "AS_PASSWORD": "admin",
    "AS_REPO": "2",
    "CARTOGRAPHER_BASEURL": "https://cartographer.rockarch.org",
    "CARTOGRAPHER_HEALTH_CHECK_PATH": "/status",
    "DYNAMODB_TABLE": "fetches",
    "RESTRICTED_FINDING_AID_STATUSES": ["closed"],
    "VALID_ID0_PREFIXES": ["FA"],
}


class DataFetcherMethodTests(TestCase):

    @patch('src.fetch_data.DataFetcher.get_config')
    def setUp(self, mock_config):
        mock_config.return_value = DEFAULT_CONFIG
        self.fetcher = DataFetcher(*DEFAULT_ARGS)

    def set_up_dynamo(self):
        client = boto3.client('dynamodb', region_name=self.fetcher.aws_region)
        client.create_table(
            TableName=self.fetcher.config['DYNAMODB_TABLE'],
            AttributeDefinitions=[
                {
                    'AttributeName': 'ObjectStatus',
                    'AttributeType': 'S'
                },
                {
                    'AttributeName': 'ObjectType',
                    'AttributeType': 'S'
                },
            ],
            KeySchema=[
                {
                    'AttributeName': 'ObjectStatus',
                    'KeyType': 'RANGE',
                },
                {
                    'AttributeName': 'ObjectType',
                    'KeyType': 'HASH',
                },
            ],
            ProvisionedThroughput={
                'ReadCapacityUnits': 25,
                'WriteCapacityUnits': 25
            },
        )
        return client

    def set_up_sns(self):
        client = boto3.client('sns', region_name=self.fetcher.aws_region)
        topic_arn = client.create_topic(Name='test-topic.fifo', Attributes={'FifoTopic': 'true'})['TopicArn']
        self.fetcher.sns_topic = topic_arn
        sqs_conn = boto3.resource('sqs', region_name=self.fetcher.aws_region)
        sqs_conn.create_queue(QueueName="test-queue")
        client.subscribe(
            TopicArn=topic_arn,
            Protocol="sqs",
            Endpoint=f"arn:aws:sqs:us-east-1:{DEFAULT_ACCOUNT_ID}:test-queue",
        )
        queue = sqs_conn.get_queue_by_name(QueueName="test-queue")
        return queue

    def test_init(self):
        self.assertEqual(self.fetcher.service_name, 'data_fetch')
        self.assertEqual(self.fetcher.aws_region, 'us-east-1')
        self.assertEqual(self.fetcher.dynamodb_role_arn, f'arn:aws:iam::{DEFAULT_ACCOUNT_ID}:role/dynamodb-role')
        self.assertEqual(self.fetcher.sns_role_arn, f'arn:aws:iam::{DEFAULT_ACCOUNT_ID}:role/sns-role')
        self.assertEqual(self.fetcher.sns_topic, 'sns-topic')
        self.assertEqual(self.fetcher.ssm_role_arn, f'arn:aws:iam::{DEFAULT_ACCOUNT_ID}:role/ssm-role')
        self.assertEqual(self.fetcher.source_system, 'archivesspace')
        self.assertEqual(self.fetcher.object_status, 'updated')
        self.assertEqual(self.fetcher.object_type, 'archival_object')
        self.assertEqual(self.fetcher.config, DEFAULT_CONFIG)

        """Test valid object types"""
        invalid_object_type = DEFAULT_ARGS.copy()
        invalid_object_type[8] = 'foo'
        with pytest.raises(Exception) as err:
            DataFetcher(*invalid_object_type)
        self.assertTrue(str(err.value).startswith('Requested object type'))

        """Test valid object statuses"""
        invalid_object_status = DEFAULT_ARGS.copy()
        invalid_object_status[7] = 'bar'
        with pytest.raises(Exception) as err:
            DataFetcher(*invalid_object_status)
        self.assertTrue(str(err.value).startswith('Requested object status'))

    @patch('src.fetch_data.DataFetcher.is_running')
    @patch('src.fetch_data.DataFetcher.set_is_running')
    @patch('src.fetch_data.DataFetcher.get_last_run_time')
    @patch('src.fetch_data.DataFetcher.is_exportable')
    @patch('src.fetch_data.DataFetcher.send_data_to_sns')
    @patch('src.fetch_data.DataFetcher.send_delete_request')
    @patch('src.fetch_data.DataFetcher.send_success_message')
    @patch('src.fetch_data.DataFetcher.send_failure_message')
    @patch('src.fetch_data.DataFetcher.set_last_run_time')
    @patch('src.clients.ArchivesSpaceClient.__init__')
    @patch('src.clients.ArchivesSpaceClient.get_updated_identifiers')
    @patch('src.clients.ArchivesSpaceClient.get_deleted_identifiers')
    @patch('src.clients.ArchivesSpaceClient.resolve_identifiers')
    @patch('src.clients.CartographerClient.__init__')
    @patch('src.clients.CartographerClient.get_updated_identifiers')
    @patch('src.clients.CartographerClient.get_deleted_identifiers')
    @patch('src.clients.CartographerClient.resolve_identifiers')
    def test_fetch_updated(
            self,
            mock_cartographer_resolve,
            mock_cartographer_get_deleted,
            mock_cartographer_get_updated,
            mock_cartographer_init,
            mock_as_resolve,
            mock_as_get_deleted,
            mock_as_get_updated,
            mock_as_init,
            mock_set_last_run_time,
            mock_failure_message,
            mock_success_message,
            mock_delete_message,
            mock_data_to_sns,
            mock_is_exportable,
            mock_get_last_run_time,
            mock_set_is_running,
            mock_is_running):
        """Set up mocks"""
        fetched_obj = {"uri": "1234"}
        mock_as_init.return_value = None
        mock_as_get_updated.return_value = [fetched_obj]
        mock_as_resolve.return_value = [fetched_obj]
        mock_cartographer_init.return_value = None
        mock_cartographer_get_updated.return_value = [fetched_obj]
        mock_cartographer_resolve.return_value = [fetched_obj]
        mock_is_exportable.return_value = True
        mock_get_last_run_time.return_value = 12345

        """Fetcher is already running"""
        mock_is_running.return_value = True
        self.fetcher.fetch()
        mock_set_is_running.assert_not_called()
        mock_get_last_run_time.assert_not_called()
        mock_is_exportable.assert_not_called()
        mock_data_to_sns.assert_not_called()
        mock_delete_message.assert_not_called()
        mock_success_message.assert_not_called()
        mock_failure_message.assert_not_called()
        mock_set_last_run_time.assert_not_called()
        mock_as_init.assert_not_called()
        mock_as_get_updated.assert_not_called()
        mock_as_get_deleted.assert_not_called()
        mock_as_resolve.assert_not_called()
        mock_cartographer_init.assert_not_called()
        mock_cartographer_get_updated.assert_not_called()
        mock_cartographer_get_deleted.assert_not_called()
        mock_cartographer_resolve.assert_not_called()

        """Fetching from AS"""
        mock_is_running.return_value = False
        self.fetcher.fetch()
        mock_set_is_running.assert_has_calls(
            [call(self.fetcher.object_status, self.fetcher.object_type), call(self.fetcher.object_status, self.fetcher.object_type, status=False)])
        mock_get_last_run_time.assert_called_once_with(self.fetcher.object_status, self.fetcher.object_type)
        mock_is_exportable.assert_called_once_with(fetched_obj)
        mock_data_to_sns.assert_called_once_with(fetched_obj)
        mock_delete_message.assert_not_called()
        mock_success_message.assert_called_once_with()
        mock_failure_message.assert_not_called()
        mock_set_last_run_time.assert_called_once_with(self.fetcher.object_status, self.fetcher.object_type, ANY)
        mock_as_init.assert_called_once_with(
            baseurl=self.fetcher.config['AS_BASEURL'],
            username=self.fetcher.config['AS_USERNAME'],
            password=self.fetcher.config['AS_PASSWORD'],
            repo=self.fetcher.config['AS_REPO'])
        mock_as_get_updated.assert_called_once_with(self.fetcher.object_type, 12345)
        mock_as_get_deleted.assert_not_called()
        mock_as_resolve.assert_called_once()
        mock_cartographer_init.assert_not_called()
        mock_cartographer_get_updated.assert_not_called()
        mock_cartographer_get_deleted.assert_not_called()
        mock_cartographer_resolve.assert_not_called()

        """Reset mocks"""
        mock_set_is_running.reset_mock()
        mock_get_last_run_time.reset_mock()
        mock_is_exportable.reset_mock()
        mock_data_to_sns.reset_mock()
        mock_delete_message.reset_mock()
        mock_success_message.reset_mock()
        mock_failure_message.reset_mock()
        mock_set_last_run_time.reset_mock()
        mock_as_init.reset_mock()
        mock_as_get_updated.reset_mock()
        mock_as_resolve.reset_mock()

        """Fetching from Cartographer"""
        self.fetcher.source_system = 'cartographer'
        self.fetcher.fetch()
        mock_set_is_running.assert_has_calls(
            [call(self.fetcher.object_status, self.fetcher.object_type), call(self.fetcher.object_status, self.fetcher.object_type, status=False)])
        mock_get_last_run_time.assert_called_once_with(self.fetcher.object_status, self.fetcher.object_type)
        mock_is_exportable.assert_called_once_with(fetched_obj)
        mock_data_to_sns.assert_called_once_with(fetched_obj)
        mock_delete_message.assert_not_called()
        mock_success_message.assert_called_once_with()
        mock_failure_message.assert_not_called()
        mock_set_last_run_time.assert_called_once_with(self.fetcher.object_status, self.fetcher.object_type, ANY)
        mock_as_init.assert_not_called()
        mock_as_get_updated.assert_not_called()
        mock_as_get_deleted.assert_not_called()
        mock_as_resolve.assert_not_called()
        mock_cartographer_init.assert_called_once_with(
            baseurl=self.fetcher.config['CARTOGRAPHER_BASEURL'],
            health_check_path=self.fetcher.config['CARTOGRAPHER_HEALTH_CHECK_PATH'])
        mock_cartographer_get_updated.assert_called_once_with(self.fetcher.object_type, 12345)
        mock_cartographer_get_deleted.assert_not_called()
        mock_cartographer_resolve.assert_called_once()

        """Non exportable object"""
        mock_is_exportable.return_value = False
        self.fetcher.fetch()
        mock_delete_message.assert_called_once_with(fetched_obj)

    @patch('src.fetch_data.DataFetcher.is_running')
    @patch('src.fetch_data.DataFetcher.set_is_running')
    @patch('src.fetch_data.DataFetcher.get_last_run_time')
    @patch('src.fetch_data.DataFetcher.is_exportable')
    @patch('src.fetch_data.DataFetcher.send_data_to_sns')
    @patch('src.fetch_data.DataFetcher.send_delete_request')
    @patch('src.fetch_data.DataFetcher.send_success_message')
    @patch('src.fetch_data.DataFetcher.send_failure_message')
    @patch('src.fetch_data.DataFetcher.set_last_run_time')
    @patch('src.clients.ArchivesSpaceClient.__init__')
    @patch('src.clients.ArchivesSpaceClient.get_updated_identifiers')
    @patch('src.clients.ArchivesSpaceClient.get_deleted_identifiers')
    @patch('src.clients.ArchivesSpaceClient.resolve_identifiers')
    @patch('src.clients.CartographerClient.__init__')
    @patch('src.clients.CartographerClient.get_updated_identifiers')
    @patch('src.clients.CartographerClient.get_deleted_identifiers')
    @patch('src.clients.CartographerClient.resolve_identifiers')
    def test_fetch_deleted(
            self,
            mock_cartographer_resolve,
            mock_cartographer_get_deleted,
            mock_cartographer_get_updated,
            mock_cartographer_init,
            mock_as_resolve,
            mock_as_get_deleted,
            mock_as_get_updated,
            mock_as_init,
            mock_set_last_run_time,
            mock_failure_message,
            mock_success_message,
            mock_delete_message,
            mock_data_to_sns,
            mock_is_exportable,
            mock_get_last_run_time,
            mock_set_is_running,
            mock_is_running):
        """Set up mocks"""
        fetched_obj = {"uri": "1234"}
        mock_as_init.return_value = None
        mock_as_get_deleted.return_value = [fetched_obj]
        mock_as_resolve.return_value = [fetched_obj]
        mock_cartographer_init.return_value = None
        mock_cartographer_get_deleted.return_value = [fetched_obj]
        mock_cartographer_resolve.return_value = [fetched_obj]
        mock_is_exportable.return_value = True
        mock_get_last_run_time.return_value = 12345

        self.fetcher.object_status = 'deleted'

        """Fetching from AS"""
        mock_is_running.return_value = False
        self.fetcher.fetch()
        mock_set_is_running.assert_has_calls(
            [call(self.fetcher.object_status, self.fetcher.object_type), call(self.fetcher.object_status, self.fetcher.object_type, status=False)])
        mock_get_last_run_time.assert_called_once_with(self.fetcher.object_status, self.fetcher.object_type)
        mock_is_exportable.assert_not_called()
        mock_data_to_sns.assert_not_called()
        mock_delete_message.assert_called_once_with(fetched_obj)
        mock_success_message.assert_called_once_with()
        mock_failure_message.assert_not_called()
        mock_set_last_run_time.assert_called_once_with(self.fetcher.object_status, self.fetcher.object_type, ANY)
        mock_as_init.assert_called_once()
        mock_as_get_updated.assert_not_called()
        mock_as_get_deleted.assert_called_once()
        mock_as_resolve.assert_not_called()
        mock_cartographer_init.assert_not_called()
        mock_cartographer_get_updated.assert_not_called()
        mock_cartographer_get_deleted.assert_not_called()
        mock_cartographer_resolve.assert_not_called()

        """Reset mocks"""
        mock_set_is_running.reset_mock()
        mock_get_last_run_time.reset_mock()
        mock_delete_message.reset_mock()
        mock_success_message.reset_mock()
        mock_failure_message.reset_mock()
        mock_set_last_run_time.reset_mock()
        mock_as_init.reset_mock()
        mock_as_get_deleted.reset_mock()

        """Fetching from Cartographer"""
        self.fetcher.source_system = 'cartographer'
        self.fetcher.fetch()
        mock_set_is_running.assert_has_calls(
            [call(self.fetcher.object_status, self.fetcher.object_type), call(self.fetcher.object_status, self.fetcher.object_type, status=False)])
        mock_get_last_run_time.assert_called_once_with(self.fetcher.object_status, self.fetcher.object_type)
        mock_is_exportable.assert_not_called()
        mock_data_to_sns.assert_not_called()
        mock_delete_message.assert_called_once_with(fetched_obj)
        mock_success_message.assert_called_once_with()
        mock_failure_message.assert_not_called()
        mock_set_last_run_time.assert_called_once_with(self.fetcher.object_status, self.fetcher.object_type, ANY)
        mock_as_init.assert_not_called()
        mock_as_get_updated.assert_not_called()
        mock_as_get_deleted.assert_not_called()
        mock_as_resolve.assert_not_called()
        mock_cartographer_init.assert_called_once()
        mock_cartographer_get_updated.assert_not_called()
        mock_cartographer_get_deleted.assert_called_once()
        mock_cartographer_resolve.assert_not_called()

    @patch('src.fetch_data.DataFetcher.is_running')
    @patch('src.fetch_data.DataFetcher.set_is_running')
    @patch('src.fetch_data.DataFetcher.set_last_run_time')
    @patch('src.fetch_data.DataFetcher.get_last_run_time')
    @patch('src.fetch_data.DataFetcher.send_failure_message')
    def test_fetch_with_exception(
            self,
            mock_failure_message,
            mock_get_last_run_time,
            mock_set_last_run_time,
            mock_set_is_running,
            mock_is_running):
        mock_is_running.return_value = False
        mock_get_last_run_time.side_effect = Exception("foo")

        self.fetcher.fetch()

        mock_failure_message.assert_called_once()
        mock_set_is_running.assert_has_calls(
            [call(self.fetcher.object_status, self.fetcher.object_type), call(self.fetcher.object_status, self.fetcher.object_type, status=False)])
        mock_set_last_run_time.assert_not_called()

    @mock_aws
    def test_get_last_run_time(self):
        client = self.set_up_dynamo()
        output = self.fetcher.get_last_run_time(self.fetcher.object_status, self.fetcher.object_type)
        self.assertEqual(output, 0)

        client.put_item(
            TableName=self.fetcher.config['DYNAMODB_TABLE'],
            Item={
                'ObjectStatus': {'S': self.fetcher.object_status},
                'ObjectType': {'S': self.fetcher.object_type},
                'LastRunTime': {'N': '12345'}
            }
        )
        output = self.fetcher.get_last_run_time(self.fetcher.object_status, self.fetcher.object_type)
        self.assertEqual(output, 12345)

    @mock_aws
    def test_set_last_run_time(self):
        client = self.set_up_dynamo()
        self.fetcher.set_last_run_time(self.fetcher.object_status, self.fetcher.object_type, 12345)
        response = client.get_item(
            TableName=self.fetcher.config['DYNAMODB_TABLE'],
            Key={'ObjectStatus': {'S': self.fetcher.object_status},
                 'ObjectType': {'S': self.fetcher.object_type}})
        self.assertEqual(response['Item']['LastRunTime']['N'], '12345')

    @mock_aws
    def test_is_running(self):
        client = self.set_up_dynamo()
        output = self.fetcher.is_running(self.fetcher.object_status, self.fetcher.object_type)
        self.assertFalse(output)

        client.put_item(
            TableName=self.fetcher.config['DYNAMODB_TABLE'],
            Item={
                'ObjectStatus': {'S': self.fetcher.object_status},
                'ObjectType': {'S': self.fetcher.object_type},
                'IsRunning': {'BOOL': True}
            }
        )
        output = self.fetcher.is_running(self.fetcher.object_status, self.fetcher.object_type)
        self.assertTrue(output)

    @mock_aws
    def test_set_is_running(self):
        client = self.set_up_dynamo()
        self.fetcher.set_is_running(self.fetcher.object_status, self.fetcher.object_type, status=False)
        response = client.get_item(
            TableName=self.fetcher.config['DYNAMODB_TABLE'],
            Key={'ObjectStatus': {'S': self.fetcher.object_status},
                 'ObjectType': {'S': self.fetcher.object_type}})
        self.assertFalse(response['Item']['IsRunning']['BOOL'])

    @patch('src.fetch_data.object_published')
    @patch('src.fetch_data.ancestors_published')
    @patch('src.fetch_data.valid_id0')
    @patch('src.fetch_data.valid_finding_aid_status')
    def test_is_exportable(self, mock_fa_status, mock_id0, mock_ancestors, mock_published):
        self.fetcher.is_exportable({})
        mock_published.assert_called_once_with({})
        mock_ancestors.assert_called_once_with({})
        mock_id0.assert_called_once_with({}, DEFAULT_CONFIG['VALID_ID0_PREFIXES'])
        mock_fa_status.assert_called_once_with({}, DEFAULT_CONFIG['RESTRICTED_FINDING_AID_STATUSES'])

    @mock_aws
    def test_send_data_to_sns(self):
        queue = self.set_up_sns()
        self.fetcher.send_data_to_sns({"uri": "12345"})
        messages = queue.receive_messages(MaxNumberOfMessages=1)
        message_body = json.loads(messages[0].body)
        self.assertEqual(message_body['Message'], json.dumps({"uri": "12345"}))
        self.assertEqual(
            message_body['MessageAttributes'],
            {'service': {
                'Type': 'String',
                'Value': self.fetcher.service_name,
            },
                'requested_action': {
                'Type': 'String',
                'Value': 'merge',
            }})

    @mock_aws
    def test_send_delete_request(self):
        queue = self.set_up_sns()
        self.fetcher.send_delete_request({"uri": "12345"})
        messages = queue.receive_messages(MaxNumberOfMessages=1)
        message_body = json.loads(messages[0].body)
        self.assertEqual(message_body['Message'], '3aai9usY3AZzCSFkB3RSQ9')
        self.assertEqual(
            message_body['MessageAttributes'],
            {'service': {
                'Type': 'String',
                'Value': self.fetcher.service_name,
            },
                'requested_action': {
                'Type': 'String',
                'Value': 'delete',
            }})

    @mock_aws
    def test_send_success_message(self):
        queue = self.set_up_sns()
        self.fetcher.send_success_message()
        messages = queue.receive_messages(MaxNumberOfMessages=1)
        message_body = json.loads(messages[0].body)
        self.assertEqual(message_body['Message'], 'Fetch for updated archival_object completed successfully')
        self.assertEqual(
            message_body['MessageAttributes'],
            {'service': {
                'Type': 'String',
                'Value': self.fetcher.service_name,
            },
                'object_status': {
                'Type': 'String',
                'Value': 'updated',
            },
                'object_type': {
                'Type': 'String',
                'Value': 'archival_object',
            },
                'outcome': {
                'Type': 'String',
                'Value': 'SUCCESS',
            },
                'message': {
                'Type': 'String',
                'Value': 'Fetch for updated archival_object completed successfully',
            }})

    @mock_aws
    def test_send_failure_message(self):
        queue = self.set_up_sns()
        self.fetcher.send_failure_message(Exception('foo'))
        messages = queue.receive_messages(MaxNumberOfMessages=1)
        message_body = json.loads(messages[0].body)
        self.assertEqual(message_body['Message'], '')
        self.assertEqual(
            message_body['MessageAttributes'],
            {'service': {
                'Type': 'String',
                'Value': self.fetcher.service_name,
            },
                'object_status': {
                'Type': 'String',
                'Value': 'updated',
            },
                'object_type': {
                'Type': 'String',
                'Value': 'archival_object',
            },
                'outcome': {
                'Type': 'String',
                'Value': 'FAILURE',
            },
                'message': {
                'Type': 'String',
                'Value': 'foo',
            }})
