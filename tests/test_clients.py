from unittest import TestCase
from unittest.mock import patch

from electronbonder.client import ElectronBond

from src.clients import ArchivesSpaceClient, CartographerClient


class MockResponse(object):
    """Class used to mock HTTP responses"""

    def __init__(self, json_data, status_code, **kwargs):
        """Sets data, status code, and any other data passed in."""
        self.json_data = json_data
        self.status_code = status_code
        self.text = "v4.0.0"
        for k in kwargs:
            setattr(self, k, kwargs[k])

    def json(self):
        """Mocks the json method of an HTTP response"""
        return self.json_data

    def raise_for_status(self):
        pass


class ArchivesSpaceClientTests(TestCase):

    @patch('asnake.client.ASnakeClient.get')
    @patch('asnake.client.ASnakeClient.authorize')
    def test_get_updated_identifiers(self, mock_authorize, mock_get):
        get_response = {"results": ["1", "2", "3"]}
        mock_get.side_effect = [MockResponse({}, 200), MockResponse(get_response, 200)]
        client = ArchivesSpaceClient(
            baseurl="https://as.rockarch.org/api",
            username="admin",
            password="admin",
            repo="2")
        output = client.get_updated_identifiers("archival_object", 12345)
        self.assertEqual(output, get_response)
        mock_get.assert_called_with(
            '/repositories/2/archival_objects',
            params={'all_ids': True, 'modified_since': 12345})

    @patch('asnake.client.ASnakeClient.get')
    @patch('asnake.client.ASnakeClient.authorize')
    def test_get_deleted_identifiers(self, mock_authorize, mock_get):
        get_response = {
            "this_page": 1,
            "last_page": 1,
            "results": [
                "/repositories/2/archival_objects/1",
                "/repositories/2/resourdes/1",
                "/repositories/2/archival_objects/2"]}
        mock_get.side_effect = [MockResponse({}, 200), MockResponse(get_response, 200)]
        client = ArchivesSpaceClient(
            baseurl="https://as.rockarch.org/api",
            username="admin",
            password="admin",
            repo="2")
        output = client.get_deleted_identifiers("archival_object", 12345)
        self.assertEqual(output, ["/repositories/2/archival_objects/1", "/repositories/2/archival_objects/2"]
                         )  # should only return objects matching object type
        mock_get.assert_called_with('delete-feed', params={'modified_since': 12345, 'page_size': 100, 'page': 1})

    @patch('asnake.client.ASnakeClient.get')
    @patch('asnake.client.ASnakeClient.authorize')
    def test_resolve_identifiers(self, mock_authorize, mock_get):
        get_response = [{"uri": "/repositories/2/archival_objects/1"}, {"uri//repositories/2/archival_objects/2"}]
        mock_get.side_effect = [MockResponse({}, 200), MockResponse(get_response, 200)]
        client = ArchivesSpaceClient(
            baseurl="https://as.rockarch.org/api",
            username="admin",
            password="admin",
            repo="2")
        output = client.resolve_identifiers(["1", "2"], "archival_object")
        self.assertEqual(list(output), get_response)
        mock_get.assert_called_with(
            '/repositories/2/archival_objects',
            params={
                'id_set': ['1', '2'],
                'resolve': [
                    'ancestors',
                    'ancestors::linked_agents',
                    'instances::top_container',
                    'instances::digital_object',
                    'linked_agents',
                    'subjects']
            }
        )

    @patch('asnake.client.ASnakeClient.get')
    @patch('asnake.client.ASnakeClient.authorize')
    def test_get_endpoint(self, mock_authorize, mock_get):
        mock_get.side_effect = [MockResponse({}, 200)]
        client = ArchivesSpaceClient(
            baseurl="https://as.rockarch.org/api",
            username="admin",
            password="admin",
            repo="2")
        for object_type, expected_endpoint in [
                ('resource', '/repositories/2/resources'),
                ('archival_object', '/repositories/2/archival_objects'),
                ('subject', '/subjects'),
                ('agent_person', '/agents/people'),
                ('agent_corporate_entity', '/agents/corporate_entities'),
                ('agent_family', '/agents/families'),
                ('foo', None)]:
            output = client.get_endpoint(object_type)
            self.assertEqual(output, expected_endpoint)


class CartographerClientTests(TestCase):

    baseurl = "https://cartographer.rockarch.org"
    health_check_path = "/status"

    @patch('electronbonder.client.ElectronBond.get')
    def test_init(self, mock_get):
        mock_get.return_value.raise_for_status.return_value = None
        client = CartographerClient(baseurl=self.baseurl, health_check_path=self.health_check_path)
        self.assertIsInstance(client.client, ElectronBond)
        mock_get.assert_called_once_with(self.health_check_path)

        mock_get.return_value.raise_for_status.side_effect = Exception()
        with self.assertRaises(Exception) as err:
            CartographerClient(baseurl=self.baseurl, health_check_path=self.health_check_path)
        self.assertTrue(str(err.exception).startswith('Cartographer is not available'))

    @patch('electronbonder.client.ElectronBond.get')
    def test_get_updated_identifiers(self, mock_get):
        mock_get.return_value.raise_for_status.return_value = None
        client = CartographerClient(baseurl=self.baseurl, health_check_path=self.health_check_path)

        mock_get.return_value.json.return_value = {'results': [{'id': '1'}]}
        output = client.get_updated_identifiers('arrangement_map_component', 12345)
        self.assertEqual(output, ['/api/components/1/'])
        mock_get.assert_called_with('/api/components/', params={'modified_since': 12345})

    @patch('electronbonder.client.ElectronBond.get')
    def test_get_deleted_identifiers(self, mock_get):
        mock_get.return_value.raise_for_status.return_value = None
        client = CartographerClient(baseurl=self.baseurl, health_check_path=self.health_check_path)

        mock_get.return_value.json.return_value = {
            'results': [{'ref': '/api/components/1', 'archivesspace_uri': '/repositories/2/archival_objects/1'}]}
        output = client.get_deleted_identifiers('arrangement_map_component', 12345)
        self.assertEqual(output, ['/repositories/2/archival_objects/1'])
        mock_get.assert_called_with('/api/delete-feed/', params={'deleted_since': 12345})

    @patch('electronbonder.client.ElectronBond.get')
    def test_resolve_identifiers(self, mock_get):
        mock_get.return_value.raise_for_status.return_value = None
        client = CartographerClient(baseurl=self.baseurl, health_check_path=self.health_check_path)

        mock_get.return_value.json.return_value = {'uri': '/api/components/1'}
        output = client.resolve_identifiers(['/api/components/1'], 'arrangement_map_component')
        self.assertEqual(list(output), [{'uri': '/api/components/1'}])
        mock_get.assert_called_with('/api/components/1')
