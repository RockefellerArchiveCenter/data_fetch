from asnake.aspace import ASpace
from electronbonder.client import ElectronBond
from requests.exceptions import ConnectTimeout

from .helpers import list_chunks

MAX_TIMEOUTS = 3


class ArchivesSpaceClient(object):
    """Fetches updated and deleted data from ArchivesSpace."""
    page_size = 25

    def __init__(self, baseurl=None, username=None, password=None, session_token=None, repo=None):
        for _ in range(MAX_TIMEOUTS):
            try:
                self.client = ASpace(
                    baseurl=baseurl,
                    username=username,
                    password=password,
                    session_token=session_token
                ).client
                self.repo = repo
                break
            except ConnectTimeout:
                pass

    def get_session_token(self):
        return self.client.session.headers.get(self.client.config['session_header_name'])

    def log_out(self):
        return self.client.post('/logout').json()

    def get_updated_identifiers(self, object_type, last_run):
        params = {"all_ids": True, "modified_since": last_run}
        endpoint = self.get_endpoint(object_type)
        return self.client.get(endpoint, params=params).json()

    def get_deleted_identifiers(self, object_type, last_run):
        data = []
        for d in self.client.get_paged(
                "delete-feed", params={"modified_since": last_run}):
            if self.get_endpoint(object_type) in d:
                data.append(d)
        return data

    def get_endpoint(self, object_type):
        repo_baseurl = f"/repositories/{self.repo}"
        endpoint = None
        if object_type == 'resource':
            endpoint = f"{repo_baseurl}/resources"
        elif object_type == 'archival_object':
            endpoint = f"{repo_baseurl}/archival_objects"
        elif object_type == 'subject':
            endpoint = "/subjects"
        elif object_type == 'agent_person':
            endpoint = "/agents/people"
        elif object_type == 'agent_corporate_entity':
            endpoint = "/agents/corporate_entities"
        elif object_type == 'agent_family':
            endpoint = "/agents/families"
        return endpoint

    def resolve_identifiers(self, fetched_ids, object_type):
        for id_list in list_chunks(fetched_ids, self.page_size):
            params = {
                "id_set": id_list,
                "resolve": ["ancestors", "ancestors::linked_agents", "instances::top_container", "instances::digital_object", "linked_agents", "subjects"]}
            page = self.client.get(self.get_endpoint(object_type), params=params).json()
            for obj in page:
                yield obj


class CartographerClient(object):
    """Fetches updated and deleted data from Cartographer."""
    base_endpoint = "/api/components/"

    def __init__(self, baseurl, health_check_path):
        client = ElectronBond(baseurl=baseurl)
        try:
            resp = client.get(health_check_path)
            resp.raise_for_status()
            self.client = client
        except Exception as e:
            raise Exception(
                f"Cartographer is not available: {e}")

    def get_updated_identifiers(self, object_type, last_run):
        data = []
        for obj in self.client.get(
                self.base_endpoint, params={"modified_since": last_run}).json()['results']:
            data.append(f"{self.base_endpoint}{obj.get('id')}/")
        return data

    def get_deleted_identifiers(self, object_type, last_run):
        data = []
        for deleted_ref in self.client.get(
                '/api/delete-feed/', params={"deleted_since": last_run}).json()['results']:
            if self.base_endpoint in deleted_ref['ref']:
                data.append(deleted_ref.get('archivesspace_uri'))
        return data

    def resolve_identifiers(self, fetched_ids, object_type):
        for obj_ref in fetched_ids:
            resp = self.client.get(obj_ref)
            resp.raise_for_status()
            yield resp.json()
