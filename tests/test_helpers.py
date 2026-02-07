from src.helpers import (ancestors_published, get_es_id, list_chunks,
                         object_published, valid_finding_aid_status, valid_id0)


def test_ancestors_published():
    for obj, expected in [
            ({'has_unpublished_ancestor': True}, False),
            ({'has_unpublished_ancestor': False}, True),
            ({}, True)]:
        output = ancestors_published(obj)
        assert output == expected


def test_get_es_id():
    uri = "/repositories/2/archival_objects/894812"
    for obj in [{"uri": uri}, {"archivesspace_uri": uri}]:
        output = get_es_id(obj)
        assert output == '4hWUXYgS6WgbXvY38fP39i'


def test_list_chunks():
    initial_list = [1, 2, 3, 4, 5, 6, 7]
    output = list(list_chunks(initial_list, 3))
    assert output == [[1, 2, 3], [4, 5, 6], [7]]


def test_object_published():
    for obj, expected in [
            ({"publish": True}, True),
            ({"publish": False}, False),
            ({}, False)]:
        output = object_published(obj)
        assert output == expected


def test_valid_finding_aid_status():
    for obj, statuses, expected in [
            ({}, [], True),
            ({"jsonmodel_type": "agent"}, ["revised"], True),
            ({"jsonmodel_type": "resource", "finding_aid_status": "published"}, ["revised"], True),
            ({"jsonmodel_type": "resource", "finding_aid_status": "revised"}, ["revised"], False),
            ({"jsonmodel_type": "archival_object", "ancestors": [{"_resolved": {"finding_aid_status": "published"}}]}, ["revised"], True),
            ({"jsonmodel_type": "archival_object", "ancestors": [{"_resolved": {"finding_aid_status": "revised"}}]}, ["revised"], False)]:
        output = valid_finding_aid_status(obj, statuses)
        assert output == expected


def test_valid_id0():
    for obj, prefixes, expected in [
            ({}, [], True),
            ({}, ["FA"], True),
            ({"id_0": "FA001"}, ["FA"], True),
            ({"id_0": "FA001"}, ["MSS"], False)]:
        output = valid_id0(obj, prefixes)
        assert output == expected
