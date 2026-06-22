import shortuuid


def list_chunks(lst, n):
    """Yield successive n-sized chunks from list.
    Args:
        lst (list): list to chunkify
        n (integer): size of chunk to produce
    """
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


def get_es_id(obj):
    uri = obj.get('uri') if obj.get('uri') else obj.get('archivesspace_uri')
    return shortuuid.uuid(name=uri)


def object_published(obj):
    """Returns a boolean indicating whether the object is published."""
    return obj.get("publish", False)


def ancestors_published(obj):
    """Returns a boolean indicating whether the object has unpublished ancestors."""
    return not obj.get("has_unpublished_ancestor", False)


def valid_id0(obj, valid_prefixes=[]):
    if isinstance(valid_prefixes, str):
        valid_prefixes = valid_prefixes.split(",")
    """Returns a boolean indicating whether the object's id_0 field is in a configured list."""
    if len(valid_prefixes):
        if obj.get("id_0") and not any(
                [obj.get("id_0").startswith(prefix) for prefix in valid_prefixes]):
            return False
    return True


def valid_finding_aid_status(obj, restricted_statuses=[]):
    """
    Returns a boolean indicating whether the finding aid status for the object's
    resource is not in a list of configured restricted statuses.
    """
    if isinstance(restricted_statuses, str):
        restricted_statuses = restricted_statuses.split(",")
    if len(restricted_statuses) and obj.get("jsonmodel_type") in ["resource", "archival_object"]:
        resource = obj["ancestors"][-1]["_resolved"] if obj["jsonmodel_type"] == "archival_object" else obj
        if any([resource.get("finding_aid_status") == value for value in restricted_statuses]):
            return False
    return True
