import json
import requests

URL = 'http://localhost:8000'

def run_graphql(query, variables):
    r = requests.post(f'{URL}/graphql', json={ 'query': query, 'variables': variables })
    if r.status_code >= 500:
        raise requests.HTTPError(r.text)
    r.raise_for_status()
    return r.json()

def post_json(sid, data):
    r = requests.post(f'{URL}/data/{sid}', files=[('entries', (sid, json.dumps(data)))])
    if r.status_code >= 500:
        raise requests.HTTPError(r.text)
    r.raise_for_status()
    return r.json()

def get_json(sid):
    r = requests.get(f'{URL}/data/{sid}', params={'latest': True, 'last_entry_id': '-'})
    if r.status_code >= 500:
        raise requests.HTTPError(r.text)
    r.raise_for_status()
    return r.json()

def test_streams_lifecycle():

    assert run_graphql('''
mutation Flush {
  flush
}
    ''', {}) == {   "data": {     "flush": 1   } }

    assert run_graphql('''
query GetStreams {
    streams {
        id
        firstEntryId
        lastEntryId
    }
}
    ''', {}) == { "data": { "streams": [] } }
    
    assert run_graphql('''
mutation SetMeta {
  updateStreamMeta(streamId: "asdf", meta: {x: 1, y: 3})
}
    ''', {}) == {   "data": {     "updateStreamMeta": {       "meta_set": True     }   } }

    assert run_graphql('''
query GetStreams {
    streams {
        id
        firstEntryId
        lastEntryId
        meta
    }
}
    ''', {}) == { "data": { "streams": [ { "id": "asdf", "firstEntryId": "", "lastEntryId": "", "meta": {           "x": 1,           "y": 3         } } ] } }
    

    assert run_graphql('''
mutation SetMeta {
  updateStreamMeta(streamId: "asdf", meta: {x: 1, z: 3})
}
    ''', {}) == {   "data": {     "updateStreamMeta": {       "meta_set": True     }   } }

    assert run_graphql('''
query GetStreams {
    streams {
        id
        meta
    }
}
    ''', {}) == { "data": { "streams": [ { "id": "asdf", "meta": {           "x": 1,           "z": 3         } } ] } }
    

    post_json('asdf', {"x": 5})
    post_json('asdf', {"x": 6})
    data = get_json('asdf')
    assert data == {"x": 6}

    assert run_graphql('''
query GetStreams {
    streams {
        id
        length
        firstEntryJson
        lastEntryJson
    }
}
    ''', {}) == { "data": { "streams": [ { "id": "asdf", "length": 2, "firstEntryJson": {"x": 5}, "lastEntryJson": {"x": 6} } ] } }

    assert run_graphql('''
mutation DeleteStream {
  deleteStream(streamId: "asdf")
}
    ''', {}) == {   "data": {     "deleteStream": {       "data_deleted": True,       "meta_deleted": True, 'stream_deleted': True    }   } }

    assert run_graphql('''
query GetStreams {
    streams {
        id
        firstEntryId
        lastEntryId
    }
}
    ''', {}) == { "data": { "streams": [] } }


