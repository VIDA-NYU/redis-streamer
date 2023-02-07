import json
import requests

URL = 'http://localhost:8000'

def run_graphql(query, variables):
    r = requests.post(f'{URL}/graphql', json={ 'query': query, 'variables': variables })
    if r.status_code >= 500:
        raise requests.HTTPError(r.text)
    r.raise_for_status()
    return r.json()

def post_json(sid, data, **kw):
    r = requests.post(f'{URL}/data/{sid}', params=kw, files=[('entries', (sid, json.dumps(data)))])
    if r.status_code >= 500:
        raise requests.HTTPError(r.text)
    r.raise_for_status()
    return r.json()

def get_json(sid, **kw):
    r = requests.get(f'{URL}/data/{sid}', params={'latest': True, 'last_entry_id': '-', **kw})
    if r.status_code >= 500:
        raise requests.HTTPError(r.text)
    r.raise_for_status()
    return r.json()

def test_device_lifecycle():
    
    assert run_graphql('''
mutation Flush {
  flush
}
    ''', {}) == {   "data": {     "flush": 1   } }

    assert run_graphql('''
query GetDevices {
    devices {
        id
        streams {
            id
        }
    }
}
    ''', {}) == { "data": { "devices": [] } }
    
    assert run_graphql('''
mutation ConnectDevice {
  connectDevice(deviceId: "asdf", meta: {x: 1})
}
    ''', {}) == {   "data": {     "connectDevice": {       "connection_status_changed": True,       "connected": True,       "is_new_device": True,       "meta_set": True,       "fired:device.connected": True,       "fired:device.meta": True     }   } }

    assert run_graphql('''
mutation ConnectDevice {
  connectDevice(deviceId: "asdf", meta: {x: 1})
}
    ''', {}) == {   "data": {     "connectDevice": {       "connection_status_changed": False,       "connected": True,       "is_new_device": False,       "meta_set": True,       "fired:device.connected": True,       "fired:device.meta": True     }   } }

    assert run_graphql('''
query GetDevices {
  devices {
    id
    streams {
      id
    }
  }
}
    ''', {}) == {   "data": {     "devices": [       {         "id": "asdf",         "streams": []       }     ]   } }

    assert run_graphql('''
mutation SetMeta {
  updateStreamMeta(streamId: "zzzz", meta: {x: 1, z: 3}, deviceId: "asdf")
}
    ''', {}) == {'data': {'updateStreamMeta': {'meta_set': True}}}

    assert run_graphql('''
query GetDevices {
    devices {
    id meta
    streams {
      id meta
    }
  }
}
    ''', {}) == {'data': {'devices': [{'id': 'asdf', 'meta': {"x": 1}, 'streams': [{'id': 'zzzz', 'meta': {"x": 1, "z": 3}}]}]}}



    post_json('zxcv', {"x": 2}, device_id='asdf')
    post_json('zxcv', {"x": 3}, device_id='asdf')
    data = get_json('zxcv', device_id='asdf')
    assert data == {"x": 3}

    post_json('xxxx', {"x": 5})
    post_json('xxxx', {"x": 6})
    data = get_json('xxxx')
    assert data == {"x": 6}

    assert run_graphql('''
query GetEverythingDevices {
  devices {
    streamIds
    id
    meta
    streams {
      id
      length
      entriesAdded
      error
      firstEntryData
    #   firstEntryId
      firstEntryJson
      firstEntryString
    #   firstEntryTime
      groups
      lastEntryData
    #   lastEntryId
      lastEntryJson
      lastEntryString
    #   lastEntryTime
    #   lastGeneratedId
    #   maxDeletedEntryId
      meta
      radixTreeKeys
      radixTreeNodes
    #   recordedFirstEntryId
    }
  }
}
    ''', {}) == {'data': {'devices': [{'streamIds': ['zxcv'], 'id': 'asdf', 'meta': {'x': 1}, 'streams': [
        {'id': 'zxcv', 'length': 2, 'entriesAdded': 2, 'error': '', 'firstEntryData': 'eyJ4IjogMn0=', 'firstEntryJson': {'x': 2}, 'firstEntryString': '{"x": 2}', 'groups': 0, 'lastEntryData': 'eyJ4IjogM30=', 'lastEntryJson': {'x': 3}, 'lastEntryString': '{"x": 3}', 'meta': {}, 'radixTreeKeys': 1, 'radixTreeNodes': 2}, 
        {'id': 'zzzz', 'length': 0, 'entriesAdded': 0, 'error': 'no such key', 'firstEntryData': '', 'firstEntryJson': {}, 'firstEntryString': '', 'groups': 0, 'lastEntryData': '', 'lastEntryJson': {}, 'lastEntryString': '', 'meta': {'x': 1, 'z': 3}, 'radixTreeKeys': 0, 'radixTreeNodes': 0}]}]}}



    assert run_graphql('''
query GetStreams {
    streams { id length }
}
    ''', {}) == {   "data": {     "streams": [       
        {         "id": ":event:device.connected",         "length": 2       },       
        {         "id": ":event:device.meta",         "length": 2       },       
        {         "id": "asdf:zxcv",         "length": 2       },       
        {         "id": "asdf:zzzz",         "length": 0       },       
        {         "id": "default:xxxx",         "length": 2       }     ]   } }

    assert run_graphql('''
query GetDeviceStreams {
    devices {
        streams {
            id
            length
            firstEntryJson
            lastEntryJson
        }
    }
}
    ''', {}) == {   "data": {     "devices": [       {         "streams": [           
        {             "id": "zxcv",             "length": 2,             "firstEntryJson": {"x": 2},             "lastEntryJson": {"x": 3}           },           
        {             "id": "zzzz",             "length": 0,             "firstEntryJson": {},             "lastEntryJson": {}           }         
    ]       }     ]   } }

    assert run_graphql('''
mutation DeleteStream {
  zxcv: deleteStream(streamId: "zxcv", deviceId: "asdf")
  zzzz: deleteStream(streamId: "zzzz", deviceId: "asdf")
}
    ''', {}) == {   "data": {     
        "zxcv": {       "data_deleted": True,       "meta_deleted": False, 'stream_deleted': True    },
        "zzzz": {       "data_deleted": False,       "meta_deleted": True, 'stream_deleted': False    }   } }

    assert run_graphql('''
query GetStreams {
    devices {
        id
        streams { id }
    }
}
    ''', {}) == {   "data": {     "devices": [       {         "id": "asdf",         "streams": []       }     ]   } }


