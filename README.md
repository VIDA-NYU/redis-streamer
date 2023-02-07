# Redis Streamer

A graphql + websocket client for Redis Streams.

## Getting started

To bring up redis and the API, do:
```bash
docker-compose up -d --build
```
Access the API here: http://localhost:8000

Access the GraphQL playground here: http://localhost:8000/graphql

### Sending and Receiving Data

To send/receive data, you should do it over websockets. i.e. `ws://localhost:8000`

> NOTE: By default, this websocket library limits incoming messages to 1MB and will throw an error if they are larger. To disable this, you can
> either set `max_size=None` to disable it entirely, or set it to some sensible number e.g. `max_size=2**26` is `64MB`.

`pip install websockets`

#### Send: `/data/{stream_id}/push`

```python
import json
import websockets

async def send_data(sid: str):
    async with websockets.connect(f'ws://localhost:8000/data/{sid}/push') as ws:
        while True:
            # do what you do
            data = generate_some_data()
            # get it ready to store
            data = serialize_bytes(data)
            # send the header - used for batched uploads (or to manually set the timestamp)
            await ws.send(json.dumps([ len(data) ]))
            # send the data
            await ws.send(data)
```


If you want to skip sending the header:

```python
import websockets

async def send_data(sid: str):
    async with websockets.connect(f'ws://localhost:8000/data/{sid}/push?header=0') as ws:
        while True:
            # do what you do
            data = generate_some_data()
            # get it ready to store
            data = serialize_bytes(data)
            # send the data
            await ws.send(data)
```

#### Receive: `/data/{stream_id}/pull`
```python
import json
import websockets

async def receive_data(sid: str):
    async with websockets.connect(f'ws://localhost:8000/data/{sid}/pull', max_size=None) as ws:
        while True:
            # read the header
            header = json.loads(await ws.recv())
            # read the data
            entries = await ws.recv()

            # unpack the header
            sids, ts, offsets = tuple(zip(*header)) or ((),)*3
            # split up the data (for cases where you query multiple streams)
            for sid, t, start, end in zip(sids, ts, offsets, offsets[1:] + (None,)):
                do_something_with_data(sid, t, entries[start:end])
```

To skip the header and assume single messages:

```python
import websockets

async def receive_data(sid: str):
    async with websockets.connect(f'ws://localhost:8000/data/{sid}/pull?header=0', max_size=None) as ws:
        while True:
            # read the data
            data = await ws.recv()
            # parse internal timestamp
            timestamp, data = my_parse_header_and_payload(data)
            do_something_with_data(timestamp, data)
```

To allow frame dropping:
```python
import websockets

async def receive_data(sid: str):
    async with websockets.connect(f'ws://localhost:8000/data/{sid}/pull?header=0&latest=1', max_size=None) as ws:
        while True:
            # read the data
            data = await ws.recv()
            # parse internal timestamp
            timestamp, data = my_parse_header_and_payload(data)
            do_something_with_data(timestamp, data)
```

### Sending and Receiving Data without Websockets

For cases where you are unable to use websockets, you can also just regular REST requests to send the data.

`pip install requests`

To query data:
```python 
import requests

sid = "my-stream"

# get messages from the current timestamp - blocks for 500 ms by default
r = requests.get(f'ws://localhost:8000/data/{sid}')
r.raise_for_status()
data = r.content

# get the last entry id to query with next time
last_entry_id = r.headers['x-last-entry-id']

# get the next data point after the one you just received
r = requests.get(f'ws://localhost:8000/data/{sid}', params={'last_entry_id': last_entry_id})
r.raise_for_status()
data = r.content


# get the last data point in the queue (no matter how old it is)
r = requests.get(f'ws://localhost:8000/data/{sid}', params={'last_entry_id': '0', 'latest': True})
r.raise_for_status()
data = r.content

# start reading from the beginning of the queue
r = requests.get(f'ws://localhost:8000/data/{sid}', params={'last_entry_id': '0'})
r.raise_for_status()
data = r.content

# get a message from 5 minutes ago (the next message after the provided timestamp)
t = time.time() - 5*60
r = requests.get(f'ws://localhost:8000/data/{sid}', params={'last_entry_id': f'{int(t * 1000)}-0'})
r.raise_for_status()
data = r.content

# get the latest message, ignoring anything older than 5 minutes ago
t = time.time() - 5*60
r = requests.get(f'ws://localhost:8000/data/{sid}', params={'last_entry_id': f'{int(t * 1000)}-0', 'latest': True})
r.raise_for_status()
data = r.content

# block the request for up to five seconds and return the first message that comes in.
t = time.time() - 5*60
r = requests.get(f'ws://localhost:8000/data/{sid}', params={'last_entry_id': '$', 'block': 5000})
r.raise_for_status()
data = r.content
if not data:
    print("No new data")
```

To send data:
```python 
import requests

sid = "my-stream"
r = requests.post(f'ws://localhost:8000/data/{sid}', files=[('entries', (sid, data))])
r.raise_for_status()
```

### Using Graphql

You can query the graphql - playground and schema are available (once you start the server) [here](http://localhost:8000/graphql). 

Make requests like this:
```python
import requests
requests.post("http://localhost:8000/graphql" json=({ "query": 'query YourQuery { ... $x ... }', "variables": { 'x': 5 } }))
```

### Querying devices
This API is designed to handle streams from multiple different sensors. This works by adding a prefix string to streams.
If no device is declared, it writes under the device `default`.

#### Listing devices

```t
query GetDevices {
  connected: devices {
    id  # the name of the device
  }

  seen: devices(include_all: true) {
    id
  }
}
```

The only difference between `connected` and `seen` is that devices may be removed from `connected` if they are deemed disconnected, but will remain in `seen`.

Device disconnected is currently not handled automatically, so it is a bookkeeping stage to be handled by the client for now.

#### Listing device streams
variables: `{id: "my-device"}`
```t
query GetDevices {
  devices {
    streamIds(device_id: $id)  # just return the names of the streams
    streams(device_id: $id) {  # return the full metadata for all streams
      streamId
      firstEntryId
      lastEntryId
      length
    }
  }
}
```

Under the hood, streams are stored with their device prefix, but that prefix is removed when viewing them here.

#### Connecting/Disconnecting a device
variables: `{id: "my-device", meta: {parameterA: 11.2, parameterB: "xyz"}}`
```t
mutation {
  connectDevice(id: $id, meta: $meta)
}
```
variables: `{id: "my-device"}`
```t
mutation {
  disconnectDevice(id: $id)
}
```

### Querying Stream Metadata


You can query this stream info:

`sid` represents the stream ID (the redis key).
```t
query GetStreams {
  devices {
    sids  # get just the names without querying everything else
    streams {
      # redis XINFO STREAM information
      id
      entriesAdded
      firstEntryId
      firstEntryData
      firstEntryString
      firstEntryJson
      firstEntryTime
      lastEntryId
      lastEntryData
      lastEntryTime
      lastGeneratedId
      maxDeletedEntryId
      groups
      length
      radixTreeKeys
      radixTreeNodes
      recordedFirstEntryId
      # error message for XINFO STREAMS
      error

      # user-defined stream metadata
      meta
    }
  }

}
```

For information about the XINFO STREAM fields, see [these docs](https://redis.io/commands/xinfo-stream/).

The only difference is that we broke out `first-entry` and `last-entry` into parts:
 - `first-entry-id`: The redis timestamp, e.g. `"1638125133432-0"`
 - `first-entry-time`: The redis timestamp in iso datetime format, e.g. ``
 - `first-entry-data`: The first value in the stream as a base64 encoded string
 - `first-entry-string`: The first value in the stream as a utf-8 encoded string
 - `first-entry-json`: The first value in the stream parsed as json

The same applies for `last-entry`.

If you want to ignore the concept of devices, you can also just query all streams:
```t
query Streams {
  streams {
    id
  }
}
```
These streams will retain any device prefixes and also lists out system event streams.

#### Setting metadata
You can attach arbitrary JSON to a stream to store whatever info you need.

variables: `{sid: "glf", meta: {format: "mp4"}}`
```t
mutation {
  updateStreamMeta(sid: $id, meta: $meta)
}
```
variables: `{sids: ["glf"]}`
```t
{
  streams(sids: $sids) {
    sid
    meta
  }
}
```

#### Subscribing to data
For small and/or text-based data streams, you can use graphql subscriptions to receive the data.

This is here for convenience and probably shouldn't be used for high-volume data since it does base64 encoding. Instead use the websocket methods above.

variables: `{sids: ["glf"]}`
```t
subscription DataSubscription {
  data(streamIds: $sids) {
    streamId
    time
    data  # base64 encoded data
    string  # read data as utf-8 text
    json
  }
}
```

## Testing

A sample client is available in `tests/api.py`.

### Pushing an incrementing integer
Writing to the stream:
```bash
python tests/api.py push_increment counter
```

Reading from the stream:
```bash
python tests/api.py pull_raw counter
```

### Pushing random noise jpeg images
Writing both rgb and greyscale images:
```bash
python tests/api.py push_image blah --shape '[700,700,3]'
python tests/api.py push_image blah_gray --shape '[400,200]'
```

Pulling images, dropping frames we don't get to in time so we don't fall behind:
```bash
python tests/api.py pull_image blah --latest
python tests/api.py pull_image blah_gray --latest
```