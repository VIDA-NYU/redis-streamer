# Redis Streamer

A graphql + websocket client for Redis Streams.

## Getting started

```bash
docker-compose up -d --build
```
Access here: http://localhost:8000

### Sending and Receiving Data

To send/receive data, you should do it over websockets. i.e. `ws://localhost:8000`

#### Send: `/data/{stream_id}/push`

```python
import json
import websockets

async def send_data(sid: str):
    async with websockets.connect(f'ws://localhost:8000/data/{sid}/push') as ws:
        # do what you do
        data = generate_some_data()
        # get it ready to store
        data = serialize_bytes(data)
        # send the header
        await ws.send(json.dumps([ len(data) ]))
        # send the data
        await ws.send(data)
```

#### Receive: `/data/{stream_id}/pull`
```python
import json
import websockets

async def receive_data(sid: str):
    async with websockets.connect(f'ws://localhost:8000/data/{sid}/pull') as ws:
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


### Querying Stream Metadata

The stream metadata is available using a graphql endpoint - playground [here](http://localhost:8000/graphql). 

Make requests like this:
```bash
POST "http://localhost:8000/graphql"
     json=({ query: 'query YourQuery { ... }' })
```

You can query this stream info:

`sid` represents the stream ID (the redis key).
```t
query GetStreams {
  sids  # get just the names without querying everything else
  streams {
    # redis XINFO STREAM information
    sid
    entriesAdded
    firstEntryId
    firstEntryData
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
```

For information about the XINFO STREAM fields, see [these docs](https://redis.io/commands/xinfo-stream/).

The only difference is that we broke out `first-entry` and `last-entry` into parts:
 - `first-entry-id`: The redis timestamp, e.g. `"1638125133432-0"`
 - `first-entry-time`: The redis timestamp in iso datetime format, e.g. ``
 - `first-entry-data`: The first value in the stream as a base64 encoded string

The same applies for `last-entry`.

#### Setting metadata
You can attach arbitrary JSON to a stream to store whatever info you need.

```t
mutation {
  streamMeta(
    sid: "glf",
    meta: {format: "mp4"},
  )
}
```

```t
{
  streams(sids: ["glf"]) {
    sid
    meta
  }
}
```

#### Subscribing to data
For small and/or text-based data streams, you can use graphql subscriptions to receive the data.

This is here for convenience and probably shouldn't be used for high-volume data since it does base64 encoding. Instead use the websocket methods above.

```t
subscription DataSubscription {
  data(sids: ["glf"]) {
    sid
    time
    data  # base64 encoded data
    utf8  # read data as utf-8 text
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