import asyncio
import io
import orjson
from fastapi import APIRouter, Query, Path, File, UploadFile
from fastapi.responses import StreamingResponse
from redis_streamer import Agent, utils
from redis_streamer.config import DEFAULT_DEVICE, ENABLE_MULTI_DEVICE_PREFIXING

app = APIRouter()

@app.post('/{stream_id}', summary='Send data to one or multiple streams')
async def send_data_entries(
        sid: str = Path(..., alias='stream_id', description='The unique ID of the stream'),
        entries: list[UploadFile] = File(..., description='A list of data entries (as multiform files) to be added into the stream(s).'),
        device_id: str=Query(DEFAULT_DEVICE, description='You should give devices names if you want to manage multiple devices.'),
        prefix: str=Query('', description='Add a prefix to the streams. If a device ID is provided, this will come after the device ID.'),
):
    """Send data into one or multiple streams using multipart/form-data,
    each part represent a separate entry of a stream. Set
    **stream_id** to `*` to upload data to multiple streams. In this
    case, the **filename** field of the multipart header will be used as
    stream ids.

    """
    sids = [x.filename.split('/') for x in entries] if sid == '*' else [sid] * len(entries)
    if ENABLE_MULTI_DEVICE_PREFIXING:
        prefix = f'{device_id or DEFAULT_DEVICE}:{prefix}'
    sids = [f'{prefix}{s}' for s in sids]
    data = await asyncio.gather(*(x.read() for x in entries))
    return await Agent().add_entries(zip(sids, [None]*len(sids), data))


@app.get('/{stream_id}', summary='Retrieve data from one or multiple streams', response_class=StreamingResponse)
async def get_data_entries(
        sid: str = Path(..., alias='stream_id', description='The unique ID of the stream'),
        last_entry_id: str=Query('-', description="Start retrieving entries later than the provided ID"),
        latest: bool=Query(False, description="Should we return the latest available frame?"),
        count: int=Query(1, description="the maximum number of entries for each receive"),
        block: int=Query(None, description="Should it block if no data is available?"),
        device_id: str=Query(DEFAULT_DEVICE, description='You should give devices names if you want to manage multiple devices.'),
        prefix: str=Query('', description='Add a prefix to the streams. If a device ID is provided, this will come after the device ID.'),
    ):
    """This retrieves **count** elements that have later timestamps
    than **last_entry_id** from the specified data stream. The entry
    ID should be in the form of:

    `<millisecondsTime>-<sequenceNumber>` 

    More info can be found on [Redis's
    documentation](https://redis.io/docs/manual/data-types/streams/#entry-ids). Special
    IDs such as `0` and `$` are also accepted. In addition, if
    **last_entry_id** is a `*`, the latest **count** entries will be
    returned.

    If successful, the response header will include an `entry-offset`
    field describing the offsets of the batch
    `[[stream_id,entry_id,offset],...]` in JSON format.

    This can also be used to retrieve data from multiple streams. To
    do so, set **stream_id** to a list of stream IDs separated by
    `+`. For example, to retrieve data from `main` and `depth` stream,
    set **stream_id** to `main+depth`. This means that stream id must
    not contain the `+` sign. When multiple streams are specified,
    **last_entry_id** could be set specifically for each stream using
    the similar `+` separator (e.g. **last_entry_id**=`$+$`), or for
    the all streams (e.g. just `$`).

    """
    agent = Agent()
    
    if ENABLE_MULTI_DEVICE_PREFIXING:
        prefix = f'{device_id or DEFAULT_DEVICE}:{prefix}'
    
    cursor = agent.init_cursor({f'{prefix}{sid}': last_entry_id})
    entries, cursor = await agent.read(cursor, latest=latest, count=count, block=block)
    
    if ENABLE_MULTI_DEVICE_PREFIXING:
        entries = [(s[len(prefix):] if s.startswith(prefix) else s, xs) for s, xs in entries]
    
    offsets, content = utils.pack_entries(entries)
    return StreamingResponse(
        io.BytesIO(content),
        headers={
            'x-offsets': orjson.dumps(offsets).decode('utf-8'), 
            'x-last-entry-id': cursor[f'{prefix}{sid}']
        },
        media_type='application/octet-stream')
