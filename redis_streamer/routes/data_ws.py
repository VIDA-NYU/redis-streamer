'''

Specification:



XREAD {
    cmd = XREAD,
    streams: {
        id: previous timestamp,
    },
    count: int,
    block: bool,
}


XADD: {
    cmd: XADD,
    keys: [*keys],
}

'''
import time
import asyncio
import orjson
from fastapi import APIRouter, Path, Query, WebSocket, WebSocketDisconnect
from websockets.exceptions import ConnectionClosed

from .. import utils
from ..core import ctx, Agent
from redis_streamer.config import DEFAULT_DEVICE, ENABLE_MULTI_DEVICE_PREFIXING

app = APIRouter()


# ---------------------------------------------------------------------------- #
#                             Single Stream Format                             #
# ---------------------------------------------------------------------------- #

@app.websocket('/{stream_id}/pull')
async def pull_stream_data_ws(
        ws: WebSocket,
        stream_id: str = Path(..., description='The unique ID of the stream'),
        last_entry_id: str=Query('$', description="Start retrieving entries later than the provided ID"),
        block: int|None=Query(5000, description="How long to block, in milliseconds"),
        latest: bool=Query(False, description='Should we allow frame skipping? Ok for some data (e.g. jpeg), not for others (e.g. mp4). Can use with max_fps or ack to reduce frame rate.'),
        max_fps: float=Query(0, description='Should we limit the frame rate that data is sent? Useful in cases with latest=True.'),
        device_id: str=Query(DEFAULT_DEVICE, description='You should give devices names if you want to manage multiple devices.'),
        keep_device_id_in_stream_id: bool|None=Query(None, description='This will remove the device ID from stream IDs. Set this to False to disable.'),
        ack: bool=Query(False, description="Should the server wait for you to send back a (text) message before sending the next payload? Can be useful to avoid messages piling up in the queue."),
        prefix: str=Query('', description='Add a prefix to the streams. If a device ID is provided, this will come after the device ID.'),
):
    '''Pull data.
    
    Protocol:
     - receive offset json. [(stream_id, timestamp, end_index), ... for each data payload]
     - receive data bytes. Can contain multiple messages, refer to offset 
        i.e. (data[previous_end_index:end_index])
    '''
    await ws.accept()
    agent = Agent(ws)

    # prepare device prefix
    if ENABLE_MULTI_DEVICE_PREFIXING:
        device_id = device_id or DEFAULT_DEVICE
        prefix = f'{device_id}:{prefix}'
        if keep_device_id_in_stream_id is None:
            keep_device_id_in_stream_id = '*' in device_id

    try:
        t0 = time.time()
        cursor = agent.init_cursor({f'{prefix}{stream_id}': last_entry_id})
        while True:
            results, cursor = await agent.read(cursor, latest=latest, block=block)
            # strip device ID from stream IDs
            if not keep_device_id_in_stream_id:
                results = [(s[len(prefix):] if s.startswith(prefix) else s, xs) for s, xs in results]

            # prepare and send back data
            offsets, entries = utils.pack_entries(results)
            await ws.send_json(offsets)
            await ws.send_bytes(entries)
            # rate limiting
            if max_fps:
                await asyncio.sleep(max(0, 1 / max_fps - (time.time() - t0)))
                t0 = time.time()
            if ack:
                await ws.receive_text()
    except (WebSocketDisconnect, ConnectionClosed):
        pass


@app.websocket('/{stream_id}/push')
async def push_stream_data_ws(
        ws: WebSocket,
        stream_id: str = Path(..., description='The unique ID of the streams'),
        ack: bool=Query(False, description='Whether the server should send back a response after storing. '
                                           'If you enable this, you must read the response otherwise everything will hang.'),
        device_id: str=Query(DEFAULT_DEVICE, description='You should give devices names if you want to manage multiple devices.'),
        prefix: str=Query('', description='Add a prefix to the streams. If a device ID is provided, this will come after the device ID.'),
):
    '''Push data.'''
    await ws.accept()
    agent = Agent(ws)
    stream_ids = stream_id.split('+')
    if ENABLE_MULTI_DEVICE_PREFIXING:
        prefix = f'{device_id or DEFAULT_DEVICE}:{prefix}'

    try:
        while True:
            sids, ts, offsets = parse_offsets(await ws.receive_json(), stream_ids)
            sids = [f'{prefix}{s}' for s in sids]
            data = await ws.receive_bytes()
            entries = get_data_from_offsets(data, offsets)
            result = await agent.add_entries(zip(sids, ts, entries))
            if ack:
                await ws.send_json(result)
    except (WebSocketDisconnect, ConnectionClosed):
        pass


def parse_offsets(offsets: list, sids: list[str]):
    ts = (None,)*max(len(offsets), 1)
    if offsets and isinstance(offsets[0], list):
        if len(offsets[0]) == 3:
            sids, ts, offsets = zip(*offsets)
        else:
            sids, offsets = zip(*offsets)
    if len(sids) != len(offsets):
        raise ValueError("")
    
    return sids, ts, offsets

def get_data_from_offsets(data, offsets):
    if not offsets or offsets[0] != 0:
        offsets = (0,)+tuple(offsets)
    return [data[i:j] for i, j in zip(offsets, offsets[1:])]
