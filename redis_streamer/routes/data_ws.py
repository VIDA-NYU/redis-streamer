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
        latest: bool=Query(False, description='Should we allow frame skipping? Ok for some data, not for others.'),
        max_fps: float=Query(0, description='Should we allow frame skipping? Ok for some data, not for others.'),
):
    '''Pull data.
    
    Protocol:
     - receive offset json. [(stream_id, timestamp, end_index), ... for each data payload]
     - receive data bytes. Can contain multiple messages, refer to offset 
        i.e. (data[previous_end_index:end_index])
    '''
    await ws.accept()
    agent = Agent(ws)
    try:
        t0 = time.time()
        cursor = agent.init_cursor({stream_id: last_entry_id})
        while True:
            results, cursor = await agent.read(cursor, latest=latest, block=block)
            offsets, entries = utils.pack_entries(results)
            await ws.send_json(offsets)
            await ws.send_bytes(entries)
            if max_fps:
                await asyncio.sleep(max(0, 1 / max_fps - (time.time() - t0)))
                t0 = time.time()
    except (WebSocketDisconnect, ConnectionClosed):
        pass


@app.websocket('/{stream_id}/push')
async def push_stream_data_ws(
        ws: WebSocket,
        stream_id: str = Path(..., description='The unique ID of the streams'),
        ack: bool=Query(False, description='Whether the server should send back a response after storing. '
                                           'If you enable this, you must read the response otherwise everything will hang.'),
):
    '''Push data.'''
    await ws.accept()
    agent = Agent(ws)
    stream_ids = stream_id.split('+')
    try:
        while True:
            sids, ts, offsets = parse_offsets(await ws.receive_json(), stream_ids)
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
