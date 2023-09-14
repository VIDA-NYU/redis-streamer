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


# @app.websocket('/prompt')
# async def prompt_data_ws(ws: WebSocket):
#     ''''''
#     await ws.accept()
#     agent = Agent(ws)
#     try:
#         async for query in recv_queries(ws):
#             results = await agent.run_commands(query)
#             pack_entries(results)
#     except (WebSocketDisconnect, ConnectionClosed):
#         pass


@app.websocket('/pull')
async def prompt_pull_data_ws(ws: WebSocket):
    '''stream XREAD calls

    Protocol:
     - send query as json (text). Send empty text to repeat previous query.
     - receive metadata as json (text)
     - receive data as bytes

    Query format:
        streams (dict): A dictionary of stream IDs and redis timestamps (exclusive). 
            i.e. [stream_id]: timestamp
        count (int): How many messages should we query for at once? The messages will
            still be streamed back one by one. Default: 1
        block (int): number of milliseconds to wait, if no data is available. If null, 
            it will wait indefinitely. Default: None

    Metadata format:
        stream_id (str): The stream ID
        time (str): The redis timestamp of the sample.
    '''
    await ws.accept()
    agent = Agent()
    runner = Runner(agent, ws)
    try:
        async for query in recv_queries(ws):
            xs = await runner.run_commands([query], 'XREAD')
            for sid, t, data, metadata in xs:
                await ws.send_json({
                    'stream_id': sid,
                    'time': t,
                    'metadata': metadata,
                })
                await ws.send_bytes(data)
    except (WebSocketDisconnect, ConnectionClosed):
        pass

@app.websocket('/push')
async def prompt_push_data_ws(
    ws: WebSocket,
    ack: bool=Query(False, description='Whether the server should send back a response after storing. '
                                        'If you enable this, you must read the response otherwise everything will hang.'),
):
    '''stream XADD calls. 
    Process:
     - send query as json (text). Send empty text to repeat previous query.
     - send data as bytes
     - receive success as json (text).

    Query format:
        stream_id (str): The name of the stream you are sending.
        metadata (dict): Any metadata you would like to store alongside the entry.
    '''
    await ws.accept()
    agent = Agent()
    runner = Runner(agent, ws)
    try:
        async for query in recv_queries(ws):
            results = await runner.run_commands([query], 'XADD')
            if ack:
                await ws.send_json(results)
    except (WebSocketDisconnect, ConnectionClosed):
        pass


# ----------------------------------- Utils ---------------------------------- #

def serialize_metadata(sid, t, metadata):
    return {
        'stream_id': sid,
        'time': t,
        'metadata': metadata,
    }

async def recv_queries(ws):
    query = previous_query = []
    while True:
        query_str = await ws.receive_text()
        if query_str:
            query = orjson.loads(query_str)
            previous_query = query
        elif previous_query:
            query = previous_query
        else:
            raise ValueError("No query")
        yield query


class Runner:
    def __init__(self, agent, ws):
        self.agent = agent
        self.ws = ws

    async def run_commands(self, query, cmd=None):
        squeeze = isinstance(query, dict)
        async with ctx.r.pipeline() as p:
            for q in [query] if squeeze else query:
                if cmd:
                    q['cmd'] = cmd
                ack = q.pop('ack', False)
                await self.run_command(p, **query)
                if ack:
                    await self.ws.send_text('')
            xs = await p.execute()
        return xs[0] if squeeze else xs
    
    async def run_command(self, p, cmd, **query):
        return getattr(self, f'cmd__{cmd}')(p, **query)

    # ---------------------------------------------------------------------------- #
    #                                Redis Commands                                #
    # ---------------------------------------------------------------------------- #

    # ------- These are asynchronous so we can await them without checking ------- #

    async def cmd__xread(self, p, sids, **kw):
        return self.agent.xread(p, sids, **kw)

    async def cmd__xrevrange(self, p, sid, **kw):
        return self.agent.xrevrange(p, sid, **kw)

    async def cmd__xrange(self, p, sid, **kw):
        return self.agent.xrange(p, sid, **kw)

    async def cmd__xlen(self, p, sid):
        return self.agent.xlen(p, sid)

    async def cmd__xadd(self, p, sid, **kw):
        data = await self.ws.recv_bytes()
        return self.agent.xadd(p, sid, data, **kw)