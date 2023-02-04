import os
import time
import asyncio
import orjson
from redis import asyncio as aioredis

from . import utils

class Context:
    stream_maxlen = os.getenv('REDIS_STREAM_MAXLEN') or 1000
    async def init(self):
        self.r = await aioredis.from_url(
            url=os.getenv('REDIS_URL') or 'redis://127.0.0.1:6789',
            max_connections=int(os.getenv('REDIS_MAX_CONNECTIONS') or 9000),
        )
ctx = Context()

META_PREFIX = 'XMETA'



class Agent:
    '''Redis agent'''
    def __init__(self, ws=None) -> None:
        self.ws = ws

    # ---------------------------------------------------------------------------- #
    #                             Client communication                             #
    # ---------------------------------------------------------------------------- #

    async def write_bytes(self, message):
        if self.ws is None:
            raise RuntimeError("No websocket available.")
        data = await self.ws.recv_bytes()

    # ---------------------------------------------------------------------------- #
    #                               Running commands                               #
    # ---------------------------------------------------------------------------- #

    async def run_commands(self, query, cmd=None):
        squeeze = isinstance(query, dict)
        async with ctx.r.pipeline() as p:
            for q in [query] if squeeze else query:
                if cmd:
                    q['cmd'] = cmd
                ack = q.pop('ack', False)
                await self.run_command(p, **query)
                if ack:
                    if self.ws is None:
                        raise RuntimeError("No websocket provided to send ack.")
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
        return self.xread(p, sids, **kw)

    async def cmd__xrevrange(self, p, sid, **kw):
        return self.xrevrange(p, sid, **kw)

    async def cmd__xrange(self, p, sid, **kw):
        return self.xrange(p, sid, **kw)

    async def cmd__xlen(self, p, sid):
        return self.xlen(p, sid)

    async def cmd__xadd(self, p, sid, **kw):
        if self.ws is None:
            raise RuntimeError("No websocket provided to receive data from.")
        data = await self.ws.recv_bytes()
        return self.xadd(p, sid, data, **kw)

    # ----- These are synchronous because they can be used with a transaction ---- #

    def xread(self, p, sids, count=1, block=True):
        return p.xread(sids, count=count, block=block)

    def xrevrange(self, p, sid, start, end='+', inclusive=False, count=1):
        if start == '$':
            start = utils.format_epoch_time(time.time())
        if not inclusive and start != '-':
            start = f'({start}'
        return p.xrevrange(sid, end, start, count=count)

    def xrange(self, p, sid, start, end='+', inclusive=False, count=1):
        if not inclusive and start != '-':
            start = f'({start}'
        return p.xrange(sid, start, end, count=count)

    def xlen(self, p, sid):
        return p.xlen(sid)

    def xadd(self, p, sid, data, time='*', metadata=None):
        return p.xadd(sid, {b'd': data, **(metadata or {})}, time or '*')


    # ---------------------------------------------------------------------------- #
    #                            Adding data to a stream                           #
    # ---------------------------------------------------------------------------- #

    async def add_entry(self, p, sid, t, data, meta=None):
        return p.xadd(sid, {b'd': data, **(meta or {})}, t or '*', maxlen=ctx.stream_maxlen, approximate=True)

    async def add_entries(self, entries):
        async with ctx.r.pipeline() as p:
            for sid, t, entry in entries:
                await self.add_entry(p, sid, t, entry)
            return await p.execute()
            

    # ---------------------------------------------------------------------------- #
    #                              Reading Streamers                               #
    # ---------------------------------------------------------------------------- #

    def update_cursor(self, sids, data):
        return {s: max((t for t in data), default=t) for s, t in sids.items()}


    async def read(self, sids, latest=False, **kw):
        if latest:
            with ctx.r.pipeline() as p:
                for sid, t in sids.items():
                    self.xrevrange(p, sid, t, **kw)
                data = list(zip(sids, await p.execute()))
        else:
            data = await self.xread(ctx.r, sids, **kw)
        return data, self.update_cursor(sids, data)

    async def iread(self, sid, start, latest=False):
        async for v in (
            self.iread_latest(sid, start) 
            if latest else 
            self.iread_all({sid: start})
        ):
            yield v

    async def iread_all(self, sids):
        while True:
            xs = await ctx.r.xread(sids)
            for sid, xs in xs:
                for t, x in xs:
                    yield sid, t, x
                if xs:
                    sids[sid] = xs[-1][0]

    async def iread_latest(self, sid, start='$'):
        while True:
            xs: list = await self.xrevrange(ctx.r, sid, start)
            for t, x in xs:
                yield sid, t, x
            if xs:
                start = xs[-1][0]
