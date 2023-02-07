from __future__ import annotations
import os
import time
import asyncio
from redis import asyncio as aioredis

from redis_streamer import utils

class Context:
    stream_maxlen = os.getenv('REDIS_STREAM_MAXLEN') or 1000
    async def init(self):
        url = os.getenv('REDIS_URL') or 'redis://127.0.0.1:6789'
        max_connections = int(os.getenv('REDIS_MAX_CONNECTIONS') or 9000)
        print("Connecting to", url, '...')
        self.r = await aioredis.from_url(url=url, max_connections=max_connections)
        print("Connected?", await self.r.ping())
ctx = Context()

META_PREFIX = 'XMETA'



class Agent:
    '''Redis streaming agent'''
    def __init__(self, ws=None) -> None:
        self._ws = ws

    @property
    def ws(self):
        if self._ws is None:
            raise RuntimeError("No websocket available.")
        return self._ws

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
        data = await self.ws.recv_bytes()
        return self.xadd(p, sid, data, **kw)

    # ----- These are synchronous because they can be used with a transaction ---- #

    def xread(self, p, sids, count=1, block=None):
        return p.xread(sids, count=count, block=block)

    def xrevrange(self, p, sid, start, end='+', inclusive=False, count=1):
        if start == '$':
            start = utils.format_epoch_time(time.time())
        if not inclusive and start != '-':
            start = f'({start}'
        return p.xrevrange(sid, end, start, count=count)

    def xrange(self, p, sid, start, end='+', inclusive=False, count=1):
        if start == '$':
            start = utils.format_epoch_time(time.time())
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

    def init_cursor(self, sids: list[str]|dict[str, str], prefix='') -> dict[str, str]:
        # allow list - default to after now
        if isinstance(sids, list):
            sids = {s: '$' for s in sids}
        if prefix:
            sids = {f'{prefix}{s}': t for s, t in sids.items()}
        # replace dollar with explicit time
        for k, l in sids.items():
            if l == '$':
                sids[k] = utils.format_epoch_time(time.time())
        return sids

    # def encode_cursor(self, sids):
    #     return {utils.maybe_encode(k): utils.maybe_encode(l) for k, l in sids.items()}

    def update_cursor(self, sids: dict[str, str], data: list[str|tuple]) -> dict[str, str]:
        for s, ts in data:
            if ts:
                sids[s] = max(t for t, x in ts)
        return sids

    async def read(self, sids, latest=False, block=None, **kw) -> tuple[list, dict[str, str]]:#tuple[list[str|list[tuple[str|list[bytes]]]], dict[str, str]]
        if latest:
            async with ctx.r.pipeline() as p:
                for sid, t in sids.items():
                    self.xrevrange(p, sid, t, **kw)
                data = list(zip(sids, await p.execute()))
            if not any(x for s, x in data):
                data = await self.xread(ctx.r, sids, block=block, **kw)
        else:
            data = await self.xread(ctx.r, sids, block=block, **kw)

        # decode stream IDs and timestamps
        data = decode_xread_format(data)
        return data, self.update_cursor(sids, data)


def decode_xread_format(data):
    return [
        (utils.maybe_decode(s), [(utils.maybe_decode(t), x) for t, x in xs])
        for s, xs in data
    ]


def init_stream_cursor(sids):
    t = utils.format_epoch_time(time.time())
    sids = {k: t if l == '$' else l for k, l in sids.items()}
    return sids