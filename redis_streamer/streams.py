import os
import time
import typing
from typing import AsyncGenerator

import strawberry
import asyncio
import orjson
from redis import asyncio as aioredis
from . import utils
from .core import ctx, Agent



META_PREFIX = 'XMETA'



@strawberry.type
class Stream:
    sid: str
    first_entry: str|None=None
    last_entry: str|None=None
    last_generated_id: str|None
    recorded_first_entry_id: str
    length: int
    error: str|None
    meta: dict

    @strawberry.field
    def first_entry_time(self):
        return utils.format_iso(self.first_entry) if self.first_entry else None

    @strawberry.field
    def last_entry_time(self):
        return utils.format_iso(self.last_entry) if self.last_entry else None

    # @strawberry.field
    # async def meta(self) -> dict:
    #     return await ctx.r.get(f'{META_PREFIX}:{self.sid}')

    @classmethod
    def from_info_meta(cls, sid, info, meta=None):
        if isinstance(info, Exception):
            info = {'error': str(info)}
        if isinstance(info, Exception):
            info = {'error': str(info)}
        d = {'sid': sid, **({k.replace('-', '_'): v for k, v in (info or {}).items()})}
        if d.get('first_entry'):
            d['first_entry_id'], d['first_entry_data'] = d.pop('first_entry')
        if d.get('last_entry'):
            d['last_entry_id'], d['last_entry_data'] = d.pop('last_entry')
        d['meta'] = {'error': str(meta)} if isinstance(info, Exception) else meta
        return cls(**d)

    @strawberry.subscription
    async def data_stream(self, target: int = 100) -> AsyncGenerator[int, None]:
        for i in range(target):
            yield i
            await asyncio.sleep(0.5)


class Streams:
    @strawberry.field
    async def stream_ids(self) -> list[str]:
        return sorted([x.decode('utf-8') async for x in ctx.r.scan_iter(_type='stream')])

    @strawberry.field
    async def streams(self, stream_ids: list[str]) -> list[Stream]:
        async with ctx.r.pipeline() as pipe:
            for sid in stream_ids:
                pipe.get(f'{META_PREFIX}:{sid}').xinfo_stream(sid)
            res = await pipe.execute(raise_on_error=False)
            return [
                Stream.from_info_meta(sid, info, meta)
                for sid, meta, info in zip(stream_ids, res[::2], res[1::2])
            ]




@strawberry.type
class Mutation:
    @strawberry.mutation
    async def stream_meta(self, sid: str, *, _update=False, **meta):
        key = f'{META_PREFIX}:{sid}'
        if _update:
            previous = await ctx.r.get(key)
            if previous:
                meta = dict(orjson.loads(previous), **meta)
        return await ctx.r.set(key, orjson.dumps(meta))

@strawberry.type
class DataMetadata:
    sid: str
    time: str

@strawberry.type
class DataPayload:
    sid: str
    time: str
    data: bytes


@strawberry.type
class DataSubscription:
    @strawberry.subscription
    async def timestamps(
        self, streams: dict[str, str], count=1, block=True
    ) -> AsyncGenerator[DataMetadata, None]:
        last = streams
        while True:
            result = await ctx.r.xread(last, count=count, block=block)
            for sid, xs in result:
                for t, _ in xs:
                    yield DataMetadata(sid=sid, time=t)
                if xs:
                    last[sid] = max((t for t, _ in xs)).decode('utf-8')

    @strawberry.subscription
    async def all_data(
        self, streams: dict[str, str], count=1, block=True
    ) -> AsyncGenerator[DataPayload, None]:

        last = streams
        while True:
            result = await ctx.r.xread(last, count=count, block=block)
            for sid, xs in result:
                for t, d in xs:
                    yield DataPayload(sid=sid, time=t, data=d)
                if xs:
                    last[sid] = max((t for t, _ in xs)).decode('utf-8')

    @strawberry.subscription
    async def latest_data(
        self, streams: dict[str, str], count=1, block=True
    ) -> AsyncGenerator[DataPayload, None]:

        now = utils.format_epoch_time(time.time())
        last = {sid: t for sid, t in streams}
        while True:
            async with ctx.r.pipeline() as p:
                for sid, last_time in last.items():
                    if last_time != '-':  # query using the timestamp exclusively
                        last_time = f'({last_time}'
                    p.xrevrange(sid, '+', last_time, count=count)

                for sid, xs in zip(last, await p.execute()):
                    for t, d in xs:
                        yield DataPayload(sid=sid, time=t, data=d)
                    if xs:
                        last[sid] = max((t for t, _ in xs)).decode('utf-8')




schema = strawberry.Schema(
    Streams, 
    mutation=Mutation, 
    subscription=DataSubscription)