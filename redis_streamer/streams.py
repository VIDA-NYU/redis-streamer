from __future__ import annotations
import os
import time
import typing
from typing import AsyncGenerator
import base64

import strawberry
from strawberry.scalars import JSON, Base64
import asyncio
import orjson
from redis import asyncio as aioredis
from redis_streamer import utils, ctx, Agent



META_PREFIX = 'XMETA'


Utf8 = strawberry.scalar(
   typing.NewType("Utf8", bytes),
    serialize=lambda v: utils.maybe_decode(v),
    parse_value=lambda v: v.encode("utf-8"),
)



# ---------------------------------------------------------------------------- #
#                                    Queries                                   #
# ---------------------------------------------------------------------------- #

@strawberry.type
class Stream:
    id: str
    first_entry_id: Utf8=strawberry.UNSET
    last_entry_id: Utf8=strawberry.UNSET

    first_entry_data_bytes: strawberry.Private[dict[bytes, bytes]]
    last_entry_data_bytes: strawberry.Private[dict[bytes, bytes]]

    last_generated_id: Utf8=strawberry.UNSET
    recorded_first_entry_id: Utf8=strawberry.UNSET
    max_deleted_entry_id: Utf8=strawberry.UNSET

    radix_tree_keys: int=strawberry.UNSET
    radix_tree_nodes: int=strawberry.UNSET

    groups: int=strawberry.UNSET
    length: int=strawberry.UNSET
    entries_added: int=strawberry.UNSET

    error: str=strawberry.UNSET
    meta: JSON

    @strawberry.field
    def first_entry_time(self, format: str="") -> str:
        return utils.format_iso(self.first_entry_id, format) if self.first_entry_id else strawberry.UNSET

    @strawberry.field
    def last_entry_time(self, format: str="") -> str:
        return utils.format_iso(self.last_entry_id, format) if self.last_entry_id else strawberry.UNSET

    @strawberry.field
    def first_entry_data(self, utf8: bool=False) -> JSON:
        return decode_dict(self.first_entry_data_bytes, utf8)

    @strawberry.field
    def last_entry_data(self, utf8: bool=False) -> JSON:
        return decode_dict(self.last_entry_data_bytes, utf8)

    @classmethod
    def from_info_meta(cls, sid, info, meta=None):
        if isinstance(info, Exception):
            info = {'error': str(info)}
        d = {'id': sid, **({k.replace('-', '_'): v for k, v in (info or {}).items()})}
        if d.get('first_entry'):
            d['first_entry_id'], d['first_entry_data_bytes'] = d.pop('first_entry')
        if d.get('last_entry'):
            d['last_entry_id'], d['last_entry_data_bytes'] = d.pop('last_entry')
        d['meta'] = {'error': str(meta)} if isinstance(meta, Exception) else (meta or {})
        return cls(**d)




@strawberry.type
class Streams:
    @strawberry.field
    async def sids(self, search_meta: bool=False) -> list[str]:
        keys = {x.decode('utf-8') async for x in ctx.r.scan_iter(_type='stream')}
        if search_meta:
            keys.update({k[len(META_PREFIX)+1:].decode('utf-8') for k in await ctx.r.keys(f'{META_PREFIX}:*')})
        return sorted(keys)

    @strawberry.field
    async def streams(self, sids: list[str]|None=None) -> list[Stream]:
        sids = sids or await Streams.ids(self, search_meta=True)
        async with ctx.r.pipeline() as pipe:
            for sid in sids:
                pipe.get(f'{META_PREFIX}:{sid}').xinfo_stream(sid)
            res = await pipe.execute(raise_on_error=False)
            return [
                Stream.from_info_meta(sid, info, orjson.loads(meta) if isinstance(meta, bytes) else meta)
                for sid, meta, info in zip(sids, res[::2], res[1::2])
            ]



def decode_dict(data, utf8=False):
    return {
        k.decode('utf-8'): (v if utf8 else base64.b64encode(v)).decode('utf-8')
        for k, v in data.items()
    }



# ---------------------------------------------------------------------------- #
#                                   Mutations                                  #
# ---------------------------------------------------------------------------- #


@strawberry.type
class Mutation:
    @strawberry.mutation
    async def stream_meta(self, sid: str, meta: JSON, *, update: bool=False) -> int:
        key = f'{META_PREFIX}:{sid}'
        if update:
            previous = await ctx.r.get(key)
            if previous:
                meta = {**orjson.loads(previous), **meta}
        return await ctx.r.set(key, orjson.dumps({**meta}))



# ---------------------------------------------------------------------------- #
#                                 Subscriptions                                #
# ---------------------------------------------------------------------------- #


@strawberry.type
class DataSubscription:
    # @strawberry.subscription
    # async def timestamps(
    #     self, streams: JSON, count: int=1, block: int|None=None
    # ) -> AsyncGenerator[DataMetadata, None]:
    #     agent = Agent()
    #     cursor = agent.init_cursor(streams)
    #     while True:
    #         result, cursor = await agent.read(cursor, count=count, block=block)
    #         for sid, xs in result:
    #             ts, xs = zip(*xs) or ((),())
    #             yield DataMetadata(sid=sid, time=ts)

    @strawberry.subscription
    async def data(
        self, sids: JSON, count: int=1, block: int=5000,
        latest: bool=False,
    ) -> AsyncGenerator[DataPayloads, None]:
        agent = Agent()
        cursor = agent.init_cursor(sids)
        while True:
            result, cursor = await agent.read(cursor, count=count, block=block, latest=latest)
            for sid, xs in result:
                ts, xs = list(zip(*xs)) or ((),())
                yield DataPayloads(sid=sid, time=list(ts), data=[x[b'd'] for x in xs])



# -------------------------------- Data Types -------------------------------- #

@strawberry.type
class DataMetadata:
    sid: Utf8
    time: Utf8


@strawberry.type
class DataPayload:
    sid: Utf8
    time: Utf8
    data: Base64

    @strawberry.field
    def utf8(self) -> str:
        return self.data.decode('utf-8')

    @strawberry.field
    def json(self) -> JSON:
        return orjson.loads(self.data)


@strawberry.type
class DataPayloads:
    sid: Utf8
    time: list[Utf8]
    data: list[Base64]

    @strawberry.field
    def utf8(self) -> list[str]:
        return [x.decode('utf-8') for x in self.data]

    @strawberry.field
    def json(self) -> list[JSON]:
        return [orjson.loads(x) for x in self.data]




# ---------------------------------------------------------------------------- #
#                                    Schema                                    #
# ---------------------------------------------------------------------------- #


schema = strawberry.Schema(
    Streams, 
    mutation=Mutation, 
    subscription=DataSubscription)