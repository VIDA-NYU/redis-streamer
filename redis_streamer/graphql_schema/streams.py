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
from redis_streamer.config import *





Utf8 = strawberry.scalar(
   typing.NewType("Utf8", bytes),
    serialize=lambda v: utils.maybe_decode(v),
    parse_value=lambda v: v.encode("utf-8"),
)



# ---------------------------------------------------------------------------- #
#                                    Queries                                   #
# ---------------------------------------------------------------------------- #



# --------------------------------- Resolvers -------------------------------- #

async def get_stream_ids(match: str|None=None, search_meta: bool=False, prefix: str='') -> list[str]:
    if prefix:
        match = f'{prefix}{match or "*"}'
    keys = {x.decode('utf-8') async for x in ctx.r.scan_iter(match=match or None, _type='stream')}
    if search_meta:
        meta_prefix = f'{STREAM_META_PREFIX}:'
        keys.update({
            k[len(meta_prefix):].decode('utf-8') 
            # async for k in ctx.r.scan_iter(f'{meta_prefix}*', _type='hash')
            async for k in ctx.r.scan_iter(f'{meta_prefix}*', _type='string')
        })
    if prefix:
        keys = {k[len(prefix):] for k in keys}
    return sorted(keys)

async def get_streams(ids: list[str]|None=None, match: str|None=None, prefix: str='') -> list[Stream]:
    # get list of stream IDs
    ids = ids or await get_stream_ids(match=match, search_meta=True, prefix=prefix)
    # query stream info and meta
    async with ctx.r.pipeline() as p:
        for sid in ids:
            # p.hgetall(f'{STREAM_META_PREFIX}:{prefix or ""}{sid}').xinfo_stream(f'{prefix or ""}{sid}')
            p.get(f'{STREAM_META_PREFIX}:{prefix or ""}{sid}').xinfo_stream(f'{prefix or ""}{sid}')
        res = await p.execute(raise_on_error=False)
    # create stream objects
    return [
        Stream.from_info_meta(sid, info, orjson.loads(meta) if isinstance(meta, bytes) else meta)
        for sid, meta, info in zip(ids, res[::2], res[1::2])
    ]

async def get_stream(id: str) -> Stream:
    # query stream info and meta
    async with ctx.r.pipeline() as pipe:
        # pipe.hgetall(f'{STREAM_META_PREFIX}:{id}').xinfo_stream(id)
        pipe.get(f'{STREAM_META_PREFIX}:{id}').xinfo_stream(id)
        meta, info = await pipe.execute(raise_on_error=False)
    meta = orjson.loads(meta) if isinstance(meta, bytes) else meta
    # create stream object
    return Stream.from_info_meta(id, info, meta)


# ------------------------------- Schema Types ------------------------------- #

@strawberry.type
class Stream:
    id: str
    first_entry_id: Utf8=''
    last_entry_id: Utf8=''

    first_entry_data_bytes: strawberry.Private[dict[bytes, Base64]|None]=None
    last_entry_data_bytes: strawberry.Private[dict[bytes, Base64]|None]=None

    last_generated_id: Utf8=''
    recorded_first_entry_id: Utf8=''
    max_deleted_entry_id: Utf8=''

    radix_tree_keys: int=0
    radix_tree_nodes: int=0

    groups: int=0
    length: int=0
    entries_added: int=0

    error: str=''
    meta: JSON

    @strawberry.field(description="The first timestamp in iso format")
    def first_entry_time(self, format: str="") -> str:
        return utils.format_iso(self.first_entry_id, format) if self.first_entry_id else ''

    @strawberry.field(description="The last timestamp in iso format")
    def last_entry_time(self, format: str="") -> str:
        return utils.format_iso(self.last_entry_id, format) if self.last_entry_id else ''

    @strawberry.field(description="The first data point")
    def first_entry_data(self) -> Base64:
        return self.first_entry_data_bytes[b'd'] if self.first_entry_data_bytes else b''

    @strawberry.field(description="The last data point")
    def last_entry_data(self) -> Base64:
        return self.last_entry_data_bytes[b'd'] if self.last_entry_data_bytes else b''

    @strawberry.field(description="The last data point as a string")
    def first_entry_string(self) -> str:
        return self.first_entry_data_bytes[b'd'].decode('utf-8') if self.first_entry_data_bytes else ''

    @strawberry.field(description="The last data point as a string")
    def last_entry_string(self) -> str:
        return self.last_entry_data_bytes[b'd'].decode('utf-8') if self.last_entry_data_bytes else ''

    @strawberry.field(description="The first data point as json")
    def first_entry_json(self) -> JSON:
        return orjson.loads(self.first_entry_data_bytes[b'd']) if self.first_entry_data_bytes else {}

    @strawberry.field(description="The last data point as json")
    def last_entry_json(self) -> JSON:
        return orjson.loads(self.last_entry_data_bytes[b'd']) if self.last_entry_data_bytes else {}


    @classmethod
    def from_info_meta(cls, id, info, meta=None, data_format: str|None=None):
        if isinstance(info, Exception):
            info = {'error': str(info)}
        d = {'id': id, **({k.replace('-', '_'): v for k, v in (info or {}).items()})}
        if 'first_entry' in d:
            d['first_entry_id'], d['first_entry_data_bytes'] = d.pop('first_entry') or ('', None)
        if 'last_entry' in d:
            d['last_entry_id'], d['last_entry_data_bytes'] = d.pop('last_entry') or ('', None)
        meta = {'error': str(meta)} if isinstance(meta, Exception) else (meta or {})
        d['meta'] = {utils.maybe_decode(k): v for k, v in meta.items()}
        data_format = data_format or d['meta'].get('data_response_format') or DEFAULT_STREAM_FORMAT
        return STREAMS_BY_FORMAT[data_format.lower()](**d)


@strawberry.type
class JsonStream(Stream):
    @strawberry.field(description="The first data point as json")
    def first_entry_data(self) -> JSON:
        return orjson.loads(self.first_entry_data_bytes[b'd']) if self.first_entry_data_bytes else {}

    @strawberry.field(description="The last data point as json")
    def last_entry_data(self) -> JSON:
        return orjson.loads(self.last_entry_data_bytes[b'd']) if self.last_entry_data_bytes else {}


@strawberry.type
class StringStream(Stream):
    @strawberry.field(description="The last data point as a string")
    def first_entry_data(self, format: str='utf-8') -> str:
        return self.first_entry_data_bytes[b'd'].decode(format) if self.first_entry_data_bytes else ''

    @strawberry.field(description="The first data point as a string")
    def last_entry_data(self, format: str='utf-8') -> str:
        return self.last_entry_data_bytes[b'd'].decode(format) if self.last_entry_data_bytes else ''

DEFAULT_STREAM_FORMAT = 'base64'
STREAMS_BY_FORMAT = {
    'json': JsonStream,
    'string': StringStream,
    DEFAULT_STREAM_FORMAT: Stream,
}

@strawberry.type
class Streams:
    streamIds: list[str] = strawberry.field(resolver=get_stream_ids)
    streams: list[Stream] = strawberry.field(resolver=get_streams)
    stream: Stream = strawberry.field(resolver=get_stream)



# ----------------------------------- Utils ---------------------------------- #

def decode_dict(data, utf8=False):
    return {
        k.decode('utf-8'): (v if utf8 else base64.b64encode(v)).decode('utf-8')
        for k, v in data.items()
    }


# ---------------------------------------------------------------------------- #
#                                   Mutations                                  #
# ---------------------------------------------------------------------------- #

async def update_stream_meta(stream_id: str, meta: JSON, device_id: str=DEFAULT_DEVICE, update: bool=False) -> dict[str, int]:
    if ENABLE_MULTI_DEVICE_PREFIXING:
        stream_id = f'{device_id or DEFAULT_DEVICE}:{stream_id}'
    # return await ctx.r.hset(f'{STREAM_META_PREFIX}:{sid}', mapping=meta)
    if update:
        previous = await ctx.r.get(stream_id)
        if previous:
            meta = {**orjson.loads(previous), **meta}
    return {
        "meta_set": await ctx.r.set(f'{STREAM_META_PREFIX}:{stream_id}', orjson.dumps(meta))
    }

async def delete_stream(stream_id: str, device_id: str=DEFAULT_DEVICE) -> dict[str, bool]:
    # return await ctx.r.xdel(f'{STREAM_META_PREFIX}:{sid}', mapping=meta)
    async with ctx.r.pipeline() as p:
        if ENABLE_MULTI_DEVICE_PREFIXING:
            stream_id = f'{device_id or DEFAULT_DEVICE}:{stream_id}'
        p.xtrim(stream_id, 0, approximate=False)
        p.delete(f'{STREAM_META_PREFIX}:{stream_id}')
        p.delete(stream_id)
        return dict(zip(
            ['data_deleted', 'meta_deleted', 'stream_deleted'],
            map(bool, await p.execute(raise_on_error=False))
        ))


@strawberry.type
class StreamMutation:
    @strawberry.mutation
    async def update_stream_meta(self, stream_id: str, meta: JSON, device_id: str=DEFAULT_DEVICE, update: bool=False) -> JSON:
        return await update_stream_meta(stream_id, meta, device_id, update=update)

    @strawberry.mutation
    async def delete_stream(self, stream_id: str, device_id: str=DEFAULT_DEVICE) -> JSON:
        return await delete_stream(stream_id, device_id)


# ---------------------------------------------------------------------------- #
#                                 Subscriptions                                #
# ---------------------------------------------------------------------------- #


@strawberry.type
class StreamSubscription:
    @strawberry.subscription
    async def streams(
        self, stream_ids: JSON, device: str|None, count: int=0, block: int=5000,
        latest: bool=False,
    ) -> AsyncGenerator[DataPayloads|DataPayload, None]:
        agent = Agent()
        cursor = agent.init_cursor(stream_ids, prefix=f'{device}:')
        while True:
            result, cursor = await agent.read(cursor, count=count or 1, block=block, latest=latest)
            for sid, xs in result:
                if count:
                    ts, xs = list(zip(*xs)) or ((),())
                    yield DataPayloads(stream_id=sid, time=list(ts), data=[x[b'd'] for x in xs])
                else:
                    for t, x in xs:
                        yield DataPayload(stream_id=sid, time=t, data=x[b'd'])
                        # , meta={k.decode('utf-8'): v.decode('utf-8') for k, v in x.items() if k != b'd'}



# -------------------------------- Data Types -------------------------------- #

@strawberry.type
class DataMetadata:
    stream_id: Utf8
    time: Utf8


@strawberry.type
class DataPayload:
    stream_id: Utf8
    time: Utf8
    data: Base64

    @strawberry.field
    def string(self, format: str='utf-8') -> str:
        return self.data.decode(format)

    @strawberry.field
    def json(self) -> JSON:
        return orjson.loads(self.data)


@strawberry.type
class DataPayloads:
    stream_id: Utf8
    time: list[Utf8]
    data: list[Base64]

    @strawberry.field
    def string(self, format: str='utf-8') -> list[str]:
        return [x.decode(format) for x in self.data]

    @strawberry.field
    def json(self) -> list[JSON]:
        return [orjson.loads(x) for x in self.data]
