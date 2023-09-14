from __future__ import annotations
import typing
import base64

import strawberry
from strawberry.scalars import JSON, Base64
import orjson
from redis_streamer import utils, ctx
from . import streams
from redis_streamer.config import *


Utf8 = strawberry.scalar(
   typing.NewType("Utf8", bytes),
    serialize=lambda v: utils.maybe_decode(v),
    parse_value=lambda v: v.encode("utf-8"),
)



# ---------------------------------------------------------------------------- #
#                                    Queries                                   #
# ---------------------------------------------------------------------------- #

@strawberry.type
class Device:
    id: str
    
    @strawberry.field
    async def meta(self) -> JSON:
        return orjson.loads((await ctx.r.get(f'{DEVICE_META_PREFIX}:{self.id}')) or "{}")

    @strawberry.field
    async def stream_ids(self) -> list[str]:
        return await streams.get_stream_ids(prefix=f'{self.id or DEFAULT_DEVICE}:')

    @strawberry.field
    async def streams(self) -> list[streams.Stream]:
        return await streams.get_streams(prefix=f'{self.id or DEFAULT_DEVICE}:')

    @strawberry.field
    async def stream(self, stream_id: str) -> streams.Stream:
        return await streams.get_stream(f'{self.id or DEFAULT_DEVICE}:{stream_id}')



@strawberry.type
class Devices:
    @strawberry.field
    async def devices(self, ids: list[str]|None=None, include_all: bool=False) -> list[Device]:
        return await get_devices(ids, include_all)

    @strawberry.field
    async def device(self, id: str=DEFAULT_DEVICE) -> Device:
        return Device(id=id)

async def get_devices(ids: list[str]|None=None, include_all: bool=False):
    if ids:
        return [Device(id=did) for did in ids]
    return await (get_all_devices() if include_all else get_connected_devices())

async def get_connected_devices():
    return [Device(id=x.decode('utf-8')) for x in await ctx.r.smembers(DEVICES_CONNECTED_KEY)]

async def get_all_devices():
    return [Device(id=x.decode('utf-8')) for x in await ctx.r.smembers(DEVICES_SEEN_KEY)]

# ---------------------------------------------------------------------------- #
#                                   Mutations                                  #
# ---------------------------------------------------------------------------- #

async def connect_device(device_id: str, meta: dict[str, typing.Any]) -> dict[str, int]:
    async with ctx.r.pipeline() as p:
        p.sadd(DEVICES_CONNECTED_KEY, device_id).sismember(DEVICES_CONNECTED_KEY, device_id)
        p.sadd(DEVICES_SEEN_KEY, device_id)
        p.set(f'{DEVICE_META_PREFIX}:{device_id}', orjson.dumps(meta or {}))
        p.xadd(f'{EVENT_PREFIX}:device.connected', {b'd': orjson.dumps({ "device_id": device_id, "connected": True, "meta": meta })})
        p.xadd(f'{EVENT_PREFIX}:device.meta', {b'd': orjson.dumps({ "device_id": device_id, "meta": meta })})
        return dict(zip(
            ['connection_status_changed', 'connected', 'is_new_device', 'meta_set', 'fired:device.connected', 'fired:device.meta'], 
            map(bool, await p.execute(raise_on_error=False))))

async def update_device_meta(device_id: str, meta: dict[str, typing.Any]) -> dict[str, int]:
    async with ctx.r.pipeline() as p:
        p.set(f'{DEVICE_META_PREFIX}:{device_id}', orjson.dumps(meta or {}))
        p.xadd(f'{EVENT_PREFIX}:device.meta', {b'd': orjson.dumps({ "device_id": device_id, "meta": meta })})
        return dict(zip(
            ['meta_set', 'fired:device.meta'], 
            map(bool, await p.execute(raise_on_error=False))))

async def disconnect_device(device_id: str) -> dict[str, int]:
    async with ctx.r.pipeline() as p:
        p.rem(DEVICES_CONNECTED_KEY, device_id).sismember(DEVICES_CONNECTED_KEY, device_id)
        p.xadd(f'{EVENT_PREFIX}:device.connected', {b'd': orjson.dumps({ "device_id": device_id, "connected": False })})
        return dict(zip(
            ['connection_status_changed', 'connected', 'fired:device.connected'], 
            map(bool, await p.execute(raise_on_error=False))))

@strawberry.type
class DeviceMutation:
    @strawberry.mutation
    async def connect_device(self, device_id: str, meta: JSON) -> JSON:
        return await connect_device(device_id, meta)

    @strawberry.mutation
    async def update_device_meta(self, device_id: str, meta: JSON) -> JSON:
        return await update_device_meta(device_id, meta)

    @strawberry.mutation
    async def disconnect_device(self, device_id: str) -> JSON:
        return await disconnect_device(device_id)
