from __future__ import annotations
import typing
import base64

import strawberry
from strawberry.scalars import JSON, Base64
from strawberry_sqlalchemy_mapper import strawberry_dataclass_from_model
import orjson
from redis_streamer import utils, ctx
from . import streams
from redis_streamer.config import *
from redis_streamer.models import session, RecordingModel


# ---------------------------------------------------------------------------- #
#                                    Queries                                   #
# ---------------------------------------------------------------------------- #

@strawberry.type
@strawberry_dataclass_from_model(RecordingModel)
class Recording:
    pass

@strawberry.type
class RecordingsQuery:
    def recordings(self) -> typing.List[Recording]:
        return session.query(RecordingModel).all()

    @strawberry.field
    def recording(self, id: strawberry.ID) -> Recording:
        return session.query(RecordingModel).get(id)


@strawberry.type
class RecordingMutation:
    @strawberry.mutation
    async def start(self, device_id: str, meta: JSON) -> JSON:
        return

    @strawberry.mutation
    async def stop(self, device_id: str, meta: JSON) -> JSON:
        return

    @strawberry.mutation
    async def rename(self, device_id: str) -> JSON:
        return await disconnect_device(device_id)
