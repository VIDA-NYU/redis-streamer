from __future__ import annotations
import typing
import asyncio
# import ray
import strawberry
from strawberry.scalars import JSON, Base64
from strawberry_sqlalchemy_mapper import StrawberrySQLAlchemyMapper
from redis_streamer.config import *
from redis_streamer.models import session, RecordingModel, get_recording, create_recording, end_recording, rename_recording, delete_recording
from .. import recorder

strawberry_sqlalchemy_mapper = StrawberrySQLAlchemyMapper()

# ---------------------------------------------------------------------------- #
#                                    Queries                                   #
# ---------------------------------------------------------------------------- #

@strawberry.type
@strawberry_sqlalchemy_mapper.type(RecordingModel)
class Recording:
    pass

@strawberry.type
class Recordings:
    @strawberry.field
    def recordings(self) -> typing.List[Recording]:
        return session.query(RecordingModel).all()

    @strawberry.field
    def recording(self, name: strawberry.ID) -> Recording:
        return session.query(RecordingModel).get(name)
    
    @strawberry.field
    async def current_recording(self) -> str|None:
        return await writer.current_recording()


# writer = recorder.RecordingWriter.remote()
writer = recorder.RecordingWriter()

@strawberry.type
class RecordingMutation:
    @strawberry.mutation
    async def start_recording(self, name: str='') -> JSON:
        name = create_recording(name)
        await writer.start(name)
        return get_recording(name).as_dict()

    @strawberry.mutation
    async def stop_recording(self) -> JSON:
        # name = ray.get(writer.get_current_recording_name.remote())
        name = await writer.current_recording()
        print(name)
        if not name:
            raise RuntimeError("No recording running")
        print('before stop')
        await writer.stop()
        print('after stop')
        end_recording(name)
        return get_recording(name).as_dict()

    @strawberry.mutation
    async def rename_recording(self, name: str, new_name: str) -> JSON:
        rename_recording(name, new_name)
        return get_recording(new_name).as_dict()

    @strawberry.mutation
    async def delete_recording(self, name: str) -> None:
        delete_recording(name)
        return 