from __future__ import annotations
import typing
import asyncio
from fastapi import Request
# import ray
import strawberry
from strawberry.scalars import JSON, Base64
from strawberry_sqlalchemy_mapper import StrawberrySQLAlchemyMapper
from redis_streamer.config import *
from redis_streamer.models import (
    session, RecordingModel, 
    get_recording, get_last_recording,
    create_recording, end_recording, rename_recording, 
    delete_recording, delete_all_recordings)
from ..recorder import RecordingWriter

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
        return await RecordingWriter.current_recording()


# writer = recorder.RecordingWriter.remote()
# Writer = recorder.RecordingWriter

@strawberry.type
class RecordingMutation:
    @strawberry.mutation
    async def start_recording(self, name: str='') -> JSON:
        name = create_recording(name)
        await RecordingWriter.start(name)
        return get_recording(name).as_dict()

    @strawberry.mutation
    async def stop_recording(self) -> JSON:
        # name = ray.get(writer.get_current_recording_name.remote())
        name = await RecordingWriter.current_recording()
        if not name:
            raise RuntimeError("No recording running")
        print('stop', name)
        await RecordingWriter.stop()
        print('after stop')
        end_recording(name)
        return get_recording(name).as_dict()

    @strawberry.mutation
    async def rename_recording(self, name: str, previous_name: str='') -> JSON:
        if not previous_name:
            previous_name = get_last_recording().name

        print('rename', previous_name, '->', name)
        rename_recording(previous_name, name)
        return get_recording(name).as_dict()

    @strawberry.mutation
    async def delete_recording(self, name: str) -> None:
        print('delete', name)
        delete_recording(name)
        return 
    
    @strawberry.mutation
    async def delete_all_recordings(self) -> None:
        delete_all_recordings(confirm=True)
        return 
    
    