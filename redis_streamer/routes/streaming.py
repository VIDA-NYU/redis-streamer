'''
'''
import os
from fastapi import APIRouter, Query, Path
from fastapi.responses import StreamingResponse

from ..core import Agent

app = APIRouter()


@app.get('/{stream_id}', summary='Stream data', response_class=StreamingResponse)
async def streaming_data(
        stream_id: str=Path(None, description='The ID of the stream'),
        last_entry_id: str=Query('$', description="Start retrieving entries later than the provided ID"),
        latest: bool=Query(False, description='Should we allow frame skipping? Ok for some data, not for others.')
    ):
    """Raw format streaming.
    TODO: handle data with headers.
    """
    agent = Agent()
    stream_id, dtype = os.path.splitext(stream_id)
    return StreamingResponse(
        (data async for sid, t, data in agent.iread(stream_id, last_entry_id, latest=latest)), 
        media_type=MIMETYPES.get(dtype.strip('.').lower(), DEFAULT))

DEFAULT = 'application/octet-stream'
MIMETYPES = {
    'mjpeg': "multipart/x-mixed-replace;boundary=frame",
}