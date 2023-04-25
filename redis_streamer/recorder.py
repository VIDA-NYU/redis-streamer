import os
import tqdm
import asyncio
# import ray
from multiprocessing import Event
from concurrent.futures import ProcessPoolExecutor
import datetime
from .core import ctx, Agent
from .models import session, RecordingModel, create_recording, end_recording
from .graphql_schema.streams import get_stream_ids

# ray.init()

PREFIX = ':recording:current'
RECORDING_NAME = f'{PREFIX}:name'

class RecordingWriter:
    def __init__(self) -> None:
        self.pool = ProcessPoolExecutor()

    async def current_recording(self):
        return (await ctx.r.get(RECORDING_NAME) or b'').decode('utf-8') or None

    async def start(self, name):
        current_name = await self.current_recording()
        if current_name:
            raise RuntimeError(f"already recording {current_name}")
        await ctx.r.set(RECORDING_NAME, name)
        self.pool.submit(self._record, name, RECORDING_NAME)
        print("submitteds")

    async def stop(self):
        current_name = await self.current_recording()
        print('stop')
        if not current_name:
            raise RuntimeError("not recording")
        print('before')
        await ctx.r.delete(RECORDING_NAME)
        print('after')

    @classmethod
    def _record(cls, name: str, key: str):
        return asyncio.run(cls._record_async(name, key))

    @classmethod
    async def _record_async(cls, name: str, key: str):
        name = name.encode()
        print("started")
        while await ctx.r.get(key) == name:
            print(f'recording {name} :)')
            await asyncio.sleep(1)
        print("done")



# @ray.remote
# class RecordingWriter:
#     def __init__(self):
#         self.current_recording = None
#         self.last_recording = None

#     def get_current_recording_name(self):
#         return self.current_recording

#     async def record(self, name, prefix='', last_entry_id="$", batch=10):
#         self.current_recording = name
#         self.stopped = False
#         while not self.stopped:
#             print(f'recording {self.current_recording} :)')
#             await asyncio.sleep(1)
#         print("done")

#         # agent = Agent()
#         # cursor = agent.init_cursor({s: last_entry_id for s in await get_stream_ids(prefix=prefix)})
#         # try:
#         #     while not self.stopped:
#         #         # read data from redis
#         #         results, cursor = await agent.read(cursor, count=batch or 1)
#         #         for sid, xs in results:
#         #             for ts, data in xs:
#         #                 await writer.write(sid, data, ts)
#         # finally:
#         #     pass

#     def stop(self):
#         self.stopped = True
#         self.last_recording = self.current_recording
#         self.current_recording = False

#     def replay(self):
#         pass


class Writers:
    def __init__(self, cls):
        self.cls = cls
        self.writers = {}
        self.is_entered = False

    async def __aenter__(self):
        self.is_entered = True
        await asyncio.gather(*(w.__aenter__() for w in self.writers.values()))
        return self
    
    async def get_writer(self, sid):
        if sid not in self.writers:
            self.writers[sid] = w = self.cls(sid)
            if self.is_entered:
                await w.__aenter__()
        return self.writers[sid]
    
    async def write(self, sid, data, ts):
        writer = await self.get_writer(sid)
        await writer.write(data, ts)
    
    async def __aexit__(self, *a):
        await asyncio.gather(*(w.__aexit__(*a) for w in self.writers.values()))
        self.is_entered = False


MB = 1024*1024

class RawWriter:
    raw=True
    def __init__(self, name, store_dir='', max_len=1000, max_size=9.5*MB, **kw):
        super().__init__(**kw)
        #self.fname = os.path.join(store_dir, f'{name}.zip')
        self.dir = os.path.join(store_dir, name)
        os.makedirs(self.dir, exist_ok=True)
        self.name = name
        self.max_len = max_len
        self.max_size = max_size

    def context(self, sample=None, t_start=None):
        try:
            self.size = 0
            self.buffer = []
            with tqdm.tqdm(total=self.max_len, desc=self.name) as self.pbar:
                yield self
        finally:
            if self.buffer:
                self._dump(self.buffer)
                self.buffer.clear()

    def _dump(self, data):
        if not data:
            return
        import zipfile
        fname = os.path.join(self.dir, f'{data[0][1]}_{data[-1][1]}.zip')
        tqdm.tqdm.write(f"writing {fname}")
        with zipfile.ZipFile(fname, 'a', zipfile.ZIP_STORED, False) as zf:
            for d, ts in data:
                zf.writestr(ts, d)

    def write(self, data, ts):
        self.pbar.update()
        self.size += len(data)
        self.buffer.append([data, ts])
        if len(self.buffer) >= self.max_len or self.size >= self.max_size:
            self._dump(self.buffer)
            self.buffer.clear()
            self.pbar.reset()
            self.size = 0


