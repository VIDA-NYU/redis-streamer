import os
import tqdm
import asyncio
import ray
import datetime
from .core import ctx, Agent
from .models import session, RecordingModel
from .graphql_schema.streams import get_stream_ids

ray.init()


@ray.remote
class Recorder:
    def __init__(self):
        pass

    async def record(self, name, prefix='', last_entry_id="$", batch=1, block=5000):
        self.stopped = False

        session.add(RecordingModel(name=name, start_time=datetime.datetime.now()))
        session.commit()
        rec_entry = session.query(RecordingModel).filter(RecordingModel.name == name).order_by(RecordingModel.start_time).last()

        agent = Agent()
        stream_ids = await get_stream_ids()
        cursor = agent.init_cursor({s: last_entry_id for s in stream_ids})
        try:
            while not self.stopped:
                # read data from redis
                results, cursor = await agent.read(cursor, latest=False, count=batch or 1, block=block)
                for sid, xs in results:
                    for ts, data in xs:
                        await writer[sid].write(data, ts)
        finally:
            rec_entry.end_time = datetime.datetime.now()
            session.commit()

    def stop(self):
        self.stopped = True

    def replay(self):
        pass


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


