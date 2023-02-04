import io
import time
import tqdm
import json
import asyncio
import functools
import websockets
import requests
from urllib.parse import urlencode
from PIL import Image


class async2sync:
    '''Helper to have a method be both sync and async.'''
    def __init__(self, func_async):
        self.func_async = func_async
        functools.update_wrapper(self, func_async)

    def __get__(self, inst, own):
        return self.__class__(self.func_async.__get__(inst, own))

    def __call__(self, *a, **kw):
        return asyncio.run(self.func_async(*a, **kw))

    async def asyncio(self, *a, **kw):
        return await self.func_async(*a, **kw)

    


class API:
    def __init__(self, url='http://localhost:8000'):
        self._url = url
        protocol, base = url.split('://', 1)
        self._wsurl = f'ws://{base}'
        self.sess = requests.Session()

    def url(self, url: str, isws=False, **params):
        params_str = urlencode({k: v for k, v in params.items() if k and v is not None}) if params else ''
        params_str = f'?{params_str}' if params_str else ''
        url = f"{self._wsurl if isws else self._url}/{url.rstrip('/')}{params_str}"
        print(url)
        return url
    
    def wsurl(self, url: str, **params):
        return self.url(url, isws=True, **params)

    def get(self, t):
        self.sess.get(f'{self.url}')

    def ls(self):
        return self.sess.post(self.url('graphql'), json={
            'query': '''{
                streams {
                    sid,
                    firstEntry,
                    lastEntry,
                    firstEntryTime,
                    lastEntryTime,
                }
            }'''
        }).content

    @async2sync
    async def push_increment(self, sid, max_value=1000, **kw):
        async with self.push_connect_async(sid, **kw) as ws:
            for i in range(max_value):
                await ws.send_data(json.dumps(i).encode('utf-8'))

    @async2sync
    async def pull_raw(self, sid, max_value=1000, **kw):
        async with self.pull_connect_async(sid, **kw) as ws:
            while True:
                entries = await ws.recv_data()
                for sid, t, data in entries:
                    tqdm.tqdm.write(f'{sid}: {t} | {data}')

    @async2sync
    async def push_image(self, sid, shape=(100, 100, 3), fps=100, **kw):
        import numpy as np
        t0 = time.time()
        X = np.random.randint(0, 255, size=shape).astype('uint8')
        buf = io.BytesIO()
        Image.fromarray(X).save(buf, format='jpeg')
        X = buf.getvalue()
        async with self.push_connect_async(sid, **kw) as ws:
            while True:
                await ws.send_data(X)
                if fps:
                    await asyncio.sleep(1/fps)#-(time.time() - t0)
                    t0 = time.time()

    @async2sync
    async def pull_image(self, sid, **kw):
        import cv2
        import numpy as np
        async with self.pull_connect_async(sid, **kw) as ws:
            while True:
                entries = await ws.recv_data()
                for sid, t, data in entries:
                    im = np.array(Image.open(io.BytesIO(data)))
                    # im = np.load(io.BytesIO(data)).astype('uint8')
                    cv2.imshow(sid, im)
                    cv2.waitKey(1)

    # @async2sync
    # async def pull(self, sid, last=None, **kw):
    #     asyncio.run(self.pull_async(sid, last=last, **kw))

    # async def push_async(self, sid, max_value=1000, fps=10):
    #     return WebsocketStream(self.wsurl(f'data/{sid}/push'))

    
    def push_connect_async(self, sid, **kw):
        return WebsocketStream(self.wsurl(f'data/{sid}/push', **kw))

    def pull_connect_async(self, sid, last=None, **kw):
        return WebsocketStream(self.wsurl(f'data/{sid}/pull', last=last, **kw))


class WebsocketStream:
    _pbar = None
    def __init__(self, url, show_pbar=True, **kw) -> None:
        self.url = url
        self.kw = kw
        self.show_pbar = show_pbar
        kw.setdefault('close_timeout', 10)
        kw.setdefault('max_size', 2**24)

    async def __aenter__(self):
        self.connector = websockets.connect(self.url)
        self.ws = await self.connector.__aenter__()  # type: ignore
        return self

    async def __aexit__(self, *a):
        await self.connector.__aexit__(*a)
        if self._pbar is not None:
            self._pbar.close()
            self._pbar = None
        return 

    async def recv_data(self):
        if self.show_pbar:
            if self._pbar is None:
                self._pbar = tqdm.tqdm()
            self._pbar.update()

        offsets = json.loads(await self.ws.recv())
        entries = await self.ws.recv()
        await asyncio.sleep(0)

        sids, ts, offsets = tuple(zip(*offsets)) or ((),)*3
        return [
            (sid, t, entries[start:end])
            for sid, t, start, end in zip(sids, ts, offsets, offsets[1:] + (None,))
        ]

    async def send_data(self, data):
        await self.ws.send(json.dumps([len(data)]))
        await self.ws.send(data)
        await asyncio.sleep(0)

        if self.show_pbar:
            if self._pbar is None:
                self._pbar = tqdm.tqdm()
            self._pbar.update()



if __name__ == '__main__':
    import fire
    fire.Fire(API)