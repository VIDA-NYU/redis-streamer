import io
import time
import tqdm
import shlex
import json
import asyncio
import functools
import websockets
import requests
from urllib.parse import urlencode

import cv2
import numpy as np
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
    '''Query the Redis Streamer API from both Python and the command line.
    '''
    def __init__(self, url='http://localhost:8000'):
        self._url = url
        base = url.split('://', 1)[-1]
        self._wsurl = f'ws://{base}'
        self.sess = requests.Session()

    def asurl(self, url: str, isws=False, **params):
        '''Helper to form a URL.'''
        # create url query string
        params_str = urlencode({k: v for k, v in params.items() if k and v is not None}) if params else ''
        params_str = f'?{params_str}' if params_str else ''

        url = f"{self._wsurl if isws else self._url}/{url.rstrip('/')}{params_str}"
        print(url)
        return url

    def graphql(self, query, vars=None):
        return self.sess.post(self.asurl('graphql'), json={ 'query': query, 'variables': vars or {} }).json()

    def ls(self, *fields):
        return self.graphql('''query GetStreams {
            streams {
                id
                firstEntryId
                lastEntryId
                firstEntryTime
                lastEntryTime
                ''' + '\n'.join(fields) + '''
            }
        }''', {'fields': fields})
    
    # def record(self, name):
    #     return self.cmd('set', 'RECORD:NAME', name)
    
    # def stop_record(self):
    #     return self.cmd('del', 'RECORD:NAME')

    # def xrecord(self, name):
    #     return self.cmd('XADD', 'XRECORD:NAME', "MAXLEN", "~", 100, '*', 'd', name)

    # def stop_xrecord(self):
    #     return self.cmd('XADD', 'XRECORD:NAME', "MAXLEN", "~", 100, '*', 'd', '')

    def cmd(self, *cmd):
        print(cmd)#/{shlex.join(map(str, cmd))}
        return self.sess.put(self.asurl('redis'), json={'cmd': cmd}).text
    
    def last(self, key):
        return self.cmd('xrevrange', key, '+', '-', 'COUNT', '1')

    # ---------------------------------------------------------------------------- #
    #                                Basic Streaming                               #
    # ---------------------------------------------------------------------------- #

    @async2sync
    async def push_increment(self, sid, max_value=1000, **kw):
        async with self.push_connect_async(sid, **kw) as ws:
            for i in range(max_value):
                await ws.send_data(json.dumps(i).encode('utf-8'))

    @async2sync
    async def pull_raw(self, sid, **kw):
        async with self.pull_connect_async(sid, **kw) as ws:
            while True:
                entries = await ws.recv_data()
                for sid, t, data in entries:
                    tqdm.tqdm.write(f'{sid}: {t} | {data}')


    # ---------------------------------------------------------------------------- #
    #                                Image Streaming                               #
    # ---------------------------------------------------------------------------- #

    @async2sync
    async def push_image(self, sid, size=700, shape=None, fps=None, **kw):
        # import pyinstrument
        # p=pyinstrument.Profiler()
        # try:
        #     with p:
        t0 = time.time()
        shape = shape or (size, size, 3)
        print(f"Pushing image with shape: {shape}")
        im = np.random.randint(0, 255, size=shape).astype('uint8')
        im = format_image(im)
        async with self.push_connect_async(sid, **kw) as ws:
            while True:
                await ws.send_data(im)
                if fps:
                    t1 = time.time()
                    await asyncio.sleep(max(0, 1/fps-(t1 - t0)))
                    t0 = t1
        # finally:
        #     p.print()

    @async2sync
    async def pull_image(self, sid, **kw):
        async with self.pull_connect_async(sid, **kw) as ws:
            while True:
                header, entries = await ws.recv_data()
                entries = unpack_entries(header, entries)
                for sid, t, data in entries:
                    im = load_image(data)
                    cv2.imshow(sid, im)
                    cv2.waitKey(1)

    @async2sync
    async def pull_video(self, sid, fps=30, **kw):
        import supervision as sv
        s = None
        try:
            async with self.pull_connect_async(sid, **kw) as ws:
                while True:
                    header, entries = await ws.recv_data()
                    entries = unpack_entries(header, entries)
                    for sid, t, data in entries:
                        im = load_image(data)
                        if s is None:
                            s = sv.VideoSink(f'{sid}.mp4', video_info=sv.VideoInfo(width=im.shape[1], height=im.shape[0], fps=fps))
                            s.__enter__()
                        s.write_frame(im)
        finally:
            if s is not None:
                s.__exit__(None,None,None)


    def push_image_rest(self, sid, shape=(100, 100, 3), fps=100, **kw):
        url = self.asurl(f'data/{sid}', **kw)
        print('POST', url)

        pbar = tqdm.tqdm()

        t0 = time.time()
        while True:
            im = np.random.randint(0, 255, size=shape).astype('uint8')
            im = format_image(im)
            r = requests.post(url, files=[('entries', (sid, im))])
            if r.status_code >= 500: # show internal server errors
                raise requests.HTTPError(r.text)
            r.raise_for_status()

            time.sleep(0)
            if fps:
                time.sleep(max(0, 1/fps-(time.time() - t0)))
                t0 = time.time()

            pbar.update()

    def pull_image_rest(self, sid, fps=None, last_entry_id='$', block=5000, **kw):
        if last_entry_id is True:
            last_entry_id = '-'  # python fire bug
        url = self.asurl(f'data/{sid}', last_entry_id=last_entry_id, block=block, **kw)
        print(url)
        t0 = time.time()

        pbar = tqdm.tqdm()
        
        while True:
            r = requests.get(self.asurl(f'data/{sid}', last_entry_id=last_entry_id, block=block, **kw))
            if r.status_code >= 500: # show internal server errors
                raise requests.HTTPError(r.text)
            r.raise_for_status()
            last_entry_id = r.headers['x-last-entry-id']
            header = json.loads(r.headers['x-offsets'])
            entries = r.content
            entries = unpack_entries(header, entries)
            for sid, t, data in entries:
                im = load_image(data)
                cv2.imshow(sid, im)
                cv2.waitKey(1)
            if not entries:
                print("No data available...")
            
            time.sleep(0)
            if fps:
                time.sleep(max(0, 1/fps-(time.time() - t0)))
                t0 = time.time()

            pbar.update()

    # ---------------------------------------------------------------------------- #
    #                          Websocket Context Managers                          #
    # ---------------------------------------------------------------------------- #
    
    def push_connect_async(self, sid, **kw):
        return WebsocketStream(self.asurl(f'data/{sid}/push', isws=True, **kw))

    def pull_connect_async(self, sid, last=None, **kw):
        return WebsocketStream(self.asurl(f'data/{sid}/pull', isws=True, last=last, **kw))


def format_image(im, format='jpeg'):
    buf = io.BytesIO()
    Image.fromarray(im).save(buf, format=format)
    return buf.getvalue()

def load_image(data):
    im = np.array(Image.open(io.BytesIO(data)))
    # im = np.load(io.BytesIO(data)).astype('uint8')
    return im


def unpack_entries(header: list[tuple[str, str, int]], entries: bytes) -> list[tuple[str, str, bytes]]:
    sids, ts, offsets = tuple(zip(*header)) or ((),)*3
    return [
        (sid, t, entries[start:end])
        for sid, t, start, end in zip(sids, ts, (0,) + offsets, offsets)
    ]

class WebsocketStream:
    _pbar = None
    def __init__(self, url, show_pbar=True, **kw) -> None:
        self.url = url
        self.kw = kw
        self.show_pbar = show_pbar
        kw.setdefault('close_timeout', 10)
        kw.setdefault('max_size', 2**24)

    async def __aenter__(self):
        print("Connecting to:", self.url)
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
        header = json.loads(await self.ws.recv())
        entries = await self.ws.recv()
        await asyncio.sleep(0)
        self.update_progress()
        return header, entries

    async def send_data(self, data):
        await self.ws.send(json.dumps([len(data)]))
        await self.ws.send(data)
        await asyncio.sleep(0)
        self.update_progress()

    def update_progress(self):
        if self.show_pbar:
            if self._pbar is None:
                self._pbar = tqdm.tqdm()
            self._pbar.update()


def main():
    import fire
    fire.Fire(API)

if __name__ == '__main__':
    main()