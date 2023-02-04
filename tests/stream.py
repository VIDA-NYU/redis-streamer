import os
import asyncio
import orjson
import requests
import websockets

session = requests.Session()

URI = 'https://eng-nrf233-01.engineering.nyu.edu/ptg/api'

token = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJwdGciLCJleHAiOjE2NTI1NDU3MTl9.8S6dal-Q97TfJZG7rYLQWe5n3IovU9qCmtPYo7voNN4'
headers = {'Authorization': f'Bearer {token}'}

#### STREAMS
def pull(uri='http://127.0.0.1:7890', stream='test+dev0', last_entry_id='1650811721065-1+*', limit=None):
    # r = session.post(url=f'http://{uri}/token',data={'username':'test', 'password':'test'})
    # assert(r.ok)
    # print('POST /token', r.content.decode('utf-8'))
    # token = r.json()['access_token']
    headers = {'Authorization': f'Bearer {token}'}

    wsurl = uri.replace('http://', 'ws://').replace('https://', 'wss://')

    async def start():
        i = 0
        print(f'{wsurl}/data/{stream}/pull?last_entry_id={last_entry_id}')
        async with websockets.connect(f'wss://{uri}/data/{stream}/pull?last_entry_id={last_entry_id}', extra_headers=headers) as websocket:
            with open('dump.bin', 'wb') as f:
                while True:
                    offsets = orjson.loads(await websocket.recv())
                    content = await websocket.recv()
                    f.write(content)
                    if limit and i > limit:
                        break
                    i+=1

    asyncio.run(start())


def get(uri='http://127.0.0.1:7890', stream='test+dev0', last_entry_id='1650811721065-1+*'):
    r = requests.get(f'{uri}/data/{stream}?last_entry_id={last_entry_id}', headers=headers)
    print(r.ok, r.status_code)
    print(r.headers)
    print(len(r.content))

def get_streams(uri='http://127.0.0.1:7890'):
    r = session.get(url=f'{uri}/streams', headers=headers)
    print(r.ok, r.status_code)
    print(r.headers)
    print(len(r.content))
    return r.json()


def dump(uri=URI, last_entry_id='1650811721065-1+*', n=10, outdir='dumps'):
    import tqdm
    r = session.get(url=f'{uri}/streams', headers=headers)
    streams = r.json()
    print(streams)
    assert isinstance(streams, list)
    os.makedirs(outdir, exist_ok=True)
    for stream in streams:
        os.makedirs(os.path.join(outdir, stream), exist_ok=True)
        print(stream)
        
        entry_id = last_entry_id
        pbar = tqdm.tqdm(range(n))
        for i in pbar:
            r = requests.get(f'{uri}/data/{stream}?last_entry_id={entry_id}', headers=headers)
            entry_id = orjson.loads(r.headers['entry-offset'])[0][1]
            fname = os.path.join(outdir, stream, f'{entry_id}.bin')
            with open(fname, 'wb') as f:
                f.write(r.content)
            pbar.write(fname)

if __name__ == '__main__':
    import fire
    fire.Fire()