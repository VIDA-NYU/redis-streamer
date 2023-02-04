import asyncio
import orjson
import requests
import websockets

ADDRESS = '127.0.0.1:7890'
session = requests.Session()

#### STREAMS
r = session.post(url=f'http://{ADDRESS}/token',data={'username':'test', 'password':'test'})
assert(r.ok)
print('POST /token', r.content.decode('utf-8'))
token = r.json()['access_token']
headers = {'Authorization': f'Bearer {token}'}

async def start():
    async with websockets.connect(f'ws://{ADDRESS}/data/test+dev0/pull?last_entry_id=1650811721065-1+*', extra_headers=headers) as websocket:
        while True:
            offsets = orjson.loads(await websocket.recv())
            content = await websocket.recv()
            print(offsets, len(content))

asyncio.run(start())
