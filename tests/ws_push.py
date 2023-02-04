import asyncio
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
    entries = bytearray()
    offsets = []
    for i in range(5):
        offsets.append(len(entries))
        entries += f'Entry {i}'.encode()    
    async with websockets.connect(f'ws://{ADDRESS}/data/dev0/push?batch=true&ack=true', extra_headers=headers) as websocket:
        await websocket.send(','.join(map(str,offsets)))
        await websocket.send(entries)
        ack = await websocket.recv()
        print(ack)

asyncio.run(start())
