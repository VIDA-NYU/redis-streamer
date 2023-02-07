import asyncio
from typing import List
from collections import defaultdict
from contextlib import asynccontextmanager
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse

app = FastAPI()



class OneToManyConnectionManager:
    def __init__(self):
        self.active_server: WebSocket|None = None
        self.active_clients: List[WebSocket] = []
        self._server_connect_lock = asyncio.Event()

    @asynccontextmanager
    async def connect_client(self, websocket: WebSocket):
        try:
            await websocket.accept()
            self.active_clients.append(websocket)
            yield self
        except WebSocketDisconnect:
            self.active_clients.remove(websocket)

    @asynccontextmanager
    async def connect_server(self, websocket: WebSocket):
        try:
            if self.active_server is not None:
                await self.active_server.close()
            await websocket.accept()
            self.active_server = websocket
            self._server_connect_lock.set()
            yield self
        except WebSocketDisconnect:
            pass
        finally:
            self._server_connect_lock.clear()
            self.active_server = None

    async def receive_client_bytes(self):
        pass

    async def send_clients_bytes(self, message: bytes):
        for connection in self.active_clients:
            await connection.send_bytes(message)

    async def send_server_bytes(self, message: bytes):
        await self._server_connect_lock.wait()
        await self.active_server.send_bytes(message)

managers = defaultdict(lambda: OneToManyConnectionManager())


@app.websocket("/direct/{client_id}/server")
async def server(ws: WebSocket, client_id: int):
    m = managers[client_id]
    async with m.connect_server(ws) as m:
        await ws.send_bytes(await m.receive_client_bytes())
        while True:
            await m.send_clients_bytes(await ws.receive_bytes())


@app.websocket("/direct/{client_id}/client")
async def client(ws: WebSocket, client_id: int):
    m = managers[client_id]
    async with m.connect_client(ws) as m:
        await m.send_server_bytes(await ws.receive_bytes())
        while True:
            await ws.send_bytes(await m.receive_client_bytes())

