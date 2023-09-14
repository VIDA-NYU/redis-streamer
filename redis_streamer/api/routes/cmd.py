import shlex
from typing import Any
from fastapi import APIRouter
from pydantic import BaseModel
from redis_streamer import ctx

class Command(BaseModel):
    cmd: list[str|int|float]|str

app = APIRouter()

@app.put('/', summary='Send redis commands')
async def run_cmd(cmd: Command):
    """Send arbitrary redis commands"""
    cmd = cmd.cmd
    cmd = shlex.split(cmd) if isinstance(cmd, str) else cmd
    return await ctx.r.execute_command(*cmd)

@app.get('/{key}', summary='Get redis key')
async def run_set(key: str):
    """Set a key in redis"""
    return await ctx.r.execute_command("GET", key)

@app.put('/{key}/{value}', summary='Set redis key')
async def run_set(key: str, value: Any):
    """Set a key in redis"""
    return await ctx.r.execute_command("SET", key, value)

@app.delete('/{key}', summary='Delete redis key')
async def run_set(key: str):
    """Set a key in redis"""
    return await ctx.r.execute_command("DEL", *key.split('+'))
