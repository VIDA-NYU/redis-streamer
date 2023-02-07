import _patch
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from strawberry.fastapi import GraphQLRouter

from redis_streamer import ctx
from redis_streamer import graphql_schema
from redis_streamer.routes import data_requests, data_ws #, streaming, prompt_ws



app = FastAPI()

@app.on_event("startup")
async def startup_event():
    await ctx.init()

@app.get('/')
def index():
    return 'hi :) | Redis Streamer - GraphQL playground at /graphql'

graphql_app = GraphQLRouter(graphql_schema.schema)
app.include_router(graphql_app, prefix="/graphql")
app.include_router(data_requests.app, prefix="/data")
app.include_router(data_ws.app, prefix="/data")
# app.include_router(prompt_ws.app, prefix="/data")
# app.include_router(streaming.app, prefix="/streaming")

app.add_middleware(
    CORSMiddleware,
    allow_credentials=True,
    allow_origins=['*'],
    #allow_methods=['*'],
    allow_headers=['*'],
    allow_methods=["GET", "POST", "PUT", "DELETE", "HEAD", "OPTIONS"],
)

@app.exception_handler(Exception)
async def validation_exception_handler(request, exc):
    return JSONResponse({'error': str(exc)}, status_code=500)