from fastapi import FastAPI
from strawberry.fastapi import GraphQLRouter

from redis_streamer import ctx, streams
from redis_streamer.routes import data_requests, data_ws #, streaming, prompt_ws



app = FastAPI()

@app.on_event("startup")
async def startup_event():
    await ctx.init()

@app.get('/')
def index():
    print('hi :)')
    return 'hi :)'

graphql_app = GraphQLRouter(streams.schema)
app.include_router(graphql_app, prefix="/graphql")
app.include_router(data_requests.app, prefix="/data")
app.include_router(data_ws.app, prefix="/data")
# app.include_router(prompt_ws.app, prefix="/data")
# app.include_router(streaming.app, prefix="/streaming")
