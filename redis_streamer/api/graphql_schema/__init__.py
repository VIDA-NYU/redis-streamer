import strawberry
from redis_streamer.core import ctx
from redis_streamer.config import ENABLE_MULTI_DEVICE_PREFIXING
from . import devices
from . import streams

if ENABLE_MULTI_DEVICE_PREFIXING:
    _Query = type('_Query', (devices.Devices, streams.Streams), {})
    _Mutation = type('_Mutation', (streams.StreamMutation, devices.DeviceMutation), {})
else:
    _Query = type('_Query', (streams.Streams,), {})
    _Mutation = type('_Mutation', (streams.StreamMutation,), {})

@strawberry.type
class Query(_Query):
    pass

@strawberry.type
class Mutation(_Mutation):
    @strawberry.mutation
    async def flush(self) -> int:
        return await ctx.r.flushdb()

@strawberry.type
class Subscription(streams.StreamSubscription):
    pass

# ---------------------------------------------------------------------------- #
#                                    Schema                                    #
# ---------------------------------------------------------------------------- #


schema = strawberry.Schema(
    Query, 
    mutation=Mutation, 
    subscription=Subscription)
