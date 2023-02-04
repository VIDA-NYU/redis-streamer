import fastapi


# Ignore trailing slashes - so that /recipes and /recipes/ will return the same thing
# This works by adding a second route with and without the trailing slash
# use include_in_schema to prevent duplicate routes in the documentation.
# https://github.com/tiangolo/fastapi/issues/2060

old_api_route = fastapi.APIRouter.add_api_route
def api_route_trailing_slash_fix(self, path: str, *a, include_in_schema: bool=True, **kw):
    path = path.rstrip('/')
    old_api_route(self, f'{path}/', *a, include_in_schema=False, **kw)
    return old_api_route(self, path, *a, include_in_schema=include_in_schema, **kw)
fastapi.APIRouter.add_api_route = api_route_trailing_slash_fix