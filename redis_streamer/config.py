import os

DEVICE_META_PREFIX = ':devices:meta'
DEVICES_CONNECTED_KEY = ':devices:connected'
DEVICES_SEEN_KEY = ':devices:seen'
EVENT_PREFIX = ':event'
STREAM_META_PREFIX = ':stream:meta'

DEFAULT_DEVICE = 'default'

ENABLE_MULTI_DEVICE_PREFIXING = not int(os.getenv('DISABLE_MULTI_DEVICE_PREFIXING') or 0)
