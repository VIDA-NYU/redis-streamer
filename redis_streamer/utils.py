from __future__ import annotations
import datetime



def maybe_encode(s: str|bytes, encoding='utf-8'):
    '''If the input is str, encode as utf-8 (or other).'''
    return s.encode(encoding) if isinstance(s, str) else s

def maybe_decode(s: str|bytes, encoding='utf-8'):
    '''If the input is bytes, decode as utf-8 (or other).'''
    return s.decode(encoding) if isinstance(s, bytes) else s

# ---------------------------------------------------------------------------- #
#                               Timestamp parsing                              #
# ---------------------------------------------------------------------------- #


def parse_epoch_time(tid: str|bytes):
    '''Convert a redis timestamp to epoch seconds.'''
    return int(maybe_decode(tid).split('-')[0])/1000

def format_epoch_time(tid: float, i='0'):
    '''Format a redis timestamp from epoch seconds.'''
    return f'{int(tid * 1000)}-{i}'

def parse_datetime(tid: str|bytes):
    '''Convert a redis timestamp to a datetime object.'''
    return datetime.datetime.fromtimestamp(parse_epoch_time(tid))

def format_datetime(dt: datetime.datetime):
    '''Format a redis timestamp from a datetime object.'''
    return format_epoch_time(dt.timestamp())

def format_iso(tid: str|bytes, format: str=''):
    '''Convert a redis timestamp to a iso format.'''
    if not format:
        return parse_datetime(tid).strftime(format)
    return parse_datetime(tid).isoformat()


# ---------------------------------------------------------------------------- #
#                                Data formatting                               #
# ---------------------------------------------------------------------------- #

def pack_entries(entries):
    offsets = []
    content = bytearray()
    for sid, data in entries:
        sid = maybe_decode(sid)
        for ts, d in data:
            offsets.append((sid, maybe_decode(ts), len(content)))
            content += d[b'd']
    # jsonOffsets = orjson.dumps(offsets).decode('utf-8')
    return offsets, content
