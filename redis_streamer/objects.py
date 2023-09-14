import time
import redis
import orjson
from . import utils


class RedisBase:
    """
    Base class for Redis-based data structures.

    This class provides common functionality and utility methods for Redis-based data structures.
    It is intended to be used as a base class for specific data structure implementations.

    Args:
        redis_connection (redis.Redis): A Redis connection object.
        key (str): The Redis key associated with the data structure.

    Attributes:
        _redis (redis.Redis): The Redis connection object.
        _key (str): The Redis key associated with the data structure.
    """

    def __init__(self, redis_connection, key):
        self._redis = redis_connection
        self._key = key

    def _kl(self, key):
        """
        Decode a binary Redis key to a UTF-8 string.

        Args:
            key (bytes): The binary key to decode.

        Returns:
            str: The decoded UTF-8 string key.
        """
        return key.decode('utf-8')

    def _vl(self, value):
        """
        Deserialize a binary Redis value to a Python object.

        Args:
            value (bytes): The binary value to deserialize.

        Returns:
            Any: The deserialized Python object.
        """
        return orjson.loads(value)

    def _vd(self, value):
        """
        Serialize a Python object to a binary Redis value.

        Args:
            value (Any): The Python object to serialize.

        Returns:
            bytes: The serialized binary value.
        """
        return orjson.dumps(value)



class RedisDict(RedisBase):
    """
    Redis-backed dictionary data structure.

    This class provides a Python dictionary-like interface to a Redis-backed dictionary.
    It supports various dictionary operations such as key-value retrieval, setting, deletion,
    membership checks, iteration, and more.

    Args:
        redis_connection (redis.Redis): A Redis connection object.
        key (str): The Redis key associated with the dictionary.

    Attributes:
        _redis (redis.Redis): The Redis connection object.
        _key (str): The Redis key associated with the dictionary.
    """

    def __getitem__(self, key):
        """
        Get the value associated with the specified key in the dictionary.

        Args:
            key (str): The key to retrieve the value for.

        Returns:
            Any: The value associated with the key.

        Raises:
            KeyError: If the key is not found in the dictionary.
        """
        value = self._redis.hget(self._key, key)
        if value is None:
            raise KeyError(key)
        return self._vl(value)

    def __setitem__(self, key, value):
        """
        Set the value associated with the specified key in the dictionary.

        Args:
            key (str): The key to set the value for.
            value (Any): The value to associate with the key.
        """
        self._redis.hset(self._key, key, self._vd(value))

    def __delitem__(self, key):
        """
        Remove the key-value pair with the specified key from the dictionary.

        Args:
            key (str): The key to remove from the dictionary.

        Raises:
            KeyError: If the key is not found in the dictionary.
        """
        if not self._redis.hdel(self._key, key):
            raise KeyError(key)

    def __contains__(self, key):
        """
        Check if the dictionary contains the specified key.

        Args:
            key (str): The key to check for in the dictionary.

        Returns:
            bool: True if the key is in the dictionary, False otherwise.
        """
        return self._redis.hexists(self._key, key)

    def __iter__(self):
        """
        Iterate over the keys in the dictionary.

        Yields:
            str: The next key in the dictionary.
        """
        return iter(self.keys())

    def get(self, key, default=None):
        """
        Get the value associated with the specified key in the dictionary, with a default value if the key is not found.

        Args:
            key (str): The key to retrieve the value for.
            default (Any, optional): The default value to return if the key is not found. Defaults to None.

        Returns:
            Any: The value associated with the key, or the default value if the key is not found.
        """
        value = self._redis.hget(self._key, key)
        return self._vl(value) if value is not None else default

    def keys(self):
        """
        Get a list of keys in the dictionary.

        Returns:
            List[str]: A list of keys in the dictionary.
        """
        return [self._kl(key) for key in self._redis.hkeys(self._key)]

    def values(self):
        """
        Get a list of values in the dictionary.

        Returns:
            List[Any]: A list of values in the dictionary.
        """
        return [self._vl(value) for value in self._redis.hvals(self._key)]

    def items(self):
        """
        Get a list of key-value pairs (items) in the dictionary.

        Returns:
            List[Tuple[str, Any]]: A list of key-value pairs in the dictionary.
        """
        return [(self._kl(key), self._vl(value)) for key, value in self._redis.hgetall(self._key).items()]



class RedisList(RedisBase):
    """
    Redis-backed list data structure.

    This class provides a Python list-like interface to a Redis-backed list.
    It supports various list operations such as append, extend, pop, indexing, and iteration.

    Args:
        redis_connection (redis.Redis): A Redis connection object.
        key (str): The Redis key associated with the list.

    Attributes:
        _redis (redis.Redis): The Redis connection object.
        _key (str): The Redis key associated with the list.
    """

    def __getitem__(self, index):
        """
        Get the element at the specified index in the list.

        Args:
            index (int): The index of the element to retrieve.

        Returns:
            Any: The value of the element at the specified index.

        Raises:
            IndexError: If the index is out of range.
        """
        value = self._redis.lindex(self._key, index)
        if value is None:
            raise IndexError(index)
        return self._vl(value)

    def __setitem__(self, index, value):
        """
        Set the element at the specified index in the list.

        Args:
            index (int): The index at which to set the element.
            value (Any): The value to set at the specified index.
        """
        self._redis.lset(self._key, index, self._vd(value))

    def __delitem__(self, index):
        """
        Remove the element at the specified index from the list.

        Args:
            index (int): The index of the element to remove.

        Raises:
            IndexError: If the index is out of range.
        """
        value = self._redis.lindex(self._key, index)
        if value is None:
            raise IndexError(index)
        self._redis.lrem(self._key, 1, value)

    def __len__(self):
        """
        Get the number of elements in the list.

        Returns:
            int: The number of elements in the list.
        """
        return self._redis.llen(self._key)

    def __contains__(self, value):
        """
        Check if the list contains the specified value.

        Args:
            value (Any): The value to check for in the list.

        Returns:
            bool: True if the value is in the list, False otherwise.
        """
        return self._redis.lpos(self._key, self._vd(value)) is not None

    def __iter__(self):
        """
        Iterate over the elements in the list.

        Yields:
            Any: The next element in the list.
        """
        for value in self._redis.lrange(self._key, 0, -1):
            yield self._vl(value)

    def append(self, value):
        """
        Append a value to the end of the list.

        Args:
            value (Any): The value to append to the list.
        """
        self._redis.rpush(self._key, self._vd(value))

    def extend(self, values):
        """
        Extend the list by appending multiple values to the end.

        Args:
            values (Iterable[Any]): The values to append to the list.
        """
        self._redis.rpush(self._key, *[self._vd(value) for value in values])

    def pop(self, index=-1):
        """
        Remove and return the element at the specified index (default: last element).

        Args:
            index (int, optional): The index of the element to remove. Defaults to -1 (last element).

        Returns:
            Any: The value of the removed element.

        Raises:
            IndexError: If the index is out of range.
        """
        value = self._redis.lindex(self._key, index)
        if value is None:
            raise IndexError(index)
        self._redis.lrem(self._key, 1, value)
        return self._vl(value)


class RedisSet(RedisBase):
    def __len__(self):
        """
        Get the size of the set.

        Returns:
            The number of elements in the set.
        """
        return self._redis.scard(self._key)

    def __contains__(self, element):
        """
        Check if an element is in the set.

        Args:
            element: The element to check for membership.

        Returns:
            True if the element is in the set, False otherwise.
        """
        return self._redis.sismember(self._key, self._vd(element))
    
    def __iter__(self):
        """
        Return an iterator over the elements of the set.

        Returns:
            An iterator over the elements.
        """
        return (self._vl(x) for x in self._redis.smembers(self._key))

    def __or__(self, other_set):
        """
        Perform a union operation with another set.

        Args:
            other_set: Another RedisSet instance or a set.

        Returns:
            A new RedisSet instance representing the union of the two sets.
        """
        if isinstance(other_set, RedisSet):
            return self._redis.sunion(self._key, other_set._key)
        return set(self) | set(other_set)

    def __and__(self, other_set):
        """
        Perform an intersection operation with another set.

        Args:
            other_set: Another RedisSet instance or a set.

        Returns:
            A python set representing the intersection of the two sets.
        """
        if isinstance(other_set, RedisSet):
            return self._redis.sinter(self._key, other_set._key)
        return set(self) & set(other_set)

    def __sub__(self, other_set):
        """
        Perform a difference operation with another set.

        Args:
            other_set: Another RedisSet instance or a set.

        Returns:
            A python set representing the difference between the two sets.
        """
        if isinstance(other_set, RedisSet):
            return self._redis.sdiff(self._key, other_set._key)
        return set(self) - set(other_set)

    def add(self, *elements):
        """
        Add elements to the set.

        Args:
            *elements: The elements to add to the set.
        """
        self._redis.sadd(self._key, *(self._vd(x) for x in elements))

    def remove(self, *elements):
        """
        Remove elements from the set.

        Args:
            *elements: The elements to remove from the set.
        """
        self._redis.srem(self._key, *(self._vd(x) for x in elements))

    def clear(self):
        """
        Remove all elements from the set.
        """
        self._redis.delete(self._key)

    def update(self, elements):
        """
        Update the set with a new set of elements.

        Args:
            elements: An iterable containing the new elements for the set.
        """
        self.add(*elements)


class RedisCounter(RedisBase):
    def __int__(self):
        """
        Get the current value of the counter.

        Returns:
            The current value of the counter.
        """
        value = self._redis.get(self._key)
        return int(value) if value is not None else 0

    def __iadd__(self, other):
        """
        Increment the counter by the specified amount.

        Args:
            other: The amount to increment the counter by.

        Returns:
            The updated counter.
        """
        self._redis.incrby(self._key, other)
        return self

    def __isub__(self, other):
        """
        Decrement the counter by the specified amount.

        Args:
            other: The amount to decrement the counter by.

        Returns:
            The updated counter.
        """
        self._redis.decrby(self._key, other)
        return self

    def __repr__(self):
        """
        Get a string representation of the counter.

        Returns:
            A string representation of the counter.
        """
        return str(int(self))



class RedisStreams:
    def __init__(self, redis_connection, key):
        self._redis = redis_connection
        self._key = key

    def __getitem__(self, index):
        """
        Retrieve a single element or a range of elements from the Redis Stream using an index or timestamp.

        Args:
            index (float, slice): The index (timestamp) or slice to retrieve from the Stream.

        Returns:
            dict or list: A single dictionary representing the element in the Stream if index is float.
                          A list of dictionaries representing the elements within the specified timestamp range if index is slice.

        Raises:
            IndexError: If the index (timestamp) is out of range.
        """
        if isinstance(index, slice):
            start, stop = self._get_range_from_slice(index)
            elements = self._get_elements_in_range(start, stop)
            return elements
        elif isinstance(index, (int, float)):
            element = self._get_element_at_timestamp(float(index))
            return element
        else:
            raise TypeError("Invalid index type")

    def _get_element_at_timestamp(self, timestamp):
        """
        Get an element from the Redis Stream at the specified timestamp.

        Args:
            timestamp (float): The timestamp of the element to retrieve.

        Returns:
            dict: A dictionary representing the element in the Stream.

        Raises:
            IndexError: If the timestamp is out of the range of the Stream.
        """
        result = self._redis.xrange(self._key, min=timestamp, max=timestamp, count=1)
        if not result:
            raise IndexError("Timestamp out of range")
        return result[0][1]

    def _get_range_from_slice(self, s):
        """
        Get the start and stop timestamps for a slice.

        Args:
            s (slice): The slice object representing the range.

        Returns:
            tuple: A tuple containing the start and stop timestamps.
        """
        start = s.start if s.start is not None else "-"
        stop = s.stop if s.stop is not None else "+"
        return start, stop

    def _get_elements_in_range(self, start, stop):
        """
        Get elements within the specified timestamp range from the Redis Stream.

        Args:
            start (float): The start timestamp (inclusive).
            stop (float): The stop timestamp (inclusive).

        Returns:
            list: A list of dictionaries representing the elements within the specified range.
        """
        result = self._redis.xrange(self._key, min=start, max=stop)
        return [item[1] for item in result]





    def get_next_data_point_after(self, timestamp):
        """
        Get the next data point in the stream that occurs after the specified timestamp.

        Args:
            timestamp (float): The timestamp after which to retrieve the data point.

        Returns:
            dict: A dictionary representing the data point in the stream.
        """
        result = self._redis.xrange(self._key, min=timestamp, count=1)
        return result[0] if result else None

    def get_next_data_point_before(self, timestamp):
        """
        Get the data point that occurs immediately before the specified timestamp.

        Args:
            timestamp (float): The timestamp before which to retrieve the data point.

        Returns:
            dict: A dictionary representing the data point in the stream.
        """
        result = self._redis.xrevrange(self._key, max=timestamp, count=1)
        return result[0] if result else None

    def get_latest_data_point_after(self, timestamp):
        """
        Get the most recent data point in the stream that is still after the specified timestamp.

        Args:
            timestamp (float): The timestamp after which to retrieve the data point.

        Returns:
            dict: A dictionary representing the data point in the stream.
        """
        result = self._redis.xrevrange(self._key, min=timestamp, count=1)
        return result[0] if result else None

    def get_data_point_at_timestamp(self, timestamp):
        """
        Get the data point that occurred exactly at the specified timestamp.

        Args:
            timestamp (float): The timestamp of the data point to retrieve.

        Returns:
            dict: A dictionary representing the data point in the stream.
        """
        result = self._redis.xrange(self._key, min=timestamp, max=timestamp, count=1)
        return result[0] if result else None

    def get_data_points_within_range(self, start_timestamp, stop_timestamp):
        """
        Get all data points within the specified time range.

        Args:
            start_timestamp (float): The start timestamp (inclusive).
            stop_timestamp (float): The stop timestamp (inclusive).

        Returns:
            list: A list of dictionaries representing the data points within the specified range.
        """
        result = self._redis.xrange(self._key, min=start_timestamp, max=stop_timestamp)
        return result

    def __len__(self):
        """
        Get the number of elements in the Redis Stream.

        Returns:
            int: The number of elements in the Stream.
        """
        return self._redis.xlen(self._key)



class StreamCursor(RedisDict):
    def __init__(self, redis_connection, key):
        super().__init__(redis_connection, key)

    def init(self, stream_ids: list[str] | dict[str, str], prefix='') -> dict[str, str]:
        if isinstance(stream_ids, list):
            stream_ids = {s: '$' for s in stream_ids}
        if prefix:
            stream_ids = {f'{prefix}{s}': t for s, t in stream_ids.items()}


        # Replace dollar with the current timestamp
        current_time = utils.format_epoch_time(time.time())
        stream_ids = {k: current_time if v == '$' else v for k, v in stream_ids.items()}

        # Store the initial cursor positions in the RedisDict
        super().update(stream_ids)
        return self

    def update(self, data: list[str | tuple]) -> dict[str, str]:
        super().update({sid: max(t for t, _ in ts) for sid, ts in data if ts})
        return self


class Streams:
    '''
    
    .. code-block:: python

        streams = Streams()
        cursor = StreamCursor(r, "idk").init(["main", "depthlt"])

        data = {}
        while True:
            data.update(streams.get_latest())
    
    '''
    def __init__(self, redis, key, stream_ids):
        self._cursors = StreamCursor(redis, key)
        self._cursors.init(stream_ids)

    def _xrevrange_after(self, stream_ids: dict[str, str], **kw):
        with self._redis.pipeline() as p:
            for sid, t in stream_ids.items():
                self._redis.xrevrange(p, sid, min=t, **kw)
            return list(zip(stream_ids, p.execute()))

    def _xrevrange_before(self, stream_ids: dict[str, str], **kw):
        with self._redis.pipeline() as p:
            for sid, t in stream_ids.items():
                self._redis.xrevrange(p, sid, max=t, **kw)
            return list(zip(stream_ids, p.execute()))

    def _xrevrange(self, stream_ids: dict[str, tuple[str, str]], **kw):
        with self._redis.pipeline() as p:
            for sid, (min, max) in stream_ids.items():
                self._redis.xrevrange(p, sid, max, min, **kw)
            return list(zip(stream_ids, p.execute()))

    def get_latest(self, stream_ids: dict[str, str], block=None, **kw):
        """
        Get the most recent data point in the stream that is still after the specified timestamp.

        Args:
            timestamp (float): The timestamp after which to retrieve the data point.

        Returns:
            dict: A dictionary representing the data point in the stream.
        """
        result = self._xrevrange_after(stream_ids, **kw)
        if block is not False and not any(x for s, x in result):
            result = self._redis.xread(stream_ids, block=block, **kw)
        return _singleresultdict(result)

    def get_next(self, stream_ids: dict[str, str], block=None, **kw):
        """
        Get the next data point in the stream that occurs after the specified timestamp.

        Args:
            timestamp (float): The timestamp after which to retrieve the data point.

        Returns:
            dict: A dictionary representing the data point in the stream.
        """
        result = self._redis.xread(stream_ids, block=block, **kw)
        return _singleresultdict(result)
    
    def get_prev(self, stream_ids: dict[str, str]):
        """
        Get the data point that occurs immediately before the specified timestamp.

        Args:
            timestamp (float): The timestamp before which to retrieve the data point.

        Returns:
            dict: A dictionary representing the data point in the stream.
        """
        result = self._xrevrange_before(stream_ids, count=1)
        return _singleresultdict(result)
    
    def get(self, stream_ids: dict[str, str]):
        """
        Get the data point that occurred exactly at the specified timestamp.

        Args:
            timestamp (float): The timestamp of the data point to retrieve.

        Returns:
            dict: A dictionary representing the data point in the stream.
        """
        result = self._xrevrange({
            sid: (t, utils.nonspecific_timestamp(t))
            for sid, t in stream_ids.items()
        })
        return _resultdict(result)

    def get_range(self, stream_ids: dict[str, tuple[str, str]]):
        """
        Get all data points within the specified time range.

        Args:
            start_timestamp (float): The start timestamp (inclusive).
            stop_timestamp (float): The stop timestamp (inclusive).

        Returns:
            list: A list of dictionaries representing the data points within the specified range.
        """
        result = self._xrevrange(stream_ids)
        return _resultdict(result)

def _resultdict(result):
    return {sid: xs for sid, xs in result if xs}

def _singleresultdict(result):
    return {sid: xs[0] for sid, xs in result if xs}