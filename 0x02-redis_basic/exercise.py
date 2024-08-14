#!/usr/bin/env python3
'''A module for using the Redis NoSQL data storage.
'''
import uuid
import redis
from functools import wraps
from typing import Any, Union, Callable, Optional


def count_calls(method: Callable) -> Callable:
    '''Tracks the number of calls made to a method in a Cache class.

    Args:
        method (Callable): The method to track the number of calls.

    Returns:
        Callable: The wrapped method.
    '''
    @wraps(method)
    def invoker(self, *args, **kwargs) -> Any:
        '''Invokes the given method after incrementing its call counter.

        Args:
            self: The instance of the class.
            *args: Positional arguments passed to the method.
            **kwargs: Keyword arguments passed to the method.

        Returns:
            Any: The result of the method call.
        '''
        if isinstance(self._redis, redis.Redis):
            self._redis.incr(method.__qualname__)
        return method(self, *args, **kwargs)
    return invoker


def call_history(method: Callable) -> Callable:
    '''Tracks the call details of a method in a Cache class.
    '''
    @wraps(method)
    def invoker(self, *args, **kwargs) -> Any:
        '''Returns the method's output after storing its inputs and output.
        '''
        # Create keys for input and output storage
        key_in = '{}:inputs'.format(method.__qualname__)
        key_out = '{}:outputs'.format(method.__qualname__)

        # Store input in redis
        if isinstance(self._redis, redis.Redis):
            self._redis.rpush(key_in, str(args))

        # Call the method ad store its output
        output = method(self, *args, **kwargs)

        # Store output in Redis
        if isinstance(self._redis, redis.Redis):
            self._redis.rpush(key_out, output)

        return output
    return invoker


class Cache:
    """
    A class for storing and retrieving data from Redis using UUIDs as keys.

    Attributes:
        self._redis (redis.Redis): A Redis client instance.

    Methods:
        __init__(): Initialize a Redis client instance and clear the database.
        store(data): Store the data in Redis with a UUID key and
        return the key.
    """

    def __init__(self):
        """
        Initialize a Redis client instance and clear the database.
        """
        self._redis = redis.Redis()
        self._redis.flushdb()

    @count_calls
    @call_history
    def store(self, data: Union[str, bytes, int, float]) -> str:
        """
        Store the data in Redis with a UUID key and return the key.

        Args:
            data: The data to be stored. Can be a string, bytes, int, or float.

        Returns:
            str: The UUID key used to store the data.
        """
        key = str(uuid.uuid4())
        self._redis.set(key, data)
        return key

    def get(
        self,
        key: str,
        fn: Optional[Callable[[Any], Union[str, bytes, int, float]]] = None
            ) -> Optional[Union[str, bytes, int, float]]:
        """
        Retrieves a value from a Redis data storage.

        Args:
            key: The key of the data to be retrieved.
            fn: An optional function to apply to the retrieved data.

        Returns:
            The retrieved data, optionally transformed by the function.
        """
        # Retrieve the data from Redis
        data = self._redis.get(key)

        # Apply the function to the data (if provided)
        return fn(data) if fn is not None else data

    def get_str(self, key: str) -> Optional[str]:
        """Retrieve a string value from Redis based on the given key.

            Args:
                key: The key to retrieve the string value from Redis.

            Returns:
                The retrieved string value.
        """
        # Retrieve the data using the get method
        data = self.get(key)

        # check if data is None
        if data is None:
            return None

        # Convert data to sring and return
        return str(data)


def get_int(self, key: str) -> Optional[int]:
    """
    Retrieves an integer value from a Redis data storage.

    Args:
        key: The key of the data to be retrieved.

    Returns:
        The retrieved integer value.
    """
    # Retrieve the data using the get method
    data = self.get(key)

    # Check if data is None
    if data is None:
        return None

    # Convert to integer and return
    return int(data)


def replay(fn: Callable) -> None:
    '''Displays the call history of a Cache class' method.
    '''
    if fn is None or not hasattr(fn, '__self__'):
        return
    redis_store = getattr(fn.__self__, '_redis', None)
    if not isinstance(redis_store, redis.Redis):
        return
    fxn_name = fn.__qualname__
    in_key = '{}:inputs'.format(fxn_name)
    out_key = '{}:outputs'.format(fxn_name)
    fxn_call_count = 0
    if redis_store.exists(fxn_name) != 0:
        fxn_call_count = int(redis_store.get(fxn_name) or 0)
    print('{} was called {} times:'.format(fxn_name, fxn_call_count))
    fxn_inputs = redis_store.lrange(in_key, 0, -1)
    fxn_outputs = redis_store.lrange(out_key, 0, -1)
    for fxn_input, fxn_output in zip(fxn_inputs, fxn_outputs):
        print('{}(*{}) -> {}'.format(
            fxn_name,
            fxn_input.decode("utf-8"),
            fxn_output.decode(
                "utf-8") if isinstance(fxn_output, bytes) else fxn_output,
        ))
