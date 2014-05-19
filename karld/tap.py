from collections import deque


class Bucket(object):
    """
    Encloses a function that produces results from
    an item of an iterator, accumulating any results
    in a deque.
    """
    def __init__(self, func):
        self.func = func
        self._contents = deque()

    def __call__(self, *args, **kwargs):
        result = self.func(*args, **kwargs)
        if result is not None:
            self._contents.append(result)

    def contents(self):
        """
        :returns: contents
        """
        return self._contents

    def drain_contents(self):
        """
        Starts a new collection to accumulate future contents
        and returns all of existing contents.
        """
        existing_contents = self._contents
        self._contents = deque()
        return existing_contents


def stream_tap(callables, stream):
    """
    Calls each callable with each item in the stream.
    Use with Buckets. Make a Bucket with a callable
    and then pass a tuple of those buckets
    in as the callables. After iterating over
    this generator, get contents from each Spigot.

    :param callables: collection of callable.
    :param stream: Iterator if values.
    """
    for item in stream:
        for caller in callables:
            caller(item)
        yield item
