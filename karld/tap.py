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


def cooperative_accumulation_handler(stopped_generator, spigot):
    """
    Drain the contents of the bucket from the spigot.

    :param stopped_generator: Generator which as stopped
    :param spigot: a Bucket.
    :return: The contents of the bucket.
    """
    return spigot.drain_contents()


def cooperative_accumulate(a_generator_func):
    """
    Start a Deferred whose callBack arg is a deque of the accumulation
    of the values yielded from a_generator_func.

    :param a_generator_func: A function which returns a generator.
    :return:
    """
    from twisted.internet.task import cooperate
    spigot = Bucket(lambda x: x)
    items = stream_tap((spigot,), a_generator_func())
    d = cooperate(items).whenDone()
    d.addCallback(cooperative_accumulation_handler, spigot)
    return d
