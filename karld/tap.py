from collections import deque


class Spigot(object):
    """
    Encloses a function that produces results from
    an item of an iterator, accumulating any results
    in a deque.
    """
    def __init__(self, func):
        self.func = func
        self._results = deque()

    def __call__(self, *args, **kwargs):
        result = self.func(*args, **kwargs)
        if result is not None:
            self._results.append(result)

    def results(self):
        """
        :returns: results
        """
        return self._results

    def flush_results(self):
        """
        Starts a new collection to accumulate future results
        and returns all of existing results.
        """
        existing_results = self._results
        self._results = deque()
        return existing_results


def stream_tap(callables, stream):
    """
    Calls each callable with each item in the stream.
    Use with Spigots. Make a Spigot with a callable
    and then pass a tuple of those Spigot instances
    in as the callables. After iterating over
    this generator, get results from each Spigot.

    :param callables: collection of callable.
    :param stream: Iterator if values.
    """
    for item in stream:
        for caller in callables:
            caller(item)
        yield item
