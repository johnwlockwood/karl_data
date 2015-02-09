#!/usr/bin/env python
# _*_ coding: utf-8 _*_

import sys

from functools import partial
from twisted.internet import reactor
from twisted.internet import defer
from twisted.python import log
from twisted.internet.task import deferLater
from twisted.internet.task import react
from twisted.internet.task import cooperate
from twisted.internet.defer import inlineCallbacks

from karld.iter_utils import i_batch
from karld.tap import Bucket, stream_tap, cooperative_accumulate, cooperative_accumulation_handler


def wat(value):
    print value


def protect(func, blah, *args):
    return func(*args)


pwat = partial(protect, wat)


def i_print_some(value, n=10):
    for x in range(n):
        print value
        yield


def divby_grouped():
    """
    Break up a long running operation by batching it.

    :return:
    """
    for ns in i_batch(3, sorted(list(range(10)), reverse=True)):
        for n in ns:
            print "100 divided by {} is {}".format(n, 10/n)
        yield


def divby(value):
    """
    Break up a long running operation by batching it.

    :return:
    """
    for ns in i_batch(3, sorted(value, reverse=True)):
        for n in ns:
            print "100 divided by {} is {}".format(n, 10.0/n)
        yield


def do_divby():
    task = cooperate(divby_grouped())
    d = task.whenDone()
    return d


def i_get_tenth_11(value):
    yield value[10]
    yield value[11]


def do_p1011(value):
    """
    Start a Deferred whose callBack arg is a deque of the accumulation
    of the values yielded from i_get_tenth_11(value).

    :param value:
    :return:
    """
    return cooperative_accumulate(partial(i_get_tenth_11, value))


@inlineCallbacks
def one_two_three(value, three=7):
    """
    Make a thing that will start the first task,
    allowing it to cooperate with other one_two_three tasks.
    when the first task is done, start the following one and so on.
    :param value:
    :return:
    """
    #  first deferred
    task1 = cooperate(i_print_some("one "+value, 20))
    d1 = task1.whenDone()
    yield d1

    try:
        yield do_divby()
    except ZeroDivisionError:
        log.err("log oops i messed up, but it's ok.")

    # second d
    task2 = cooperate(i_print_some("two "+value, 5))
    d2 = task2.whenDone()
    yield d2
    # third d
    task3 = cooperate(i_print_some("three "+value, three))
    d3 = task3.whenDone()
    yield d3
    defer.returnValue(value)


@inlineCallbacks
def and_the_winner_is2(value):
    """
    """
    #  first deferred
    result = yield cooperative_accumulate(partial(i_get_tenth_11, list(range(110, 150))))

    log.msg("accumulated {}".format(result))
    result = yield do_p1011(value)
    defer.returnValue(result)


@inlineCallbacks
def and_the_winner_is(value):
    """
    """
    #  first deferred
    result = yield cooperative_accumulate(partial(i_get_tenth_11, list(range(110, 150))))

    log.msg("accumulated {}".format(result))
    result = yield do_p1011(value)
    log.msg( "from do_p1011 {}".format(result))

    try:
        result = yield do_p1011(result)
        defer.returnValue(result)
    except IndexError, e:
        log.err(e)
        defer.returnValue("Couldn't get second winner")


def coi_print_some(value):
    return cooperate(i_print_some(value))


def lala(value):
    print value
    # reactor.stop()

__doc__ = """
Run some iterators cooperatively.
Run some inlineCallBacks cooperatively, meaning the each deferred
yielded from a one_two_three will complete before the next
deferred is run. with multiple one_two_three, their operations
will run cooperatively, ie: one_two_three("A") task2 could have it's
iterator advanced and then one_two_three("B") task1 iterator advanced.

When a one_two_three is called, it returns a deferred, which
is called back to when with the one_two_three arg value because of
returnValue.

gatherResults will make a Deferred and call it back once
all of the deferred's it's been given are called back.
This lets you do something after a bunch of things are
done without you having to know what order they are
being done in. without you knowing which one will
finish last.

The value of the result that gatherResults gets for a deferred
from a task.whenDone is the task's iterator, unless
a callback is added, then the callback will have it's first arg as
the task's iterator and the value of the result
that gatherResults gets is None.

Use deferLater to print holla after 5 seconds.

lala is the callback of the gatherResults,
which prints it's argument and stops the reactor, so this
script kindly ends after everything is done.

"""


def main(reactor):
    task1 = cooperate(i_print_some("hello", 50))
    taskfinish = task1.whenDone()
    taskfinish.addCallback(pwat, "short and stout.")

    task4 = coi_print_some("peter")
    t4d = task4.whenDone()
    t4d.addCallback(wat)  # will print the generator object from coi_print_some
    t4d.addCallback(pwat, "I'm a little teacup. t4d")
    t4d.addCallback(wat)  # will print None

    # t4d.callback = lambda y: wat("BOUNCE THE PLANET!")
    t4d.addErrback(log.err)
    task2 = cooperate(i_print_some("world"))
    t2d = task2.whenDone()
    # t2d.addCallback(pwat, "FUZZY WUZZY WAS A BEAR. t2d")
    t1 = one_two_three("A", three=2)
    t2 = one_two_three("B")
    t3 = one_two_three("C")
    t4 = and_the_winner_is(list(range(15)))

    t2.addCallback(wat)  # wat will be passed the returnValue of one_two_three("B"), which is "B"
    t2.addCallback(wat)  # wat will be passed None, which is the result of the call to wat above.
    # t2 here will be None because it's first callback captured the return value

    dl = deferLater(reactor, 5, wat, "holla")

    reactor.callWhenRunning(wat, "wally")

    d = defer.gatherResults([t1, t2, t3, t4d, t2d, taskfinish, dl, t4])

    d.addCallback(lala)
    return d

if __name__ == "__main__":
    log.startLogging(sys.stdout)
    # reactor.run()
    react(main, [])
