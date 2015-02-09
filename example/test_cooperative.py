# _*_ coding: utf-8 _*_
from collections import deque
from functools import partial
from itertools import chain

from twisted.internet import defer
from twisted.trial import unittest

from karld.tap import cooperative_accumulate

from cooperative_multitasking import and_the_winner_is2
from cooperative_multitasking import i_get_tenth_11
from cooperative_multitasking import do_p1011


class TestWinner(unittest.TestCase):
    @defer.inlineCallbacks
    def test_cooperative_accumulate(self):
        """
        Ensure that within an inline callback function,
        a cooperative_accumulate wrapped generator
        yields the result of the output of the generator.

        :return:
        """
        result = yield cooperative_accumulate(partial(i_get_tenth_11, list(range(110, 150))))
        self.assertEqual(result, deque([120, 121]))

    @defer.inlineCallbacks
    def test_winner(self):
        """
        Ensure that within an inline callback function,
        a cooperative_accumulate based function
        yields the result if it's cooperative generator.

        :return:
        """
        result = yield and_the_winner_is2(list(range(15)))
        self.assertEqual(result, deque([10, 11]))

    @defer.inlineCallbacks
    def test_multi_winner(self):
        """
        Ensure multiple inline callback functions will run cooperatively.

        :return:
        """
        d1 = and_the_winner_is2(list(range(15)))
        d2 = and_the_winner_is2(list(range(15, 100)))
        result = yield defer.gatherResults([d1, d2])

        self.assertEqual(result, [deque([10, 11]), deque([25, 26])])

    @defer.inlineCallbacks
    def test_trice_winner(self):
        """
        Ensure multiple inline callback functions will run cooperatively.

        :return:
        """
        d1 = and_the_winner_is2(list(range(15)))
        d2 = and_the_winner_is2(list(range(15, 100)))
        d3 = do_p1011(range(4, 50))
        result = yield defer.gatherResults([d1, d2, d3])

        self.assertEqual(result, [deque([10, 11]),
                                  deque([25, 26]),
                                  deque([14, 15])])

    @defer.inlineCallbacks
    def test_multi_winner_chain(self):
        """
        Ensure multiple inline callback functions will run cooperatively.

        Ensure the result of gatherResults can be chained together
        in order.

        :return:
        """
        called = []

        def watcher(value):
            """
            A pass through generator for i_get_tenth_11 that
            captures the value in a list, which can show the
            order of the generator iteration.

            :param value:
            :return:
            """
            for item in i_get_tenth_11(value):
                called.append(item)
                yield item

        result = yield defer.gatherResults([
            cooperative_accumulate(partial(watcher, list(range(0, 15)))),
            cooperative_accumulate(partial(watcher, list(range(15, 100)))),
            cooperative_accumulate(partial(watcher, list(range(98, 200)))),
            cooperative_accumulate(partial(watcher, list(range(145, 189))))
        ])

        final_result = list(chain.from_iterable(result))

        self.assertEqual(final_result, [10, 11, 25, 26, 108, 109, 155, 156])
        self.assertEqual(set(called), set(final_result))
        self.assertNotEqual(called, final_result)
        #  The iteration is shown to alternate between generators passed
        #   to cooperate.
        self.assertEqual(called, [10, 25, 108, 155, 11, 26, 109, 156])

    @defer.inlineCallbacks
    def test_multi_deux_chain(self):
        """
        Ensure multiple inline callback functions will run cooperatively.

        Ensure the result of gatherResults can be chained together
        in order.

        Ensure cooperatively run generators will complete
        no matter the length.

        Ensure the longest one will continue to iterate after the
        others run out of iterations.

        :return:
        """
        called = []

        def watcher(value):
            """
            A pass through generator for i_get_tenth_11 that
            captures the value in a list, which can show the
            order of the generator iteration.

            :param value:
            :return:
            """
            for item in i_get_tenth_11(value):
                called.append(item)
                yield item

        def deux_watcher(value):
            """
            A pass through generator for i_get_tenth_11 that
            captures the value in a list, which can show the
            order of the generator iteration.

            :param value:
            :return:
            """
            for item in i_get_tenth_11(value):
                called.append(item)
                yield item
            for item in i_get_tenth_11(value):
                called.append(item)
                yield item

        result = yield defer.gatherResults([
            cooperative_accumulate(partial(watcher, list(range(0, 15)))),
            cooperative_accumulate(partial(watcher, list(range(15, 100)))),
            cooperative_accumulate(partial(deux_watcher,
                                           list(range(1098, 10200)))),
            cooperative_accumulate(partial(watcher, list(range(145, 189))))
        ])

        final_result = list(chain.from_iterable(result))

        self.assertEqual(final_result,
                         [10, 11, 25, 26, 1108, 1109, 1108, 1109, 155, 156])
        self.assertEqual(set(called), set(final_result))
        self.assertNotEqual(called, final_result)
        #  The iteration is shown to alternate between generators passed
        #   to cooperate.
        self.assertEqual(called,
                         [10, 25, 1108, 155, 11, 26, 1109, 156, 1108, 1109])
