from functools import partial
from itertools import islice
from operator import methodcaller
import unittest
import types

from karld.iter_utils import i_batch
from karld.iter_utils import yield_getter_of
from karld.iter_utils import yield_nth_of


class TestIBatch(unittest.TestCase):
    """
    Ensure i_batch returns a generator that yields
     a tuple of values from the iterable, in order
     in batches no greater than the max_size. The
     items of the iterable are consumed in the amount
     of each batch yielded.
    """
    def setUp(self):
        self.items = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
        self.iterable_items = iter(self.items)

    def test_i_batch_returns_generator(self):
        """
        Ensure when i_batch returns a value, it is
        a generator and none of the iterable have
        been consumed.
        """
        batches = i_batch(3, self.iterable_items)
        self.assertIsInstance(batches, types.GeneratorType)
        self.assertEqual(tuple([0, 1, 2, 3, 4, 5, 6, 7, 8, 9]),
                         tuple(self.iterable_items))

    def test_next_after_end_stops(self):
        """
        Ensure StopIteration is raised if next
        is called on the batches once all
        batches are yielded.
        """
        batches = i_batch(3, self.iterable_items)
        batch_items = tuple(islice(batches, 4))
        self.assertEqual(4, len(batch_items))
        self.assertRaises(StopIteration, next, batches)


    def test_iterable_value_order_maintained(self):
        """
        Ensure the value order is maintained from iterable
        to batches when consumed out of order.


        Ensure i_batch yields only the amount of batches
        that the number of items of an iterable
        divide by max_size into.
        """
        v1, v2, v3, v4 = i_batch(3, self.iterable_items)

        self.assertEqual((0, 1, 2), v1)
        self.assertEqual((3, 4, 5), v2)
        self.assertEqual((6, 7, 8), v3)
        self.assertEqual((9,), v4)

    def test_list_value_order_maintained(self):
        """
        Ensure the value order is maintained from iterable
        as a list to batches when consumed out of order.
        """
        v1, v2, v3, v4 = i_batch(3, self.items)

        self.assertEqual((0, 1, 2), v1)
        self.assertEqual((3, 4, 5), v2)
        self.assertEqual((6, 7, 8), v3)
        self.assertEqual((9,), v4)

    def test_iterable_batch_size_consumed(self):
        """
        Ensure the items of the iterable are consumed
        as yielded by the generator.
        """
        batches = i_batch(6, self.iterable_items)
        v1 = next(batches)
        self.assertEqual((0, 1, 2, 3, 4, 5), v1)
        self.assertEqual((6, 7, 8, 9), tuple(self.iterable_items))


class TestYielders(unittest.TestCase):
    def test_yield_getter_of(self):
        """
        Ensure yield_getter_of with return a generator
         of the values of an iterator as gotten with
         a getter.
        """
        data = iter(("hello", "world"))
        upper_generator = yield_getter_of(partial(methodcaller, 'upper'),
                                          data)
        self.assertEqual(("HELLO", "WORLD"), tuple(upper_generator))

    def test_yield_nth_of(self):
        """
        Ensure yield_nth_of will return a generator of
         the nth values of each item of an iterator.
        """
        data = iter((("hello", "world"),
                    ("alice", "pin")))
        self.assertEqual(("world", "pin"), tuple(yield_nth_of(1, data)))
