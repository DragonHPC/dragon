""" Test mulitprocessing.Condition
"""
import unittest

from test.support import hashlib_helper

import dragon  # DRAGON import before multiprocessing

import multiprocessing

from common import (
    BaseTestCase,
    ManagerMixin,
    setUpModule,
    tearDownModule,
)


#
#
#


@unittest.skip("DRAGON: Manager not implemented")
@hashlib_helper.requires_hashdigest("md5")
class WithManagerTestContainers(BaseTestCase, ManagerMixin, unittest.TestCase):

    # DRAGON ALLOWED_TYPES = ('manager',)

    def test_list(self):
        a = self.list(list(range(10)))
        self.assertEqual(a[:], list(range(10)))

        b = self.list()
        self.assertEqual(b[:], [])

        b.extend(list(range(5)))
        self.assertEqual(b[:], list(range(5)))

        self.assertEqual(b[2], 2)
        self.assertEqual(b[2:10], [2, 3, 4])

        b *= 2
        self.assertEqual(b[:], [0, 1, 2, 3, 4, 0, 1, 2, 3, 4])

        self.assertEqual(b + [5, 6], [0, 1, 2, 3, 4, 0, 1, 2, 3, 4, 5, 6])

        self.assertEqual(a[:], list(range(10)))

        d = [a, b]
        e = self.list(d)
        self.assertEqual(
            [element[:] for element in e], [[0, 1, 2, 3, 4, 5, 6, 7, 8, 9], [0, 1, 2, 3, 4, 0, 1, 2, 3, 4]]
        )

        f = self.list([a])
        a.append("hello")
        self.assertEqual(f[0][:], [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, "hello"])

    def test_list_iter(self):
        a = self.list(list(range(10)))
        it = iter(a)
        self.assertEqual(list(it), list(range(10)))
        self.assertEqual(list(it), [])  # exhausted
        # list modified during iteration
        it = iter(a)
        a[0] = 100
        self.assertEqual(next(it), 100)

    def test_list_proxy_in_list(self):
        a = self.list([self.list(range(3)) for _i in range(3)])
        self.assertEqual([inner[:] for inner in a], [[0, 1, 2]] * 3)

        a[0][-1] = 55
        self.assertEqual(a[0][:], [0, 1, 55])
        for i in range(1, 3):
            self.assertEqual(a[i][:], [0, 1, 2])

        self.assertEqual(a[1].pop(), 2)
        self.assertEqual(len(a[1]), 2)
        for i in range(0, 3, 2):
            self.assertEqual(len(a[i]), 3)

        del a

        b = self.list()
        b.append(b)
        del b

    def test_dict(self):
        d = self.dict()
        indices = list(range(65, 70))
        for i in indices:
            d[i] = chr(i)
        self.assertEqual(d.copy(), dict((i, chr(i)) for i in indices))
        self.assertEqual(sorted(d.keys()), indices)
        self.assertEqual(sorted(d.values()), [chr(i) for i in indices])
        self.assertEqual(sorted(d.items()), [(i, chr(i)) for i in indices])

    def test_dict_iter(self):
        d = self.dict()
        indices = list(range(65, 70))
        for i in indices:
            d[i] = chr(i)
        it = iter(d)
        self.assertEqual(list(it), indices)
        self.assertEqual(list(it), [])  # exhausted
        # dictionary changed size during iteration
        it = iter(d)
        d.clear()
        self.assertRaises(RuntimeError, next, it)

    def test_dict_proxy_nested(self):
        pets = self.dict(ferrets=2, hamsters=4)
        supplies = self.dict(water=10, feed=3)
        d = self.dict(pets=pets, supplies=supplies)

        self.assertEqual(supplies["water"], 10)
        self.assertEqual(d["supplies"]["water"], 10)

        d["supplies"]["blankets"] = 5
        self.assertEqual(supplies["blankets"], 5)
        self.assertEqual(d["supplies"]["blankets"], 5)

        d["supplies"]["water"] = 7
        self.assertEqual(supplies["water"], 7)
        self.assertEqual(d["supplies"]["water"], 7)

        del pets
        del supplies
        self.assertEqual(d["pets"]["ferrets"], 2)
        d["supplies"]["blankets"] = 11
        self.assertEqual(d["supplies"]["blankets"], 11)

        pets = d["pets"]
        supplies = d["supplies"]
        supplies["water"] = 7
        self.assertEqual(supplies["water"], 7)
        self.assertEqual(d["supplies"]["water"], 7)

        d.clear()
        self.assertEqual(len(d), 0)
        self.assertEqual(supplies["water"], 7)
        self.assertEqual(pets["hamsters"], 4)

        l = self.list([pets, supplies])
        l[0]["marmots"] = 1
        self.assertEqual(pets["marmots"], 1)
        self.assertEqual(l[0]["marmots"], 1)

        del pets
        del supplies
        self.assertEqual(l[0]["marmots"], 1)

        outer = self.list([[88, 99], l])
        self.assertIsInstance(outer[0], list)  # Not a ListProxy
        self.assertEqual(outer[-1][-1]["feed"], 3)

    def test_namespace(self):
        n = self.Namespace()
        n.name = "Bob"
        n.job = "Builder"
        n._hidden = "hidden"
        self.assertEqual((n.name, n.job), ("Bob", "Builder"))
        del n.job
        self.assertEqual(str(n), "Namespace(name='Bob')")
        self.assertTrue(hasattr(n, "name"))
        self.assertTrue(not hasattr(n, "job"))


# DRAGON
if __name__ == "__main__":
    setUpModule()
    unittest.main()
    tearDownModule()
