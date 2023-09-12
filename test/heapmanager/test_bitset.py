import unittest
import random
from parameterized import parameterized
from dragon.malloc.heapmanager import BitSet


# @MCB TODO: Need more tests and data sets

class BitSetTest(unittest.TestCase):

    @classmethod
    def setUpClass(self) -> None:
        self.max_bits = 1024
        mem_size = BitSet.size(self.max_bits)
        self.memobj = bytearray(mem_size)

    @parameterized.expand([
        ["512", 512, [0, 350, 36, 55]],
        ["1024", 1024, [0, 400, 916, 42]]
    ])
    def test_bitset_handle(self, name: str, num_bits: int, bits_to_set: [int]):
        self.assertTrue(num_bits <= self.max_bits, "Trying to test too many bits")

        bset1 = BitSet.init(num_bits, self.memobj)
        bset2 = BitSet.attach(self.memobj)

        self.assertEqual(bset1.dump_to_str("", ""), bset2.dump_to_str("", ""))

        for bit in bits_to_set:
            bset1.set_bit(bit)
            self.assertEqual(1, bset1.get_bit(bit), "Bit expected to be one, was not")
            self.assertEqual(1, bset2.get_bit(bit), "Bit expected to be one, was not")

        self.assertEqual(bset1.dump_to_str("", ""), bset2.dump_to_str("", ""))

        bset1.reset_bit(bits_to_set[-1])
        self.assertEqual(bset1.right_zeroes(0), bset2.right_zeroes(0))

        for bit in bits_to_set:
            bset2.reset_bit(bit)
            self.assertEqual(0, bset1.get_bit(bit), "Bit expected to be zero, was not")
            self.assertEqual(0, bset2.get_bit(bit), "Bit expected to be zero, was not")

        self.assertEqual(bset1.right_zeroes(0), bset2.right_zeroes(0))

    @parameterized.expand([
        ["32", 32],
        ["64", 64],
        ["128", 128],
        ["256", 256],
        ["512", 512],
        ["1024", 1024]
    ])
    def test_random_set_bit(self, name: str, num_bits: int):
        self.assertTrue(num_bits <= self.max_bits, "Trying to test too many bits")

        # Get a random shuffle of bits to set to 1
        num_ones = random.randint(1, num_bits)
        bits = [0] * (num_bits - num_ones) + [1] * num_ones
        random.shuffle(bits)

        bset = BitSet.init(num_bits, self.memobj)

        for i in range(num_bits):
            if bits[i] == 1:
                bset.set_bit(i)

        for i in range(num_bits):
            self.assertEqual(bits[i], bset.get_bit(i),
                             "Bit expected to be set to {}, is actually set to {}".format(bits[i], bset.get_bit(i)))

        for i in range(num_bits):
            bset.reset_bit(i)

        for i in range(num_bits):
            self.assertEqual(0, bset.get_bit(i), "Bit expected to be zero, was not")


if __name__ == '__main__':
    unittest.main()
