import unittest
import pandas as pd

from consistent_hashing import ConsistentHashing, ConsistentHashingDouble


class TestConsistentHashing(unittest.TestCase):
    @staticmethod
    def _stats(ch):
        print(pd.Series([ch.key_lookup(key=str(idx)) for idx in range(10_000)]).value_counts(normalize=True))

    def test_ch(self):
        ch = ConsistentHashing(nodes=5)
        self._stats(ch)
        ch.distribute_keyspace(2)
        self._stats(ch)
        ch.distribute_keyspace(2)
        self._stats(ch)
        ch.distribute_keyspace(2)
        self._stats(ch)

    def test_ch_double(self):
        ch = ConsistentHashingDouble(nodes=3)
        assert ch.total_tokens == ch.nodes == 3
        self._stats(ch)
        ch.update(0, 10)
        assert ch.total_tokens == 12
        self._stats(ch)
        ch.update(2, 50)
        self._stats(ch)
        ch.update_batch({0: 20, 1: 5, 2: 1})
        assert ch.nodes == 3
        self._stats(ch)
        ch.add_node(tokens=10)
        assert ch.nodes == 4
        self._stats(ch)
        ch.remove_node()
        ch.remove_node()
        ch.remove_node()
        assert ch.nodes == 1
        self._stats(ch)

    def test_ch_double_halving(self):
        ch = ConsistentHashingDouble(nodes=3)
        self._stats(ch)
        ch.distribute_keyspace(1)
        self._stats(ch)
