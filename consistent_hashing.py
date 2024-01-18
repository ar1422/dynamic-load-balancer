import mmh3
from typing import Dict
from constants import HASH_SEED_VALUE, INITIAL_TOKENS


class ConsistentHashing(object):

    def __init__(self, nodes, seed=HASH_SEED_VALUE, initial_tokens=INITIAL_TOKENS):

        self.validate_node_value(nodes)
        self.validate_seed_value(seed)

        self.nodes = nodes
        self.seed = seed
        self.initial_tokens = initial_tokens
        self.token_hashes = {}
        self.node_tokens = {index: self.initial_tokens for index in range(self.nodes)}
        self._update_hashes()

    def key_lookup(self, key):
        key_hash = self._hash(key)
        if key_hash > self.max_token_value:
            return self.node_with_smallest_hash
        else:
            self._closest_node_after_key(key_hash)

    def distribute_keyspace(self, node_index):

        if self._all_nodes_have_one_token():
            self.node_tokens = {idx: self.initial_tokens for idx in range(self.nodes)}
            self._update_hashes()

        if self.node_tokens[node_index] > 1:
            self.update(node_index, self.node_tokens[node_index] >> 1)
            return True
        else:
            print("Cannot halve the tokens any further for the node index - {}".format(node_index))
            return False

    def _all_nodes_have_one_token(self):
        return all(token == 1 for token in self.token_hashes.values())

    def update(self, node_index, tokens):

        self._update_node_tokens(node_index, tokens)
        self._update_hashes()

    def add_node(self, tokens):

        self.node_tokens[self.nodes] = tokens
        self.nodes += 1
        self._update_hashes()

    def remove_node(self) -> None:
        assert self.nodes > 1
        del self.node_tokens[self.nodes - 1]
        self.nodes -= 1
        self._update_hashes()

    def update_batch(self, node_tokens: Dict[int, int]) -> None:

        for node_idx, tokens in node_tokens.items():
            self._update_node_tokens(node_idx, tokens)
        self._update_hashes()

    def _hash(self, key: str) -> int:
        return mmh3.hash(key, seed=self.seed)

    def _update_hashes(self) -> None:
        key_format = "{}-{}"
        self.token_hashes = {(node_index, token_index): self._hash(key_format.format(node_index, token_index))
                             for node_index in range(self.nodes)
                             for token_index in range(self.node_tokens[node_index])}

    def _update_node_tokens(self, node_index, tokens):
        self.validate_token_value(tokens)
        self.node_tokens[node_index] = tokens

    @property
    def node_with_smallest_hash(self):
        return min((h, node_idx) for (node_idx, _), h in self.token_hashes.items())[1]

    def _closest_node_after_key(self, key_hash):
        return min((abs(h - key_hash), node_idx) for (node_idx, _), h in self.token_hashes.items() if key_hash < h)[1]

    @property
    def total_tokens(self):
        return sum(self.node_tokens.values())

    @property
    def max_token_value(self):
        return max(self.token_hashes.values())

    @staticmethod
    def validate_node_value(nodes):
        if not isinstance(nodes, int):
            raise TypeError("Node values should be an integer")
        if nodes < 1:
            raise ValueError("Node value should be positive.")

    @staticmethod
    def validate_seed_value(seed):
        if not isinstance(seed, int):
            raise TypeError("Seed values should be an integer")
        if seed < 1:
            raise ValueError("Seed value should be positive.")

    @staticmethod
    def validate_token_value(tokens):
        if not isinstance(tokens, int):
            raise TypeError("Token values should be an integer")
        if tokens < 1 or not isinstance(tokens, int):
            raise ValueError("Token values should be positive.")


class ConsistentHashingDouble(ConsistentHashing):
    def __init__(self, nodes: int, seed: int = HASH_SEED_VALUE):
        super().__init__(nodes=nodes, seed=seed, initial_tokens=1)

    def distribute_keyspace(self, node_index):
        updated_node_tokens = {}
        for index, tokens in self.node_tokens.items():
            if index == node_index:
                updated_node_tokens[index] = tokens
            else:
                updated_node_tokens[index] = 2 * tokens

        self.update_batch(node_tokens=updated_node_tokens)
        return True
