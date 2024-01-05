import mmh3

from typing import Dict
from constants import HASH_SEED_VALUE, INITIAL_TOKENS


class ConsistentHashing:

    def __init__(self, nodes: int, hash_func=mmh3.hash, seed: int = HASH_SEED_VALUE, tokens_initial: int = INITIAL_TOKENS):

        self._validate_node(nodes)
        self.nodes = nodes
        self.seed = seed
        self.tokens_initial = tokens_initial
        self.token_hashes = {}
        self._hash_func = hash_func
        self.node_tokens = {idx: self.tokens_initial for idx in range(self.nodes)}
        self._update_hashes()

    @staticmethod
    def _validate_node(nodes):
        if not isinstance(nodes, int):
            raise TypeError("Number of Nodes should be integer.")

        if nodes < 1:
            raise ValueError("Number of nodes should be positive.")

    @staticmethod
    def _validate_node_tokens(tokens):
        if not isinstance(tokens, int):
            raise TypeError("Tokens should be integer.")

        if tokens < 1:
            raise ValueError("tokens should be positive.")

    def _validate_remove_last_node(self):
        if self.nodes == 1:
            raise ValueError("Current nodes = 1. Cannot remove when there is only one node left.")

    @property
    def get_max_token_value(self):
        return max(self.token_hashes.values())

    @property
    def total_tokens(self) -> int:
        return sum(self.node_tokens.values())

    @property
    def node_with_smallest_hash(self) -> int:
        return min((h, node_idx) for (node_idx, _), h in self.token_hashes.items())[1]

    def _closest_node_after_key(self, hash_value: int) -> int:
        return min((abs(h - hash_value), node_idx) for (node_idx, _), h in self.token_hashes.items() if hash_value < h)[1]

    def lookup_key(self, key):

        hash_value = self._hash_func(key=key, seed=self.seed)
        if hash_value > self.get_max_token_value:
            return self.node_with_smallest_hash
        else:
            return self._closest_node_after_key(hash_value)

    def halve_tokens_for_node(self, node_idx: int) -> bool:
        if all(t == 1 for t in self.token_hashes.values()):
            self.node_tokens = {idx: self.tokens_initial for idx in range(self.nodes)}
            self._update_hashes()

        if self.node_tokens[node_idx] > 1:
            self.update(node_idx=node_idx, tokens=self.node_tokens[node_idx] >> 1)
            return True
        else:
            print(f"can't halve the tokens for node index {node_idx}")
            return False

    def update(self, node_idx: int, tokens: int) -> None:

        self._update_node_tokens(node_idx, tokens)
        self._update_hashes()

    def add_node(self, tokens: int) -> None:

        self.nodes += 1
        self.node_tokens[self.nodes - 1] = tokens
        self._update_hashes()

    def remove_last_node(self) -> None:

        self._validate_remove_last_node()
        del self.node_tokens[self.nodes - 1]
        self.nodes -= 1
        self._update_hashes()

    def update_batch(self, node_tokens: Dict[int, int]) -> None:

        for node_idx, tokens in node_tokens.items():
            self._update_node_tokens(node_idx, tokens)
        self._update_hashes()

    def _update_hashes(self) -> None:
        self.token_hashes = {(node, token): self._hash_func(key=f"{node}-{token}", seed=self.seed)
                             for node in range(self.nodes)
                             for token in range(self.node_tokens[node])
                             }

    def _update_node_tokens(self, node, tokens) -> None:
        self._validate_node_tokens(tokens)
        self.node_tokens[node] = tokens
