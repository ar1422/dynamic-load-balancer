import ray
from consistent_hashing import ConsistentHashing


@ray.remote
class AutoScaler:
    def __init__(self, num_reducers, ch_type="halving", *args):
        self.reducer_state = {}
        self.reducer_ids = []
        self.mapper_ids = []
        ch_cls = ConsistentHashing if ch_type == "halving" else ConsistentHashing
        self.ch = ch_cls(nodes=num_reducers)
        self.threshold = 300
        self.autoscaled = False

    def register_reducer(self, reducer_id, *args):
        if reducer_id not in self.reducer_ids:
            self.reducer_ids.append(reducer_id)

    def register_mapper(self, mapper_id, *args):
        if mapper_id not in self.mapper_ids:
            self.mapper_ids.append(mapper_id)

    def update_reducer_state(self, reducer_id, queue_size, *args):
        self.reducer_state[reducer_id] = queue_size
        if self.autoscaled:
            return
        if queue_size > min(self.reducer_state.values()) * 2 + 100:
            node_idx = int(reducer_id.split("-")[-1])
            # self.autoscale(reducer_id=reducer_id)
            print(f"halving tokens for {node_idx}")
            self.ch.distribute_keyspace(node_idx)
            self.autoscaled = True

    def key_lookup(self, key):
        return self.ch.key_lookup(key)

    def autoscaler_state(self):
        return self.reducer_state, self.reducer_ids, self.mapper_ids
