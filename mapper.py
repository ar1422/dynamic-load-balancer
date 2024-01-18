import ray
from consistent_hashing import ConsistentHashing


@ray.remote
class Mapper:
    def __init__(self, mapper, name, coordinator_name, reducer_queues, autoscale=False, *args):
        self.mapper = mapper
        self.name = name
        self.reducer_queues = reducer_queues
        self.coordinator_name = coordinator_name
        self.done = False
        self.ch = ConsistentHashing(nodes=len(self.reducer_queues))
        self.autoscale = autoscale

        self.autoscaler = ray.get_actor("autoscaler")
        self.autoscaler.register_mapper.remote(self.name)

    def add_to_queue(self, data):
        output = self.mapper[data]
        index = ray.get(self.autoscaler.key_lookup.remote(output))
        self.reducer_queues[index].put(output)

    def process(self):
        coordinator = ray.get_actor(self.coordinator_name)

        while True:
            data = ray.get(coordinator.mapper_input.remote())
            if data is not None:
                self.add_to_queue(data)
            else:
                break

        for reducer_queue in self.reducer_queues:
            reducer_queue.put(None)

        self.done = True

    def reschedule_output(self, node_idx):
        self.ch.distribute_keyspace(node_idx)

    def done(self):
        return self.done
