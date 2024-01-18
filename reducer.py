import ray
from ray.util.queue import Empty


@ray.remote
class Reducer:
    def __init__(self, reducer, name, coordinator_name, input_queue, other_reducer_input_queues, output_queue, autoscale=False, *args,):
        self.reducer = reducer
        self.name = name
        self.id = int(name.split("-")[1])
        self.input_queue = input_queue
        self.reducer_queues = other_reducer_input_queues
        self.output_queue = output_queue
        self.coordinator_name = coordinator_name
        self.autoscale = autoscale
        self.done = False

        if self.autoscale:
            self.autoscaler = ray.get_actor("autoscaler")
            self.autoscaler.register_reducer.remote(self.name)

    def process(self):
        coordinator = ray.get_actor(self.coordinator_name)
        counter = 0
        while True:
            try:
                data = self.input_queue.get(block=False)
            except Empty:
                if ray.get(coordinator.can_die.remote()):
                    print("reducer dying")
                    break
                continue

            counter += 1
            if counter % 20 == 0 and self.autoscale:
                self.update_auto_scaler_state()
            if self.autoscale and data is not None:
                idx = ray.get(self.autoscaler.key_lookup.remote(data))
                if idx != self.id:
                    print("forwarding")
                    try:
                        print("before ", self.reducer_queues[idx].size())
                        self.reducer_queues[idx].put(data)
                        print("after ", self.reducer_queues[idx].size())
                    except:
                        print(data)
                        raise Exception
                    continue

            if data is None:
                ray.get(coordinator.increment_none_count.remote())
                continue
            output = self.reducer.execute(data)
            if output is not None:
                self.output_queue.put(output)

        output = self.reducer.done()
        if output is not None:
            self.output_queue.put(output)

        coordinator.register_reducer.remote()
        self.done = True

    def update_auto_scaler_state(self):
        print("updating autoscaler:", self.input_queue.size())
        self.autoscaler.update_reducer_state.remote(self.name, self.input_queue.size())

    def done(self):
        return self.done
