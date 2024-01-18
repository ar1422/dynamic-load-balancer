import ray
import time

from mapper import Mapper
from ray.util.queue import Queue
from reducer import Reducer
from threading import Lock


@ray.remote
class MapReduceCoordinator:
    def __init__(self, input_data, num_mappers, num_reducers, mapper, reducer, output_queue, ch_type="halving", autoscale=False):
        self.input_data = input_data

        self.start_time = 0
        self.running_time = 0

        self.mapper = mapper
        self.reducer = reducer

        self.num_mappers = num_mappers
        self.num_reducers = num_reducers

        self.done_mappers = 0
        self.done_reducers = 0

        self.finished = False

        self.output_queue = output_queue

        self.autoscale = autoscale
        self.ch_type = ch_type
        self.counter = 0
        self.counter_lock = Lock()

        self.reducer_queues = []

    def increment_none_count(self):
        self.counter_lock.acquire()
        self.counter += 1
        print(self.counter)
        self.counter_lock.release()

    def can_die(self):
        sizes = [i.size() for i in self.reducer_queues]
        return (self.counter == self.num_mappers * self.num_reducers) and (
            sum(sizes) == 0
        )

    def initialize_reducers(self):

        reducers = []
        reducer_queues = self.reducer_queues

        for i in range(self.num_reducers):
            input_queue = Queue()
            reducer_queues.append(input_queue)

        for i in range(self.num_reducers):
            name = f"reducer-{i}"
            reducer_obj = Reducer.options(name=name, max_concurrency=1).remote(self.reducer, name, "coordinator",
                                                                               reducer_queues[i], reducer_queues, self.output_queue, autoscale=self.autoscale)
            reducers.append(reducer_obj)

        return reducers, reducer_queues

    def initialize_mappers(self, reducer_queues):
        mappers = []

        for i in range(self.num_mappers):
            name = f"mapper-{i}"
            mapper_obj = Mapper.options(name=name).remote(self.mapper, name, "coordinator", reducer_queues, ch_type=self.ch_type, autoscale=self.autoscale)

            mappers.append(mapper_obj)

        return mappers

    def run(self):

        reducers, reducer_queues = self.initialize_reducers()
        mappers = self.initialize_mappers(reducer_queues)

        self.start_time = time.perf_counter()

        for reducer in reducers:
            reducer.process.remote()

        for mapper in mappers:
            mapper.process.remote()

    def register_reducer(self):
        self.done_reducers += 1
        if self.done_reducers == self.num_reducers:
            self.running_time = time.perf_counter() - self.start_time

            self.finished = True
            self.done_reducers = 0

    def mapper_input(self):
        if len(self.input_data) > 0:
            return self.input_data.pop(0)
        return None

    def running_time(self):
        return self.running_time

    def is_done(self):
        return self.finished
