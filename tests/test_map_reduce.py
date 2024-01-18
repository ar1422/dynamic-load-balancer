import time
import ray

from ray.util.queue import Queue
from coordinator import MapReduceCoordinator
from inputs.sample_data_1 import SAMPLE_DATA_1 as SAMPLE_DATA
from base_actors.base_mapper import map_function
from base_actors.base_reducer import reducer_function

ray.init(ignore_reinit_error=True)

print("Running experiments:")


for (num_mappers, reducers) in [(4, 10)]:
    out_queue = Queue()

    coord = MapReduceCoordinator.options(name="coordinator").remote(SAMPLE_DATA, num_mappers, reducers, map_function, reducer_function, out_queue)
    coord.run.remote()

    while not ray.get(coord.is_done.remote()):
        time.sleep(1)

    print(f"Experiment using {num_mappers} mappers and {reducers} reducers: \n")
    print(f"Running time is: {ray.get(coord.running_time.remote())}")

    ray.kill(coord)
