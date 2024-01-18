import argparse
import time
import ray
import collections
from ray.util.queue import Queue

from coordinator import MapReduceCoordinator
from inputs.sample_data_1 import SAMPLE_DATA_1 as SAMPLE_DATA
from executors import map_func, Reducer
from autoscaler import AutoScaler


def tester():
    parser = argparse.ArgumentParser()
    parser.add_argument("--autoscale", action="store_true")
    parser.add_argument("--doubling", action="store_true")
    args = parser.parse_args()
    autoscale = args.autoscale
    ch_type = "doubling" if args.doubling else "halving"
    print(f"Running with autoscale={autoscale} and consistent hashing type={ch_type}")
    ray.init(ignore_reinit_error=True)
    NUM_MAPPERS = 4
    NUM_REDUCERS = 4

    out_queue = Queue()
    reduce_func = Reducer()

    autoscaler = AutoScaler.options(name="autoscaler").remote(
        num_reducers=NUM_REDUCERS, ch_type=ch_type
    )

    coord = MapReduceCoordinator.options(name="coordinator").remote(
        SAMPLE_DATA,
        NUM_MAPPERS,
        NUM_REDUCERS,
        map_func,
        reduce_func,
        out_queue,
        ch_type=ch_type,
        autoscale=autoscale,
    )

    ray.get(coord.run.remote())

    while not ray.get(coord.is_done.remote()):
        time.sleep(5)

    # print(ray.get(autoscaler.autoscaler_state.remote()))
    print(f"Running time is: {ray.get(coord.running_time.remote())}")

    d = collections.defaultdict(int)
    while not out_queue.empty():
        out = out_queue.get()
        print(out)
        for k, v in out.items():
            d[k] += v
    print(len(d))
    print(d)


if __name__ == "__main__":
    tester()