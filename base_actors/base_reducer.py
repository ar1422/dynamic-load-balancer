import time

SLEEP_TIME = 0.1


class Reducer(object):
    def __init__(self):
        self.state = {}
        self.total_processed = 0

    def reduce(self, element):

        time.sleep(SLEEP_TIME)
        self.state[element] = self.state.get(element, 0) + 1
        self.total_processed += 1

    def done(self):
        print("Total processed = {}".format(self.total_processed))
        return self.state


def reducer_function():
    return Reducer()
