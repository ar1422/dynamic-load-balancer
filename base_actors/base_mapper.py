class Mapper(object):

    def __init__(self):
        self.total_processed = 0

    def map(self, element):
        self.total_processed += 1
        return element

    def done(self):
        print("Total processed = {}".format(self.total_processed))


def map_function():
    return Mapper()
