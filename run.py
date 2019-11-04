from pyspark import SparkContext, SparkConf
import random

def inside(r):
    x, y = random.random(), random.random()
    return x * x + y * y < 1

class RandomFilter(object):
    def __init__(self, sc, num_samples=100):
        self.sc = sc
        self.num_samples = num_samples

    def run(self):
        return self.sc.parallelize(range(0, self.num_samples)).filter(inside).count()

if __name__ == '__main__':
    conf = SparkConf().setAppName("calculate_pyspark_example")
    with SparkContext(conf=conf) as sc:
        print("Random count:", RandomFilter(sc, num_samples=100000).run())
        