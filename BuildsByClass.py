from pyspark import SparkContext
import functions as fs

if __name__ == '__main__':
    sc = SparkContext()
    chars = fs.filterByAddons(fs.readData(sc, "/cs455/project/data/chars"))
