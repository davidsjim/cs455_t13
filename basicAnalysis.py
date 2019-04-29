from pyspark import SparkContext
import json



if __name__ == '__main__':
    sc = SparkContext()
    data = readData(sc, "/cs455/project/data/chars")

    charsByLevel = data.map(lambda char: (char['character']['level'], char))
    winsByLevel = charsByLevel.mapValues(lambda char: 1 if 'winner' in char else 0).reduceByKey(lambda x, y: x + y)
    result = winsByLevel.collect()
    print(data.count())
    print(sorted(result))




