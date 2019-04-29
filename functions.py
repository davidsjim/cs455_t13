import json
from pyspark import SparkContext

def filterByAddons(rdd):
    addons = rdd.flatMap(lambda char: char['character']['addons']).map(lambda addon: (addon, 1)).reduceByKey(
        lambda x, y: x + y)
    addonsFiltered = addons.filter(lambda addon: addon[1] > 100)

    allowedAddons = set(addonsFiltered.map(lambda t: t[0]).collect())
    chars = rdd.filter(lambda char: set(char['character']['addons']).issubset(allowedAddons))
    return chars

def filterByClass(rdd):
    classes = rdd.flatMap(lambda char: char['character']['class']).map(lambda addon: (addon, 1)).reduceByKey(
        lambda x, y: x + y)
    classesFiltered = classes.filter(lambda c: c[1] > 300 and c[0] != 'Tutorial Adventurer')

    allowedClasses = set(classesFiltered.map(lambda t: t[0]).collect())
    chars = rdd.filter(lambda char: set(char['character']['class']).issubset(allowedClasses))
    return chars

def readData(sc, filename):
    rawData = sc.textFile(filename)
    data = rawData.filter(lambda line: len(line) > 1).map(lambda line: json.loads(line)).filter(
        lambda char: 'character' in char).filter(lambda char: char['character']['campaign'] == "Maj'Eyal")
    return data

def splitByClass(rdd):
    classes = rdd.map(lambda char: (char['character']['class'], char)).combineByKey(
        lambda v : [v],
        lambda x, n : x + [n],
        lambda x, y : x + y
    ).filter(lambda c : len(c[1]) > 300 and c[0] != 'Tutorial Adventurer')
    return classes

def splitByRace(rdd):
    races = rdd.map(lambda char: (char['character']['race'], char)).combineByKey(
        lambda v : [v],
        lambda x, n : x + [n],
        lambda x, y : x + y
    ).filter(lambda c : len(c[1]) > 300 and c[0] != 'Tutorial Basic')
    return races


if __name__ == '__main__':
    sc = SparkContext()
    data = splitByClass(filterByAddons(readData(sc, "/cs455/project/data/chars")))

