from pyspark import SparkContext
import json
import matplotlib.pyplot as plt
import numpy as np

blacklist = {'no_more_rares', 'bonusat10', 'cap-boost', 'expcontroller', 'starting-prodigy', 'Generous Levels', 'MoreGenLev','adventbuff'}



if __name__ == '__main__':
    sc = SparkContext()
    rawData = sc.textFile("/cs455/project/data/chars")
    data = rawData.filter(lambda line: len(line) > 1).map(lambda line: json.loads(line)).filter(
        lambda char: 'character' in char).filter(lambda char: char['character']['campaign'] == "Maj'Eyal" and char['character']['level'] <= 50).cache()

    addons = data.flatMap(lambda char: char['character']['addons']).map(lambda addon: (addon, 1)).reduceByKey(
        lambda x, y: x + y)
    addonsFiltered = addons.filter(lambda addon: addon[1] > 100 and addon[0] not in blacklist)
    allowedAddons = set(addonsFiltered.map(lambda t: t[0]).collect())

    classes = data.map(lambda char: char['character']['class']).map(lambda addon: (addon, 1)).reduceByKey(
        lambda x, y: x + y)
    classesFiltered = classes.filter(lambda c: c[1] > 300 and c[0] != 'Tutorial Adventurer')
    allowedClasses = set(classesFiltered.map(lambda t: t[0]).collect())

    chars = data.filter(lambda char: set(char['character']['addons']).issubset(allowedAddons) and char['character']['class'] in allowedClasses).cache()
    data.unpersist()

    usedClasses = chars.map(lambda char: ((char['character']['class'],  char['character']['level']), 1)).reduceByKey(lambda x,y:x+y).cache()


    for c in allowedClasses:
        levels = usedClasses.filter(lambda char: char[0][0] == c).collect()
        ys = [char[1] for char in levels]
        xs = [char[0][1] for char in levels]
        plt.plot(xs, ys, 'o', label=c)
    plt.legend()
    plt.show()

    #print(usedClasses.take(1))
    #print(len(usedClasses.take(1)[0][1][1]))



    #classesByLevel = {c: c.map(lambda char: (char[1], 1)).reduceByLey(lambda x,y:x+y).collect() for c in usedClasses.collect()}

    #print(classesByLevel)

