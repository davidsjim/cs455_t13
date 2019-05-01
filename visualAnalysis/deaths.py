from pyspark import SparkContext
import json
import matplotlib.pyplot as plt

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

    levels = chars.map(lambda char: (char['character']['level'], (char['character']['died']['times'], 1))).reduceByKey(lambda x, y: (x[0]+ y[0], x[1]+y[1])).collect()

    plt.plot([x[0] for x in levels], [(c[1][0], c[1][1]) for c in levels])
    plt.legend(["Deaths", "Characters"])
    plt.title("Character deaths by level")
    plt.ylabel("Number of characters/Deaths")
    plt.xlabel("Level")
    plt.show()