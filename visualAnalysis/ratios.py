from pyspark import SparkContext
import json
import matplotlib.pyplot as plt
import sys

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

    if len(sys.argv) > 1:
        archetypes = sys.argv[1].split(',')
    else:
        archetypes = allowedClasses

    chars = data.filter(lambda char: set(char['character']['addons']).issubset(allowedAddons) and char['character']['class'] in allowedClasses).cache()
    data.unpersist()

    usedClasses = chars.map(lambda char: ((char['character']['class'],  char['character']['level']), 1)).reduceByKey(lambda x, y: x+y).cache()
    badPractice = chars.flatMap(lambda char: [(l + 1, 1) for l in range(char['character']['level'])]).reduceByKey(
            lambda x, y: x + y).collect()
    totalChars = chars.count()

    for c in archetypes:
        aChars = usedClasses.filter(lambda char: char[0][0] == c).cache()
        levels = aChars.flatMap(lambda char: [(l + 1, char[1]) for l in range(char[0][1])]).reduceByKey(lambda x, y: x + y).collect()
        count = sum([x[1] for x in aChars.collect()])
        ys = [(float(l[1])/float(count)) /
              (float(sum([x[1] for x in badPractice if x[0] == l[0]])) / float(totalChars)) for l in levels]
        xs = [char[0] for char in levels]
        plt.plot(xs, ys, '--o', label=c)

    plt.xticks([x + 1 for x in range(50)])
    plt.ylim(bottom=0)
    plt.legend()
    plt.show()

