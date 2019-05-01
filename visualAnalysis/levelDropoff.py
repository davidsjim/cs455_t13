from pyspark import SparkContext
import json
import matplotlib.pyplot as plt
import sys

blacklist = {'no_more_rares', 'bonusat10', 'cap-boost', 'expcontroller', 'starting-prodigy', 'Generous Levels',
             'MoreGenLev', 'adventbuff'}

if __name__ == '__main__':
    archatypes = sys.argv[1].split(',')
    sc = SparkContext()
    rawData = sc.textFile("/cs455/project/data/chars")
    data = rawData.filter(lambda line: len(line) > 1).map(lambda line: json.loads(line)).filter(
        lambda char: 'character' in char).filter(
        lambda char: char['character']['campaign'] == "Maj'Eyal" and char['character']['level'] <= 50).cache()

    addons = data.flatMap(lambda char: char['character']['addons']).map(lambda addon: (addon, 1)).reduceByKey(
        lambda x, y: x + y)
    addonsFiltered = addons.filter(lambda addon: addon[1] > 100 and addon[0] not in blacklist)
    allowedAddons = set(addonsFiltered.map(lambda t: t[0]).collect())

    chars = data.filter(lambda char: set(char['character']['addons']).issubset(allowedAddons)).cache()
    data.unpersist()

    levelData = []
    for archatype in archatypes:
        aChars = chars.filter(lambda char: char['character']['class'] == archatype).cache()
        levels = aChars.flatMap(lambda char: [(l+1, 1) for l in range(char['character']['level'])]).reduceByKey(lambda x, y:x+y).collect()
        count = aChars.count()
        aChars.unpersist()
        plt.plot([l[0] for l in levels], [100*(float(y[1])/float(count)) for y in levels], '--o', label=archatype)

    plt.legend()
    plt.xticks([x+1 for x in range(50)])
    plt.title("Cumulative characters by level")
    plt.xlabel("Level")
    plt.ylabel("Percent of characters to reach level")
    plt.ylim(0,105)
    plt.show()
