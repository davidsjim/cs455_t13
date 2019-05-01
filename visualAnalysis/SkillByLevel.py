from pyspark import SparkContext
import json
import matplotlib.pyplot as plt
import sys

blacklist = {'no_more_rares', 'bonusat10', 'cap-boost', 'expcontroller', 'starting-prodigy', 'Generous Levels',
             'MoreGenLev', 'adventbuff'}

if __name__ == '__main__':
    archatype = sys.argv[1]
    skill = sys.argv[2].split(";")
    sc = SparkContext()
    rawData = sc.textFile("/cs455/project/data/chars")
    data = rawData.filter(lambda line: len(line) > 1).map(lambda line: json.loads(line)).filter(
        lambda char: 'character' in char).filter(
        lambda char: char['character']['campaign'] == "Maj'Eyal" and char['character']['level'] <= 50).cache()

    addons = data.flatMap(lambda char: char['character']['addons']).map(lambda addon: (addon, 1)).reduceByKey(lambda x, y: x + y)
    addonsFiltered = addons.filter(lambda addon: addon[1] > 100 and addon[0] not in blacklist)
    allowedAddons = set(addonsFiltered.map(lambda t: t[0]).collect())

    chars = data.filter(lambda char: set(char['character']['addons']).issubset(allowedAddons) and char['character']['class'] == archatype).filter(
        lambda char: skill[0] in char['talents']).cache()
    data.unpersist()

    talents = chars.map(lambda char: (char['character']['level'], (int(char['talents'][skill[0]]['list'][int(skill[1])]['val'].split('/')[0]), 1))).reduceByKey(
        lambda x, y: (x[0]+y[0], x[1]+y[1])).cache()

    averageTalents = talents.mapValues(lambda l: float(l[0])/float(l[1])).collect()
    levelChars = talents.mapValues(lambda l: l[1]).collect()

    if len(averageTalents) < 1:
        print("Something went wrong")
        exit()

    fig, ax1 = plt.subplots(figsize=(15, 10))
    ax2 = ax1.twinx()

    ax1.bar([x[0] for x in averageTalents], [y[1] for y in averageTalents], color='xkcd:bluegrey')

    ax2.plot([x[0] for x in levelChars], [y[1] for y in levelChars], '--o', color='r')
    ax2.set_ylabel("Number of characters")
    ax1.set_ylim(0, 5.1)
    ax2.set_yscale("log")

    plt.title("Average number of points in " + chars.take(1)[0]['talents'][skill[0]]['list'][int(skill[1])]['name'] + " for all characters in the " + archatype + " class")
    ax1.set_xlabel("Level")
    ax1.set_ylabel("Average skill points")
    plt.xticks([x+1 for x in range(50)])
    plt.show()