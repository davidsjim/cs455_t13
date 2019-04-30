from pyspark import SparkContext
import json
import matplotlib.pyplot as plt
import sys
import re

blacklist = {'no_more_rares', 'bonusat10', 'cap-boost', 'expcontroller', 'starting-prodigy', 'Generous Levels',
             'MoreGenLev', 'adventbuff'}

if __name__ == '__main__':
    archatype = sys.argv[1]
    sc = SparkContext()
    rawData = sc.textFile("/cs455/project/data/chars")
    data = rawData.filter(lambda line: len(line) > 1).map(lambda line: json.loads(line)).filter(
        lambda char: 'character' in char).filter(
        lambda char: char['character']['campaign'] == "Maj'Eyal" and char['character']['level'] <= 50).cache()

    addons = data.flatMap(lambda char: char['character']['addons']).map(lambda addon: (addon, 1)).reduceByKey(
        lambda x, y: x + y)
    addonsFiltered = addons.filter(lambda addon: addon[1] > 100 and addon[0] not in blacklist)
    allowedAddons = set(addonsFiltered.map(lambda t: t[0]).collect())

    chars = data.filter(lambda char: set(char['character']['addons']).issubset(allowedAddons) and char['character'][
        'class'] == archatype).cache()
    data.unpersist()

    talents = chars.flatMap(lambda char: [char['talents'][talent] for talent in char['talents']])

    lists = talents.flatMap(lambda talent: talent['list']).map(
        lambda talent: (talent['name'], int(talent['val'].split("/")[0]))).reduceByKey(
        lambda x, y: x + y).collect()

    num = chars.count()
    for talent in sorted(lists, key=lambda x: x[1]):
        if re.match('[A-Za-z]', talent[0]):
            print(talent[0], talent[1], float(talent[1])/float(num))


    plt.plot([x[1] for x in sorted(lists, key=lambda x: x[1], reverse=True)[:30]], 'o')
    plt.xticks([x for x in range(30)],[x[0] for x in sorted(lists, key=lambda x: x[1], reverse=True)[:30]], rotation = 30)
    plt.title("Top 40 "+archatype+" skills")
    plt.show()

