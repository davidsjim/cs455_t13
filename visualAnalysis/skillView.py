from pyspark import SparkContext
import json
import matplotlib.pyplot as plt
import numpy as np
import sys
import functions as fs

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

    talents = chars.map(lambda char: char['talents']).collect()



    for t in talents:



