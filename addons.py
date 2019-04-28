from pyspark import SparkContext
import json

if __name__ == '__main__':
    sc = SparkContext()
    rawData = sc.textFile("/cs455/project/data/chars")
    data = rawData.filter(lambda line: len(line) > 1).map(lambda line: json.loads(line)).filter(
        lambda char: 'character' in char).filter(lambda char: char['character']['campaign'] == "Maj'Eyal")
    addons = data.flatMap(lambda char: char['character']['addons']).map(lambda addon: (addon, 1)).reduceByKey(lambda x, y: x + y)
    addonsFiltered = addons.filter(lambda addon: addon[1] > 100)

    allowedAddons = set(addonsFiltered.map(lambda t: t[0]).collect())
    chars = data.filter(lambda char: set(char['character']['addons']).issubset(allowedAddons)).persist()
    charsByLevel = chars.map(lambda char: (char['character']['level'], 1)).reduceByKey(lambda x, y: x + y).collect()

    charsByClass = chars.map(lambda char: (char['character']['class'], 1)).reduceByKey(lambda x, y: x + y).filter(lambda char: char[1] > 300 and char[0] != "Tutorial Adventurer").collect()

    print(chars.count())

    for char in sorted(chars.collect()):
        print(char['character']['name'], char['character']['addons'])
