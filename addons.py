from pyspark import SparkContext
import json

if __name__ == '__main__':
    sc = SparkContext()
    rawData = sc.textFile("/cs455/project/data/chars")
    data = rawData.filter(lambda line: len(line) > 1).map(lambda line: json.loads(line)).filter(
        lambda char: 'character' in char).filter(lambda char: char['character']['campaign'] == "Maj'Eyal")
    addons = data.flatMap(lambda char: char['character']['addons']).map(lambda addon: (addon, 1)).reduceByKey(lambda x, y: x + y)
    addonsFiltered = addons.filter(lambda addon: addon[1] > 100)

    allowedAddons = set(addonsFiltered.collect())

    chars = data.filter(lambda char: set(char['character']['addons']).issubset(allowedAddons))
    charsByLevel = chars.map(lambda char: (char['character']['level'], char)).mapValues(lambda x: 1).reduceByKey(lambda x, y: x + y).collect()
    print(chars.count())

    for char in sorted(chars.collect()):
        print(char['character']['name'], char['character']['addons'])
