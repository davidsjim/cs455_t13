from pyspark import SparkContext
import json

if __name__ == '__main__':
    sc = SparkContext()

    rawData = sc.textFile("/cs455/project/data/chars")
    data = rawData.filter(lambda line: len(line) > 1).map(lambda line: json.loads(line)).filter(
        lambda char: 'character' in char).filter(lambda char: char['character']['campaign'] == "Maj'Eyal")

    # Filter for allowed addons
    addons = data.flatMap(lambda char: char['character']['addons']).map(lambda addon: (addon, 1)).reduceByKey(lambda x, y: x + y)
    addonsFiltered = addons.filter(lambda addon: addon[1] > 100 and addon[0] not in {'no_more_rares', 'bonusat10', 'cap-boost', 'expcontroller', 'starting-prodigy', 'Generous Levels', 'MoreGenLev','adventbuff'})

    allowedAddons = set(addonsFiltered.map(lambda t: t[0]).collect())
    chars = data.filter(lambda char: set(char['character']['addons']).issubset(allowedAddons)).persist()
    # charsByLevel = chars.map(lambda char: (char['character']['level'], 1)).reduceByKey(lambda x, y: x + y).collect()

    # Filter for allowed classes
    classes = chars.map(lambda char: char['character']['class']).map(lambda charClass: (charClass, 1)).reduceByKey(
        lambda x, y: x + y)
    classesFiltered = classes.filter(lambda charClass: charClass[1] > 300 and charClass[0] != "Tutorial Adventurer")
    allowedClasses = set(classesFiltered.map(lambda t: t[0]).collect())
    chars.unpersist()
    chars = chars.filter(lambda char: char['character']['class'] in allowedClasses).persist()

    charsByClassAndLevel = chars.map(lambda char: ((char['character']['class'], char['character']['level']), 1)).reduceByKey(lambda x, y: x + y).collect()

    print(chars.count())

    for char in sorted(chars.collect()):
        print(char['character']['name'], char['character']['addons'])


