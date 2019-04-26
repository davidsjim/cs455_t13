from pyspark import SparkContext
import json

if __name__ == '__main__':
    sc = SparkContext()
    rawData = sc.textFile("/cs455/project/data/chars")
    data = rawData.filter(lambda line: len(line) > 1).map(lambda line: json.loads(line)).filter(
        lambda char: 'character' in char).filter(lambda char: char['character']['campaign'] == "Maj'Eyal")
    charsByLevel = data.map(lambda char: (char['character']['level'], char))
    winsByLevel = charsByLevel.mapValues(lambda char: 1 if 'winner' in char else 0).reduceByKey(lambda x, y: x + y)
    result = winsByLevel.collect()
    print(data.count())
    print(sorted(result))
