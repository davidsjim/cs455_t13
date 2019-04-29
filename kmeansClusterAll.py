from pyspark import SparkContext
import json

from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator

if __name__ == '__main__':
    sc = SparkContext()
    rawData = sc.textFile("/cs455/project/data/chars")

    # Filter characters for only Maj Eyal Campaign
    data = rawData.filter(lambda line: len(line) > 1).map(lambda line: json.loads(line)).filter(
        lambda char: 'character' in char).filter(lambda char: char['character']['campaign'] == "Maj'Eyal")

    # Valid addons are addons where 100 or more characters use the addon
    # Filter by only valid addon characters
    addons = data.flatMap(lambda char: char['character']['addons']).map(lambda addon: (addon, 1)).reduceByKey(lambda x, y: x + y)
    addonsFiltered = addons.filter(lambda addon: addon[1] > 100)

    allowedAddons = set(addonsFiltered.map(lambda t: t[0]).collect())
    chars = data.filter(lambda char: set(char['character']['addons']).issubset(allowedAddons)).persist()

    # Only get classes with more than 300 characters to remove custom classes
    charsByClass = chars.map(lambda char: (char['character']['class'], char)).filter(lambda char: char[1] > 300 and char[0] != "Tutorial Adventurer").collect()

