from pyspark import SparkContext
import json
import functions
import numpy as np

from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.mllib.clustering import BisectingKMeans, BisectingKMeansModel


def columnizeTalentTree(tree):
    return [int(t['val'].split('/')[0]) for t in tree['list']]

def archerToColumn(char):
    column=[]
    column.append(char['character']['died']['times'] if 'died' in char['character'] else 0)
    column.append(int(char['resources']['life'].split('/')[1]))
    column.append(char['character']['level'])
    for tree in ['Technique / Archery training', "Technique / Archery prowess", "Technique / Combat techniques",
                 "Technique / Combat veteran", "Technique / Marksmanship", "Technique / Reflexes",
                 "Technique / Munitions", "Technique / Agility", "Technique / Sniper", "Cunning / Trapping",
                 "Technique / Combat training", "Technique / Mobility", "Cunning / Survival", "Technique / Conditioning"]:
        column+=columnizeTalentTree(char['talents'][tree]) if tree in char['talents'] else ([0,0,0,0] if tree != "Technique / Combat training" else [0,0,0,0,0,0])
    return column

def normalize(rows):
    desired_max, desired_min=1,-1
    X=np.array(rows)
    numerator = (X - X.min(axis=0))
    numerator*= (desired_max - desired_min)
    denom = X.max(axis=0) - X.min(axis=0)
    denom[denom == 0] = 1
    return (desired_min + numerator / denom).tolist()

if __name__ == '__main__':
    sc = SparkContext()

    data=functions.readData(sc, "/cs455/project/data/chars")
    data=functions.filterByAddons(data)
    data=functions.filterByClass(data).persist()

    archers=data.filter(lambda char: char['character']['class'] == 'Archer').map(archerToColumn)
    archerList=archers.collect()

    levelIndex=2
    levelSets=[[i] for i in range(1,16)]+[[i for i in range(5*j+16, 5*j+21)] for j in range(7)]

    normedRows=[]
    for levelSet in levelSets:
        levelRows=[row for row in archerList if row[levelIndex] in levelSet and len(row) == 61]
        normedRows+=normalize(levelRows)

    normalArchers = sc.parallelize(normedRows)

    model = BisectingKMeans.train(normalArchers, 10, maxIterations=5)
    model.predict(normalArchers)




