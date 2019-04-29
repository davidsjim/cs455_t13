from pyspark import SparkContext
import json
import functions

from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator

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

if __name__ == '__main__':
    sc = SparkContext()

    data=functions.readData(sc, "/cs455/project/data/chars")
    data=functions.filterByAddons(data)
    data=functions.filterByClass(data).persist()

    archers=data.filter(lambda char: char['character']['class'] == 'Archer').map(archerToColumn)
    archerList=archers.collect()

    levelIndex=2
    levels={row[levelIndex] for row in archerList}
    levelSums={level: ([0]*len(archerList[0], 0)) for level in levels}
    for sum in levelSums:
        sum.append([0]*len(archerList[0]))

    for row in archerList:
        level = row[levelIndex]
        for i in range(len(row)):
            if i == levelIndex:
                levelSums[level][i]+=1
                continue
            if level > 50:
                continue
            levelSums[level][0][i] += row[i]
            levelSums[level][1]=levelSums[level][1]+1



