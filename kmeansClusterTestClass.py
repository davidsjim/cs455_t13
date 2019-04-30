from pyspark import SparkContext
import json
import functions
import numpy as np

from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.mllib.clustering import BisectingKMeans, BisectingKMeansModel
from pyspark.ml.linalg import Vectors

def archerToColumn(char):
    column=[]
    column.append(char['character']['died']['times'] if 'died' in char['character'] else 0)
    column.append(int(char['resources']['life'].split('/')[1]))
    column.append(char['character']['level'])
    for tree in [ "Technique / Archery prowess", "Technique / Combat techniques",
                 "Technique / Combat veteran", "Technique / Marksmanship", "Technique / Reflexes",
                 "Technique / Munitions", "Technique / Agility", "Technique / Sniper", "Cunning / Trapping",
                 "Technique / Combat training", "Technique / Mobility", "Cunning / Survival", "Technique / Conditioning"]:
        column+=columnizeTalentTree(char['talents'][tree]) if tree in char['talents'] else ([0,0,0,0] if tree != "Technique / Combat training" else [0,0,0,0,0,0])
    return column



archerColumn=[
    {'nodes': ['character', 'died', 'times']},
    {'nodes': ['resources', 'life'], 'parse': lambda s: s.split('/')[1]},
    {'nodes': ['character', 'level']},
    {'nodes': ['resources', 'stamina'], 'parse': lambda s: s.split('/')[1]},
    {'nodes': ['offense', 'mainhand', '0', 'crit'], 'parse': lambda s: s.split('/')[1]},

    {'nodes': ['primary stats', 'strength', 'value']},
    {'nodes': ['primary stats', 'magic', 'value']},
    {'nodes': ['primary stats', 'dexterity', 'value']},
    {'nodes': ['primary stats', 'willpower', 'value']},
    {'nodes': ['primary stats', 'cunning', 'value']},
    {'nodes': ['primary stats', 'constitution', 'value']},

    {'nodes': ['defense', 'resistances', 'all'], 'parse': lambda s: s.split('%')[0][1:]},
    {'nodes': ['defense', 'resistances', 'fire'], 'parse': lambda s: s.split('%')[0][1:]},
    {'nodes': ['defense', 'resistances', 'physical'], 'parse': lambda s: s.split('%')[0][1:]},

    {'nodes': ['defense', 'immunities', 'Stun Resistance'], 'parse': lambda s: s[:-1]},
    {'nodes': ['defense', 'immunities', 'Bleed Resistance'], 'parse': lambda s: s[:-1]},
    {'nodes': ['defense', 'immunities', 'Confusion Resistance'], 'parse': lambda s: s[:-1]},
    {'nodes': ['defense', 'immunities', 'Fear Resistance'], 'parse': lambda s: s[:-1]},
    {'nodes': ['defense', 'immunities', 'Poison Resistance'], 'parse': lambda s: s[:-1]},
    {'nodes': ['defense', 'immunities', 'Instadeath Resistance'], 'parse': lambda s: s[:-1]},
    {'nodes': ['defense', 'immunities', 'Blind Resistance'], 'parse': lambda s: s[:-1]},

    {'nodes': ['defense', 'defense', 'armour']},
    {'nodes': ['defense', 'defense', 'armour_hardiness']},


    {'nodes': ['speeds', 'mental']},
    {'nodes': ['speeds', 'attack']},
    {'nodes': ['speeds', 'spell']},
    {'nodes': ['speeds', 'global']},
    {'nodes': ['speeds', 'movement']},


    {'nodes': ['talents', "Technique / Archery prowess", 'list', '0', 'val'], 'label': 'Headshot'},
    {'nodes': ['talents', "Technique / Archery prowess", 'list', '1', 'val'], 'label': 'Volley'},
    {'nodes': ['talents', "Technique / Archery prowess", 'list', '2', 'val'], 'label': 'Called Shots'},
    {'nodes': ['talents', "Technique / Archery prowess", 'list', '3', 'val'], 'label': 'Bulleyes'},

    {'nodes': ['talents', "Technique / Combat techniques", 'list', '0', 'val'], 'label': 'Rush'},
    {'nodes': ['talents', "Technique / Combat techniques", 'list', '1', 'val'], 'label': 'Precise Strikes'},
    {'nodes': ['talents', "Technique / Combat techniques", 'list', '2', 'val'], 'label': 'Perfect Strike'},
    {'nodes': ['talents', "Technique / Combat techniques", 'list', '3', 'val'], 'label': 'Blinding Speed'},

    {'nodes': ['talents', "Technique / Combat veteran", 'list', '0', 'val'], 'label': 'Quick Recovery'},
    {'nodes': ['talents', "Technique / Combat veteran", 'list', '1', 'val'], 'label': 'Fast Metabolism'},
    {'nodes': ['talents', "Technique / Combat veteran", 'list', '2', 'val'], 'label': 'Spell Shield'},
    {'nodes': ['talents', "Technique / Combat veteran", 'list', '3', 'val'], 'label': 'Unending Frenzy'},

    {'nodes': ['talents', "Technique / Marksmanship", 'list', '0', 'val'], 'label': 'Master Marksman'},
    {'nodes': ['talents', "Technique / Marksmanship", 'list', '1', 'val'], 'label': 'First Blood'},
    {'nodes': ['talents', "Technique / Marksmanship", 'list', '2', 'val'], 'label': 'Flare'},
    {'nodes': ['talents', "Technique / Marksmanship", 'list', '3', 'val'], 'label': 'Trueshot'},

    {'nodes': ['talents', "Technique / Reflexes", 'list', '0', 'val'], 'label': 'Shoot Down'},
    {'nodes': ['talents', "Technique / Reflexes", 'list', '1', 'val'], 'label': 'Intuitive Shots'},
    {'nodes': ['talents', "Technique / Reflexes", 'list', '2', 'val'], 'label': 'Sentinel'},
    {'nodes': ['talents', "Technique / Reflexes", 'list', '3', 'val'], 'label': 'Escape'},

    {'nodes': ['talents', "Technique / Munitions", 'list', '0', 'val'], 'label': 'Exotic Munitions'},
    {'nodes': ['talents', "Technique / Munitions", 'list', '1', 'val'], 'label': 'Explosive Shot'},
    {'nodes': ['talents', "Technique / Munitions", 'list', '2', 'val'], 'label': 'Enhanced Munitions'},
    {'nodes': ['talents', "Technique / Munitions", 'list', '3', 'val'], 'label': 'Alloyed Munitions'},

    {'nodes': ['talents', "Technique / Agility", 'list', '0', 'val'], 'label': 'Agile Defense'},
    {'nodes': ['talents', "Technique / Agility", 'list', '1', 'val'], 'label': 'Vault'},
    {'nodes': ['talents', "Technique / Agility", 'list', '2', 'val'], 'label': 'Bull Shot'},
    {'nodes': ['talents', "Technique / Agility", 'list', '3', 'val'], 'label': 'Rapid Shot'},

    {'nodes': ['talents', "Technique / Sniper", 'list', '0', 'val'], 'label': 'Concealment'},
    {'nodes': ['talents', "Technique / Sniper", 'list', '1', 'val'], 'label': 'Shadow Shot'},
    {'nodes': ['talents', "Technique / Sniper", 'list', '2', 'val'], 'label': 'Aim'},
    {'nodes': ['talents', "Technique / Sniper", 'list', '3', 'val'], 'label': 'Snipe'},

    {'nodes': ['talents', "Cunning / Trapping", 'list', '0', 'val'], 'label': 'Trap Mastery'},
    {'nodes': ['talents', "Cunning / Trapping", 'list', '1', 'val'], 'label': 'Lure'},
    {'nodes': ['talents', "Cunning / Trapping", 'list', '2', 'val'], 'label': 'Advanced Trap Deployment'},
    {'nodes': ['talents', "Cunning / Trapping", 'list', '3', 'val'], 'label': 'Trap Priming'},

    {'nodes': ['talents', "Technique / Combat training", 'list', '0', 'val'], 'label': 'Thick Skin'},
    {'nodes': ['talents', "Technique / Combat training", 'list', '1', 'val'], 'label': 'Armour Training'},
    {'nodes': ['talents', "Technique / Combat training", 'list', '2', 'val'], 'label': 'Light Armour Training'},
    {'nodes': ['talents', "Technique / Combat training", 'list', '3', 'val'], 'label': 'Combat Accuracy'},
    {'nodes': ['talents', "Technique / Combat training", 'list', '4', 'val'], 'label': 'Weapons Mastery'},
    {'nodes': ['talents', "Technique / Combat training", 'list', '5', 'val'], 'label': 'Dagger Mastery'},

    {'nodes': ['talents', "Technique / Mobility", 'list', '0', 'val'], 'label': 'Disengage'},
    {'nodes': ['talents', "Technique / Mobility", 'list', '1', 'val'], 'label': 'Evasion'},
    {'nodes': ['talents', "Technique / Mobility", 'list', '2', 'val'], 'label': 'Tumble'},
    {'nodes': ['talents', "Technique / Mobility", 'list', '3', 'val'], 'label': 'Trained Reactions'},

    {'nodes': ['talents', "Cunning / Survival", 'list', '0', 'val'], 'label': 'Heightened Senses'},
    {'nodes': ['talents', "Cunning / Survival", 'list', '1', 'val'], 'label': 'Device Mastery'},
    {'nodes': ['talents', "Cunning / Survival", 'list', '2', 'val'], 'label': 'Track'},
    {'nodes': ['talents', "Cunning / Survival", 'list', '3', 'val'], 'label': 'Danger Sense'},

    {'nodes': ['talents', "Technique / Conditioning", 'list', '0', 'val'], 'label': 'Vitality'},
    {'nodes': ['talents', "Technique / Conditioning", 'list', '1', 'val'], 'label': 'Unflinching Resolve'},
    {'nodes': ['talents', "Technique / Conditioning", 'list', '2', 'val'], 'label': 'Daunting Presence'},
    {'nodes': ['talents', "Technique / Conditioning", 'list', '3', 'val'], 'label': 'Adrenaline Surge'},
]

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



    df = normalArchers.toDF()
    trainingData = df.rdd.map(lambda x: (Vectors.dense(x[0:-1]), x[-1])).toDF(["features", "label"])

    kmeans = KMeans().setK(2).setSeed(1)
    model = kmeans.fit(trainingData)

    # model = BisectingKMeans.train(normalArchers, 10, maxIterations=5)
    # model.predict(normalArchers)




