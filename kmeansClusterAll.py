from pyspark import SparkContext
import json
import functions
import numpy as np
import classColumns


from pyspark.mllib.clustering import BisectingKMeans, BisectingKMeansModel

def talentValParser(talentValString):
    return int(talentValString.split('/')[0])

archerColumns=[
    {'nodes': ['character', 'died', 'times']},
    {'nodes': ['resources', 'life'], 'parse': lambda s: int(s.split('/')[1])},
    {'nodes': ['character', 'level']},
    {'nodes': ['resources', 'stamina'], 'parse': lambda s: int(s.split('/')[1])},
    {'nodes': ['offense', 'mainhand', '0', 'crit'], 'parse': lambda s: int(s.split('/')[1])},

    {'nodes': ['primary stats', 'strength', 'value'], 'label': 'strength'},
    {'nodes': ['primary stats', 'magic', 'value'], 'label': 'magic'},
    {'nodes': ['primary stats', 'dexterity', 'value'], 'label': 'dexterity'},
    {'nodes': ['primary stats', 'willpower', 'value'], 'label': 'willpower'},
    {'nodes': ['primary stats', 'cunning', 'value'], 'label': 'cunning'},
    {'nodes': ['primary stats', 'constitution', 'value'], 'label': 'constitution'},

    {'nodes': ['defense', 'resistances', 'all'], 'parse': lambda s: int(s.split('%')[0][1:])},
    {'nodes': ['defense', 'resistances', 'fire'], 'parse': lambda s: int(s.split('%')[0][1:])},
    {'nodes': ['defense', 'resistances', 'physical'], 'parse': lambda s: int(s.split('%')[0][1:])},

    {'nodes': ['defense', 'immunities', 'Stun Resistance'], 'parse': lambda s: int(s[:-1])},
    {'nodes': ['defense', 'immunities', 'Bleed Resistance'], 'parse': lambda s: int(s[:-1])},
    {'nodes': ['defense', 'immunities', 'Confusion Resistance'], 'parse': lambda s: int(s[:-1])},
    {'nodes': ['defense', 'immunities', 'Fear Resistance'], 'parse': lambda s: int(s[:-1])},
    {'nodes': ['defense', 'immunities', 'Poison Resistance'], 'parse': lambda s: int(s[:-1])},
    {'nodes': ['defense', 'immunities', 'Instadeath Resistance'], 'parse': lambda s: int(s[:-1])},
    {'nodes': ['defense', 'immunities', 'Blind Resistance'], 'parse': lambda s: int(s[:-1])},

    {'nodes': ['defense', 'defense', 'armour']},
    {'nodes': ['defense', 'defense', 'armour_hardiness']},


    {'nodes': ['speeds', 'mental']},
    {'nodes': ['speeds', 'attack']},
    {'nodes': ['speeds', 'spell']},
    {'nodes': ['speeds', 'global']},
    {'nodes': ['speeds', 'movement']},


    {'nodes': ['talents', "Technique / Archery prowess", 'list', 0, 'val'], 'label': 'Headshot', 'parse': talentValParser},
    {'nodes': ['talents', "Technique / Archery prowess", 'list', 1, 'val'], 'label': 'Volley', 'parse': talentValParser},
    {'nodes': ['talents', "Technique / Archery prowess", 'list', 2, 'val'], 'label': 'Called Shots', 'parse': talentValParser},
    {'nodes': ['talents', "Technique / Archery prowess", 'list', 3, 'val'], 'label': 'Bulleyes', 'parse': talentValParser},

    {'nodes': ['talents', "Technique / Combat techniques", 'list', 0, 'val'], 'label': 'Rush', 'parse': talentValParser},
    {'nodes': ['talents', "Technique / Combat techniques", 'list', 1, 'val'], 'label': 'Precise Strikes', 'parse': talentValParser},
    {'nodes': ['talents', "Technique / Combat techniques", 'list', 2, 'val'], 'label': 'Perfect Strike', 'parse': talentValParser},
    {'nodes': ['talents', "Technique / Combat techniques", 'list', 3, 'val'], 'label': 'Blinding Speed', 'parse': talentValParser},

    {'nodes': ['talents', "Technique / Combat veteran", 'list', 0, 'val'], 'label': 'Quick Recovery', 'parse': talentValParser},
    {'nodes': ['talents', "Technique / Combat veteran", 'list', 1, 'val'], 'label': 'Fast Metabolism', 'parse': talentValParser},
    {'nodes': ['talents', "Technique / Combat veteran", 'list', 2, 'val'], 'label': 'Spell Shield', 'parse': talentValParser},
    {'nodes': ['talents', "Technique / Combat veteran", 'list', 3, 'val'], 'label': 'Unending Frenzy', 'parse': talentValParser},

    {'nodes': ['talents', "Technique / Marksmanship", 'list', 0, 'val'], 'label': 'Master Marksman', 'parse': talentValParser},
    {'nodes': ['talents', "Technique / Marksmanship", 'list', 1, 'val'], 'label': 'First Blood', 'parse': talentValParser},
    {'nodes': ['talents', "Technique / Marksmanship", 'list', 2, 'val'], 'label': 'Flare', 'parse': talentValParser},
    {'nodes': ['talents', "Technique / Marksmanship", 'list', 3, 'val'], 'label': 'Trueshot', 'parse': talentValParser},

    {'nodes': ['talents', "Technique / Reflexes", 'list', 0, 'val'], 'label': 'Shoot Down', 'parse': talentValParser},
    {'nodes': ['talents', "Technique / Reflexes", 'list', 1, 'val'], 'label': 'Intuitive Shots', 'parse': talentValParser},
    {'nodes': ['talents', "Technique / Reflexes", 'list', 2, 'val'], 'label': 'Sentinel', 'parse': talentValParser},
    {'nodes': ['talents', "Technique / Reflexes", 'list', 3, 'val'], 'label': 'Escape', 'parse': talentValParser},

    {'nodes': ['talents', "Technique / Munitions", 'list', 0, 'val'], 'label': 'Exotic Munitions', 'parse': talentValParser},
    {'nodes': ['talents', "Technique / Munitions", 'list', 1, 'val'], 'label': 'Explosive Shot', 'parse': talentValParser},
    {'nodes': ['talents', "Technique / Munitions", 'list', 2, 'val'], 'label': 'Enhanced Munitions', 'parse': talentValParser},
    {'nodes': ['talents', "Technique / Munitions", 'list', 3, 'val'], 'label': 'Alloyed Munitions', 'parse': talentValParser},

    {'nodes': ['talents', "Technique / Agility", 'list', 0, 'val'], 'label': 'Agile Defense', 'parse': talentValParser},
    {'nodes': ['talents', "Technique / Agility", 'list', 1, 'val'], 'label': 'Vault', 'parse': talentValParser},
    {'nodes': ['talents', "Technique / Agility", 'list', 2, 'val'], 'label': 'Bull Shot', 'parse': talentValParser},
    {'nodes': ['talents', "Technique / Agility", 'list', 3, 'val'], 'label': 'Rapid Shot', 'parse': talentValParser},

    {'nodes': ['talents', "Technique / Sniper", 'list', 0, 'val'], 'label': 'Concealment', 'parse': talentValParser},
    {'nodes': ['talents', "Technique / Sniper", 'list', 1, 'val'], 'label': 'Shadow Shot', 'parse': talentValParser},
    {'nodes': ['talents', "Technique / Sniper", 'list', 2, 'val'], 'label': 'Aim', 'parse': talentValParser},
    {'nodes': ['talents', "Technique / Sniper", 'list', 3, 'val'], 'label': 'Snipe', 'parse': talentValParser},

    {'nodes': ['talents', "Cunning / Trapping", 'list', 0, 'val'], 'label': 'Trap Mastery', 'parse': talentValParser},
    {'nodes': ['talents', "Cunning / Trapping", 'list', 1, 'val'], 'label': 'Lure', 'parse': talentValParser},
    {'nodes': ['talents', "Cunning / Trapping", 'list', 2, 'val'], 'label': 'Advanced Trap Deployment', 'parse': talentValParser},
    {'nodes': ['talents', "Cunning / Trapping", 'list', 3, 'val'], 'label': 'Trap Priming', 'parse': talentValParser},

    {'nodes': ['talents', "Technique / Combat training", 'list', 0, 'val'], 'label': 'Thick Skin', 'parse': talentValParser},
    {'nodes': ['talents', "Technique / Combat training", 'list', 1, 'val'], 'label': 'Armour Training', 'parse': talentValParser},
    {'nodes': ['talents', "Technique / Combat training", 'list', 2, 'val'], 'label': 'Light Armour Training', 'parse': talentValParser},
    {'nodes': ['talents', "Technique / Combat training", 'list', 3, 'val'], 'label': 'Combat Accuracy', 'parse': talentValParser},
    {'nodes': ['talents', "Technique / Combat training", 'list', 4, 'val'], 'label': 'Weapons Mastery', 'parse': talentValParser},
    {'nodes': ['talents', "Technique / Combat training", 'list', 5, 'val'], 'label': 'Dagger Mastery', 'parse': talentValParser},

    {'nodes': ['talents', "Technique / Mobility", 'list', 0, 'val'], 'label': 'Disengage', 'parse': talentValParser},
    {'nodes': ['talents', "Technique / Mobility", 'list', 1, 'val'], 'label': 'Evasion', 'parse': talentValParser},
    {'nodes': ['talents', "Technique / Mobility", 'list', 2, 'val'], 'label': 'Tumble', 'parse': talentValParser},
    {'nodes': ['talents', "Technique / Mobility", 'list', 3, 'val'], 'label': 'Trained Reactions', 'parse': talentValParser},

    {'nodes': ['talents', "Cunning / Survival", 'list', 0, 'val'], 'label': 'Heightened Senses', 'parse': talentValParser},
    {'nodes': ['talents', "Cunning / Survival", 'list', 1, 'val'], 'label': 'Device Mastery', 'parse': talentValParser},
    {'nodes': ['talents', "Cunning / Survival", 'list', 2, 'val'], 'label': 'Track', 'parse': talentValParser},
    {'nodes': ['talents', "Cunning / Survival", 'list', 3, 'val'], 'label': 'Danger Sense', 'parse': talentValParser},

    {'nodes': ['talents', "Technique / Conditioning", 'list', 0, 'val'], 'label': 'Vitality', 'parse': talentValParser},
    {'nodes': ['talents', "Technique / Conditioning", 'list', 1, 'val'], 'label': 'Unflinching Resolve', 'parse': talentValParser},
    {'nodes': ['talents', "Technique / Conditioning", 'list', 2, 'val'], 'label': 'Daunting Presence', 'parse': talentValParser},
    {'nodes': ['talents', "Technique / Conditioning", 'list', 3, 'val'], 'label': 'Adrenaline Surge', 'parse': talentValParser},
]


def charToColumn(char, columnDefs):
    column=[]
    for element in columnDefs:
        subtree=char
        nodeIndex=0
        while nodeIndex < len(element['nodes']) :
            try:
                subtree=subtree[element['nodes'][nodeIndex]]
            except:
                break
            nodeIndex+=1
        value=None
        if nodeIndex >= len(element['nodes']):
            parsefn=element['parse'] if 'parse' in element else lambda x: x
            value=parsefn(subtree)
        else:
            value=0
        column.append(value)
    return column

def columnToArcher(col):
    char={}
    for i in range(len(archerColumns)):
        column=archerColumns[i]
        label=column['label'] if 'label' in column else column['nodes'][-1]
        val=col[i]
        char[label]=val
    return char

def normalize(rows):
    desired_max, desired_min=1,-1
    X=np.array(rows)
    numerator = (X - X.min(axis=0))
    numerator*= (desired_max - desired_min)
    denom = X.max(axis=0) - X.min(axis=0)
    denom[denom == 0] = 1
    total= (desired_min + numerator / denom).astype(np.float64)
    total-=total.mean(axis=0)
    return total.tolist()


def computeDenormalizer(rows):
    desired_max, desired_min = 1, -1
    X = np.array(rows)
    mins=X.min(axis=0)
    numerator = (X - mins)
    numerator *= (desired_max - desired_min)
    denom = X.max(axis=0) - X.min(axis=0)
    denom[denom == 0] = 1
    total = (desired_min + numerator / denom).astype(np.float64)
    mean=total.mean(axis=0)
    def denormalizer(row):
        row=np.array(row)
        row+=mean
        row -= desired_min
        row *= denom
        row +=mins
        return row.tolist()

    return denormalizer


if __name__ == '__main__':
    sc = SparkContext()

    data=functions.readData(sc, "/cs455/project/data/chars")
    data=functions.filterByAddons(data)
    data=functions.filterByClass(data).persist()

    archerToColumn=lambda char: charToColumn(char, archerColumns)
    archers=data.filter(lambda char: char['character']['class'] == 'Archer').map(archerToColumn)
    archerList=archers.collect()

    levelIndex=2
    levelSets=[tuple([i]) for i in range(1,16)]+[tuple(i for i in range(5*j+16, 5*j+21)) for j in range(7)]

    normedRows=[]
    normedLevelSets={}
    denormalizers={}
    for levelSet in levelSets:
        levelRows=[row for row in archerList if row[levelIndex] in levelSet]
        normalized=normalize(levelRows)
        normedRows+=normalized
        for i in range(len(normalized)):
            row=normalized[i]
            denormalizers[tuple(row)]=levelRows[i]


    normalArchers = sc.parallelize(normedRows).persist()

    model = BisectingKMeans.train(normalArchers, 10, maxIterations=10)

    randomRow=normedRows[0]
    print("row:", randomRow)
    print("denormed:", denormalizers[tuple(randomRow)] )
    print("cluster:", model.predict(randomRow))

    print("labeled cluster:", columnToArcher(model.centers[model.predict(randomRow)]))





