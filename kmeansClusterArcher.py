from pyspark import SparkContext
import json
import functions
import numpy as np



from pyspark.mllib.clustering import BisectingKMeans, BisectingKMeansModel

def talentValParser(talentValString):
    return int(talentValString.split('/')[0])

archerColumns=[
    {'nodes': ['character', 'died', 'times'], 'label': 'Number of Deaths'},
    {'nodes': ['resources', 'life'], 'parse': lambda s: int(s.split('/')[1]), 'label': 'Life'},
    {'nodes': ['character', 'level'], 'label': 'Level'},
    {'nodes': ['resources', 'stamina'], 'parse': lambda s: int(s.split('/')[1]), 'label': 'Stamina'},
    {'nodes': ['offense', 'mainhand', 0, 'crit'], 'parse': lambda s: int(s[:-1]), 'label': 'Crit Chance'},

    {'nodes': ['primary stats', 'strength', 'value'], 'label': 'Primary Stat: strength'},
    {'nodes': ['primary stats', 'magic', 'value'], 'label': 'Primary Stat: magic'},
    {'nodes': ['primary stats', 'dexterity', 'value'], 'label': 'Primary Stat: dexterity'},
    {'nodes': ['primary stats', 'willpower', 'value'], 'label': 'Primary Stat: willpower'},
    {'nodes': ['primary stats', 'cunning', 'value'], 'label': 'Primary Stat: cunning'},
    {'nodes': ['primary stats', 'constitution', 'value'], 'label': 'Primary Stat: constitution'},

    {'nodes': ['defense', 'resistances', 'all'], 'parse': lambda s: int(s.split('%')[0][1:]), 'label': 'Resistance: All'},
    {'nodes': ['defense', 'resistances', 'fire'], 'parse': lambda s: int(s.split('%')[0][1:]), 'label': 'Resistance: Fire'},
    {'nodes': ['defense', 'resistances', 'physical'], 'parse': lambda s: int(s.split('%')[0][1:]), 'label': 'Resistance: Physical'},

    {'nodes': ['defense', 'immunities', 'Stun Resistance'], 'parse': lambda s: int(s[:-1]), 'label': 'Immunity: Stun'},
    {'nodes': ['defense', 'immunities', 'Bleed Resistance'], 'parse': lambda s: int(s[:-1]), 'label': 'Immunity: Bleed'},
    {'nodes': ['defense', 'immunities', 'Confusion Resistance'], 'parse': lambda s: int(s[:-1]), 'label': 'Immunity: Confusion'},
    {'nodes': ['defense', 'immunities', 'Pinning Resistance'], 'parse': lambda s: int(s[:-1]), 'label': 'Immunity: Pinning'},
    {'nodes': ['defense', 'immunities', 'Fear Resistance'], 'parse': lambda s: int(s[:-1]), 'label': 'Immunity: Fear'},
    {'nodes': ['defense', 'immunities', 'Poison Resistance'], 'parse': lambda s: int(s[:-1]), 'label': 'Immunity: Poison'},
    {'nodes': ['defense', 'immunities', 'Instadeath Resistance'], 'parse': lambda s: int(s[:-1]), 'label': 'Immunity: Instadeath'},
    {'nodes': ['defense', 'immunities', 'Silence Resistance'], 'parse': lambda s: int(s[:-1]), 'label': 'Immunity: Silence'},
    {'nodes': ['defense', 'immunities', 'Blind Resistance'], 'parse': lambda s: int(s[:-1]), 'label': 'Immunity: Blind'},
    {'nodes': ['defense', 'immunities', 'Disease Resistance'], 'parse': lambda s: int(s[:-1]), 'label': 'Immunity: Disease'},

    {'nodes': ['defense', 'defense', 'armour'], 'label': 'Armour'},
    {'nodes': ['defense', 'defense', 'armour_hardiness'], 'label': 'Armour Hardiness'},


    {'nodes': ['speeds', 'mental'], 'label': 'Speed: Mental'},
    {'nodes': ['speeds', 'attack'], 'label': 'Speed: Attack'},
    {'nodes': ['speeds', 'spell'], 'label': 'Speed: Spell'},
    {'nodes': ['speeds', 'global'], 'label': 'Speed: Global'},
    {'nodes': ['speeds', 'movement'], 'label': 'Speed: Movement'},


    {'nodes': ['talents', "Technique / Archery prowess", 'list', 0, 'val'], 'label': 'Talent: Headshot', 'parse': talentValParser},
    {'nodes': ['talents', "Technique / Archery prowess", 'list', 1, 'val'], 'label': 'Talent: Volley', 'parse': talentValParser},
    {'nodes': ['talents', "Technique / Archery prowess", 'list', 2, 'val'], 'label': 'Talent: Called Shots', 'parse': talentValParser},
    {'nodes': ['talents', "Technique / Archery prowess", 'list', 3, 'val'], 'label': 'Talent: Bulleyes', 'parse': talentValParser},

    {'nodes': ['talents', "Technique / Combat techniques", 'list', 0, 'val'], 'label': 'Talent: Rush', 'parse': talentValParser},
    {'nodes': ['talents', "Technique / Combat techniques", 'list', 1, 'val'], 'label': 'Talent: Precise Strikes', 'parse': talentValParser},
    {'nodes': ['talents', "Technique / Combat techniques", 'list', 2, 'val'], 'label': 'Talent: Perfect Strike', 'parse': talentValParser},
    {'nodes': ['talents', "Technique / Combat techniques", 'list', 3, 'val'], 'label': 'Talent: Blinding Speed', 'parse': talentValParser},

    {'nodes': ['talents', "Technique / Combat veteran", 'list', 0, 'val'], 'label': 'Talent: Quick Recovery', 'parse': talentValParser},
    {'nodes': ['talents', "Technique / Combat veteran", 'list', 1, 'val'], 'label': 'Talent: Fast Metabolism', 'parse': talentValParser},
    {'nodes': ['talents', "Technique / Combat veteran", 'list', 2, 'val'], 'label': 'Talent: Spell Shield', 'parse': talentValParser},
    {'nodes': ['talents', "Technique / Combat veteran", 'list', 3, 'val'], 'label': 'Talent: Unending Frenzy', 'parse': talentValParser},

    {'nodes': ['talents', "Technique / Marksmanship", 'list', 0, 'val'], 'label': 'Talent: Master Marksman', 'parse': talentValParser},
    {'nodes': ['talents', "Technique / Marksmanship", 'list', 1, 'val'], 'label': 'Talent: First Blood', 'parse': talentValParser},
    {'nodes': ['talents', "Technique / Marksmanship", 'list', 2, 'val'], 'label': 'Talent: Flare', 'parse': talentValParser},
    {'nodes': ['talents', "Technique / Marksmanship", 'list', 3, 'val'], 'label': 'Talent: Trueshot', 'parse': talentValParser},

    {'nodes': ['talents', "Technique / Reflexes", 'list', 0, 'val'], 'label': 'Talent: Shoot Down', 'parse': talentValParser},
    {'nodes': ['talents', "Technique / Reflexes", 'list', 1, 'val'], 'label': 'Talent: Intuitive Shots', 'parse': talentValParser},
    {'nodes': ['talents', "Technique / Reflexes", 'list', 2, 'val'], 'label': 'Talent: Sentinel', 'parse': talentValParser},
    {'nodes': ['talents', "Technique / Reflexes", 'list', 3, 'val'], 'label': 'Talent: Escape', 'parse': talentValParser},

    {'nodes': ['talents', "Technique / Munitions", 'list', 0, 'val'], 'label': 'Talent: Exotic Munitions', 'parse': talentValParser},
    {'nodes': ['talents', "Technique / Munitions", 'list', 1, 'val'], 'label': 'Talent: Explosive Shot', 'parse': talentValParser},
    {'nodes': ['talents', "Technique / Munitions", 'list', 2, 'val'], 'label': 'Talent: Enhanced Munitions', 'parse': talentValParser},
    {'nodes': ['talents', "Technique / Munitions", 'list', 3, 'val'], 'label': 'Talent: Alloyed Munitions', 'parse': talentValParser},

    {'nodes': ['talents', "Technique / Agility", 'list', 0, 'val'], 'label': 'Talent: Agile Defense', 'parse': talentValParser},
    {'nodes': ['talents', "Technique / Agility", 'list', 1, 'val'], 'label': 'Talent: Vault', 'parse': talentValParser},
    {'nodes': ['talents', "Technique / Agility", 'list', 2, 'val'], 'label': 'Talent: Bull Shot', 'parse': talentValParser},
    {'nodes': ['talents', "Technique / Agility", 'list', 3, 'val'], 'label': 'Talent: Rapid Shot', 'parse': talentValParser},

    {'nodes': ['talents', "Technique / Sniper", 'list', 0, 'val'], 'label': 'Talent: Concealment', 'parse': talentValParser},
    {'nodes': ['talents', "Technique / Sniper", 'list', 1, 'val'], 'label': 'Talent: Shadow Shot', 'parse': talentValParser},
    {'nodes': ['talents', "Technique / Sniper", 'list', 2, 'val'], 'label': 'Talent: Aim', 'parse': talentValParser},
    {'nodes': ['talents', "Technique / Sniper", 'list', 3, 'val'], 'label': 'Talent: Snipe', 'parse': talentValParser},

    {'nodes': ['talents', "Cunning / Trapping", 'list', 0, 'val'], 'label': 'Talent: Trap Mastery', 'parse': talentValParser},
    {'nodes': ['talents', "Cunning / Trapping", 'list', 1, 'val'], 'label': 'Talent: Lure', 'parse': talentValParser},
    {'nodes': ['talents', "Cunning / Trapping", 'list', 2, 'val'], 'label': 'Talent: Advanced Trap Deployment', 'parse': talentValParser},
    {'nodes': ['talents', "Cunning / Trapping", 'list', 3, 'val'], 'label': 'Talent: Trap Priming', 'parse': talentValParser},

    {'nodes': ['talents', "Technique / Combat training", 'list', 0, 'val'], 'label': 'Talent: Thick Skin', 'parse': talentValParser},
    {'nodes': ['talents', "Technique / Combat training", 'list', 1, 'val'], 'label': 'Talent: Armour Training', 'parse': talentValParser},
    {'nodes': ['talents', "Technique / Combat training", 'list', 2, 'val'], 'label': 'Talent: Light Armour Training', 'parse': talentValParser},
    {'nodes': ['talents', "Technique / Combat training", 'list', 3, 'val'], 'label': 'Talent: Combat Accuracy', 'parse': talentValParser},
    {'nodes': ['talents', "Technique / Combat training", 'list', 4, 'val'], 'label': 'Talent: Weapons Mastery', 'parse': talentValParser},
    {'nodes': ['talents', "Technique / Combat training", 'list', 5, 'val'], 'label': 'Talent: Dagger Mastery', 'parse': talentValParser},

    {'nodes': ['talents', "Technique / Mobility", 'list', 0, 'val'], 'label': 'Talent: Disengage', 'parse': talentValParser},
    {'nodes': ['talents', "Technique / Mobility", 'list', 1, 'val'], 'label': 'Talent: Evasion', 'parse': talentValParser},
    {'nodes': ['talents', "Technique / Mobility", 'list', 2, 'val'], 'label': 'Talent: Tumble', 'parse': talentValParser},
    {'nodes': ['talents', "Technique / Mobility", 'list', 3, 'val'], 'label': 'Talent: Trained Reactions', 'parse': talentValParser},

    {'nodes': ['talents', "Cunning / Survival", 'list', 0, 'val'], 'label': 'Talent: Heightened Senses', 'parse': talentValParser},
    {'nodes': ['talents', "Cunning / Survival", 'list', 1, 'val'], 'label': 'Talent: Device Mastery', 'parse': talentValParser},
    {'nodes': ['talents', "Cunning / Survival", 'list', 2, 'val'], 'label': 'Talent: Track', 'parse': talentValParser},
    {'nodes': ['talents', "Cunning / Survival", 'list', 3, 'val'], 'label': 'Talent: Danger Sense', 'parse': talentValParser},

    {'nodes': ['talents', "Technique / Conditioning", 'list', 0, 'val'], 'label': 'Talent: Vitality', 'parse': talentValParser},
    {'nodes': ['talents', "Technique / Conditioning", 'list', 1, 'val'], 'label': 'Talent: Unflinching Resolve', 'parse': talentValParser},
    {'nodes': ['talents', "Technique / Conditioning", 'list', 2, 'val'], 'label': 'Talent: Daunting Presence', 'parse': talentValParser},
    {'nodes': ['talents', "Technique / Conditioning", 'list', 3, 'val'], 'label': 'Talent: Adrenaline Surge', 'parse': talentValParser},
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
    # print("row:", randomRow)
    # print("denormed:", denormalizers[tuple(randomRow)] )
    # print("cluster:", model.predict(randomRow))


    #print("labeled cluster:", columnToArcher(model.centers[model.predict(randomRow)]))
    #print("\n\n")


    goodClusters=sorted(model.centers, key=lambda center: center[0])[:5]
    for clusterRow in goodClusters:
            cluster=columnToArcher(clusterRow)
            print("DEATHS:", cluster['Number of Deaths'])
            for key, value in sorted(cluster.items(), key=lambda s: abs(s[1]))[-10:]:
                 print(key, value)





