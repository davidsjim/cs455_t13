from pyspark import SparkContext
import json
import functions
import numpy as np
import sys


from pyspark.mllib.clustering import KMeans, KMeansModel

def talentValParser(talentValString):
    return int(talentValString.split('/')[0])

archmageColumns=[
    {'nodes': ['character', 'died', 'times'], 'label': 'Number of Deaths'},
    {'nodes': ['resources', 'life'], 'parse': lambda s: int(s.split('/')[1]), 'label': 'Life'},
    {'nodes': ['character', 'level'], 'label': 'Level'},
    {'nodes': ['resources', 'stamina'], 'parse': lambda s: int(s.split('/')[1]), 'label': 'Stamina'},
    {'nodes': ['offense', 'spell', 'crit'], 'parse': lambda s: int(s[:-1]), 'label': 'Spell Crit Chance'},

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
    #{'nodes': ['defense', 'immunities', 'Bleed Resistance'], 'parse': lambda s: int(s[:-1]), 'label': 'Immunity: Bleed'},
    {'nodes': ['defense', 'immunities', 'Confusion Resistance'], 'parse': lambda s: int(s[:-1]), 'label': 'Immunity: Confusion'},
    {'nodes': ['defense', 'immunities', 'Pinning Resistance'], 'parse': lambda s: int(s[:-1]), 'label': 'Immunity: Pinning'},
    #{'nodes': ['defense', 'immunities', 'Fear Resistance'], 'parse': lambda s: int(s[:-1]), 'label': 'Immunity: Fear'},
    #{'nodes': ['defense', 'immunities', 'Poison Resistance'], 'parse': lambda s: int(s[:-1]), 'label': 'Immunity: Poison'},
    #{'nodes': ['defense', 'immunities', 'Instadeath Resistance'], 'parse': lambda s: int(s[:-1]), 'label': 'Immunity: Instadeath'},
    {'nodes': ['defense', 'immunities', 'Silence Resistance'], 'parse': lambda s: int(s[:-1]), 'label': 'Immunity: Silence'},
    {'nodes': ['defense', 'immunities', 'Blind Resistance'], 'parse': lambda s: int(s[:-1]), 'label': 'Immunity: Blind'},
    {'nodes': ['defense', 'immunities', 'Disease Resistance'], 'parse': lambda s: int(s[:-1]), 'label': 'Immunity: Disease'},

    {'nodes': ['defense', 'defense', 'armour'], 'label': 'Armour'},
    {'nodes': ['defense', 'defense', 'armour_hardiness'], 'label': 'Armour Hardiness'},


    {'nodes': ['speeds', 'mental'], 'label': 'Speed: Mental'},
    {'nodes': ['speeds', 'attack'], 'label': 'Speed: Attack'},
    {'nodes': ['speeds', 'spell'], 'label': 'Speed: Spell'},
    {'nodes': ['speeds', 'global'], 'label': 'Speed: Global'},
    #{'nodes': ['speeds', 'movement'], 'label': 'Speed: Movement'},


    {'nodes': ['talents', "Spell / Arcane", 'list', 0, 'val'], 'label': 'Talent: Arcane Power', 'parse': talentValParser},
    {'nodes': ['talents', "Spell / Arcane", 'list', 1, 'val'], 'label': 'Talent: Manathrust', 'parse': talentValParser},
    {'nodes': ['talents', "Spell / Arcane", 'list', 2, 'val'], 'label': 'Talent: Arcane Vortex', 'parse': talentValParser},
    {'nodes': ['talents', "Spell / Arcane", 'list', 3, 'val'], 'label': 'Talent: Disruption Shield', 'parse': talentValParser},

    {'nodes': ['talents', "Spell / Fire", 'list', 0, 'val'], 'label': 'Talent: Flame', 'parse': talentValParser},
    {'nodes': ['talents', "Spell / Fire", 'list', 1, 'val'], 'label': 'Talent: Flameshock', 'parse': talentValParser},
    {'nodes': ['talents', "Spell / Fire", 'list', 2, 'val'], 'label': 'Talent: Fireflash', 'parse': talentValParser},
    {'nodes': ['talents', "Spell / Fire", 'list', 3, 'val'], 'label': 'Talent: Inferno', 'parse': talentValParser},

    {'nodes': ['talents', "Spell / Earth", 'list', 0, 'val'], 'label': 'Talent: Pulverizing Auger', 'parse': talentValParser},
    {'nodes': ['talents', "Spell / Earth", 'list', 1, 'val'], 'label': 'Talent: Stone Skin', 'parse': talentValParser},
    {'nodes': ['talents', "Spell / Earth", 'list', 2, 'val'], 'label': 'Talent: Mudslide', 'parse': talentValParser},
    {'nodes': ['talents', "Spell / Earth", 'list', 3, 'val'], 'label': 'Talent: Stone Wall', 'parse': talentValParser},

    {'nodes': ['talents', "Spell / Water", 'list', 0, 'val'], 'label': 'Talent: Glacial Vapour', 'parse': talentValParser},
    {'nodes': ['talents', "Spell / Water", 'list', 1, 'val'], 'label': 'Talent: Freeze', 'parse': talentValParser},
    {'nodes': ['talents', "Spell / Water", 'list', 2, 'val'], 'label': 'Talent: Tidal Wave', 'parse': talentValParser},
    {'nodes': ['talents', "Spell / Water", 'list', 3, 'val'], 'label': 'Talent: Shivgoroth Form', 'parse': talentValParser},

    {'nodes': ['talents', "Spell / Air", 'list', 0, 'val'], 'label': 'Talent: Lightning', 'parse': talentValParser},
    {'nodes': ['talents', "Spell / Air", 'list', 1, 'val'], 'label': 'Talent: Chain Lightning', 'parse': talentValParser},
    {'nodes': ['talents', "Spell / Air", 'list', 2, 'val'], 'label': 'Talent: Feather Wind', 'parse': talentValParser},
    {'nodes': ['talents', "Spell / Air", 'list', 3, 'val'], 'label': 'Talent: Thunderstorm', 'parse': talentValParser},

    {'nodes': ['talents', "Spell / Phantasm", 'list', 0, 'val'], 'label': 'Talent: Illuminate', 'parse': talentValParser},
    {'nodes': ['talents', "Spell / Phantasm", 'list', 1, 'val'], 'label': 'Talent: Blur Sight', 'parse': talentValParser},
    {'nodes': ['talents', "Spell / Phantasm", 'list', 2, 'val'], 'label': 'Talent: Phantasmal Shield', 'parse': talentValParser},
    {'nodes': ['talents', "Spell / Phantasm", 'list', 3, 'val'], 'label': 'Talent: Invisibility', 'parse': talentValParser},

    {'nodes': ['talents', "Spell / Aether", 'list', 0, 'val'], 'label': 'Talent: Aether Beam', 'parse': talentValParser},
    {'nodes': ['talents', "Spell / Aether", 'list', 1, 'val'], 'label': 'Talent: Aether Breach', 'parse': talentValParser},
    {'nodes': ['talents', "Spell / Aether", 'list', 2, 'val'], 'label': 'Talent: Aether Avatar', 'parse': talentValParser},
    {'nodes': ['talents', "Spell / Aether", 'list', 3, 'val'], 'label': 'Talent: Pure Aether', 'parse': talentValParser},

    {'nodes': ['talents', "Spell / Wildfire", 'list', 0, 'val'], 'label': 'Talent: Blastwave', 'parse': talentValParser},
    {'nodes': ['talents', "Spell / Wildfire", 'list', 1, 'val'], 'label': 'Talent: Burning Wake', 'parse': talentValParser},
    {'nodes': ['talents', "Spell / Wildfire", 'list', 2, 'val'], 'label': 'Talent: Cleansing Flames', 'parse': talentValParser},
    {'nodes': ['talents', "Spell / Wildfire", 'list', 3, 'val'], 'label': 'Talent: Wildfire', 'parse': talentValParser},

    {'nodes': ['talents', "Spell / Stone", 'list', 0, 'val'], 'label': 'Talent: Earthen Missiles', 'parse': talentValParser},
    {'nodes': ['talents', "Spell / Stone", 'list', 1, 'val'], 'label': 'Talent: Body of Stone', 'parse': talentValParser},
    {'nodes': ['talents', "Spell / Stone", 'list', 2, 'val'], 'label': 'Talent: Earthquake', 'parse': talentValParser},
    {'nodes': ['talents', "Spell / Stone", 'list', 3, 'val'], 'label': 'Talent: Crystalline Focus', 'parse': talentValParser},

    {'nodes': ['talents', "Spell / Ice", 'list', 0, 'val'], 'label': 'Talent: Ice Shards', 'parse': talentValParser},
    {'nodes': ['talents', "Spell / Ice", 'list', 1, 'val'], 'label': 'Talent: Frozen Ground', 'parse': talentValParser},
    {'nodes': ['talents', "Spell / Ice", 'list', 2, 'val'], 'label': 'Talent: Shatter', 'parse': talentValParser},
    {'nodes': ['talents', "Spell / Ice", 'list', 3, 'val'], 'label': 'Talent: Uttercold', 'parse': talentValParser},

    {'nodes': ['talents', "Spell / Storm", 'list', 0, 'val'], 'label': 'Talent: Nova', 'parse': talentValParser},
    {'nodes': ['talents', "Spell / Storm", 'list', 1, 'val'], 'label': 'Talent: Shock', 'parse': talentValParser},
    {'nodes': ['talents', "Spell / Storm", 'list', 2, 'val'], 'label': 'Talent: Hurricane', 'parse': talentValParser},
    {'nodes': ['talents', "Spell / Storm", 'list', 3, 'val'], 'label': 'Talent: Tempest', 'parse': talentValParser},

    {'nodes': ['talents', "Spell / Meta", 'list', 0, 'val'], 'label': 'Talent: Disperse Magic', 'parse': talentValParser},
    {'nodes': ['talents', "Spell / Meta", 'list', 1, 'val'], 'label': 'Talent: Spellcraft', 'parse': talentValParser},
    {'nodes': ['talents', "Spell / Meta", 'list', 2, 'val'], 'label': 'Talent: Quicken Spells', 'parse': talentValParser},
    {'nodes': ['talents', "Spell / Meta", 'list', 3, 'val'], 'label': 'Talent: Metaflow', 'parse': talentValParser},

    {'nodes': ['talents', "Spell / Temporal", 'list', 0, 'val'], 'label': 'Talent: Congeal Time', 'parse': talentValParser},
    {'nodes': ['talents', "Spell / Temporal", 'list', 1, 'val'], 'label': 'Talent: Time Shield', 'parse': talentValParser},
    {'nodes': ['talents', "Spell / Temporal", 'list', 2, 'val'], 'label': 'Talent: Time Prison', 'parse': talentValParser},
    {'nodes': ['talents', "Spell / Temporal", 'list', 3, 'val'], 'label': 'Talent: Essence of Speed', 'parse': talentValParser},

    {'nodes': ['talents', "Spell / Conveyance", 'list', 0, 'val'], 'label': 'Talent: Phase Door', 'parse': talentValParser},
    {'nodes': ['talents', "Spell / Conveyance", 'list', 1, 'val'], 'label': 'Talent: Teleport', 'parse': talentValParser},
    {'nodes': ['talents', "Spell / Conveyance", 'list', 2, 'val'], 'label': 'Talent: Displacement Shield', 'parse': talentValParser},
    {'nodes': ['talents', "Spell / Conveyance", 'list', 3, 'val'], 'label': 'Talent: Probability Travel', 'parse': talentValParser},

    {'nodes': ['talents', "Spell / Divination", 'list', 0, 'val'], 'label': 'Talent: Arcane Eye', 'parse': talentValParser},
    {'nodes': ['talents', "Spell / Divination", 'list', 1, 'val'], 'label': 'Talent: Keen Senses', 'parse': talentValParser},
    {'nodes': ['talents', "Spell / Divination", 'list', 2, 'val'], 'label': 'Talent: Vision', 'parse': talentValParser},
    {'nodes': ['talents', "Spell / Divination", 'list', 3, 'val'], 'label': 'Talent: Premonition', 'parse': talentValParser},

    {'nodes': ['talents', "Spell / Aegis", 'list', 0, 'val'], 'label': 'Talent: Arcane Reconstruction', 'parse': talentValParser},
    {'nodes': ['talents', "Spell / Aegis", 'list', 1, 'val'], 'label': 'Talent: Shielding', 'parse': talentValParser},
    {'nodes': ['talents', "Spell / Aegis", 'list', 2, 'val'], 'label': 'Talent: Arcane Shield', 'parse': talentValParser},
    {'nodes': ['talents', "Spell / Aegis", 'list', 3, 'val'], 'label': 'Talent: Aegis', 'parse': talentValParser},

    {'nodes': ['talents', "Spell / Survival", 'list', 0, 'val'], 'label': 'Talent: Heightened Senses', 'parse': talentValParser},
    {'nodes': ['talents', "Spell / Survival", 'list', 1, 'val'], 'label': 'Talent: Device Mastery', 'parse': talentValParser},
    {'nodes': ['talents', "Spell / Survival", 'list', 2, 'val'], 'label': 'Talent: Track', 'parse': talentValParser},
    {'nodes': ['talents', "Spell / Survival", 'list', 3, 'val'], 'label': 'Talent: Danger Sense', 'parse': talentValParser},

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

def columnToArchmage(col):
    char={}
    for i in range(len(archmageColumns)):
        column=archmageColumns[i]
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

    archmageToColumn=lambda char: charToColumn(char, archmageColumns)
    archmages=data.filter(lambda char: char['character']['class'] == 'Archmage').map(archmageToColumn)
    archmageList=archmages.collect()

    levelIndex=2
    levelSets=[tuple([i]) for i in range(1,16)]+[tuple(i for i in range(5*j+16, 5*j+21)) for j in range(7)]

    normedRows=[]
    normedLevelSets={}
    denormalizers={}
    for levelSet in levelSets:
        levelRows=[row for row in archmageList if row[levelIndex] in levelSet]
        normalized=normalize(levelRows)
        normedRows+=normalized
        for i in range(len(normalized)):
            row=normalized[i]
            denormalizers[tuple(row)]=levelRows[i]


    normalArchmage = sc.parallelize(normedRows).persist()

    numClusters= int(sys.argv[1])
    numIterations= int(sys.argv[2])
    model = KMeans.train(normalArchmage, numClusters, maxIterations=numIterations)

    randomRow=normedRows[0]
    # print("row:", randomRow)
    # print("denormed:", denormalizers[tuple(randomRow)] )
    # print("cluster:", model.predict(randomRow))


    #print("labeled cluster:", columnToArcher(model.centers[model.predict(randomRow)]))
    #print("\n\n")


    goodClusters=sorted(model.centers, key=lambda center: center[0])
    for clusterRow in goodClusters:
            cluster=columnToArchmage(clusterRow)
            print()
            print()
            print("DEATHS:", cluster['Number of Deaths'])
            printCutoff=float(sys.argv[3])
            for key, value in sorted(cluster.items(), key=lambda s: abs(s[1])):
                if value>printCutoff:
                    print(key, value)


    clusterMembers = [[] for center in model.centers]
    for row in normedRows:
        clusterIndex=model.predict(row)
        clusterMembers[clusterIndex].append(columnToArchmage(denormalizers[tuple(row)]))

    for members in clusterMembers:
        members.sort(key=lambda char: char['Level'])

    for i in range(len(clusterMembers)):
        print("cluster id:",i, ". Cluster size: ", len(clusterMembers[i]), ". Avg. Deaths:", columnToArchmage(model.centers[i])['Number of Deaths'])
