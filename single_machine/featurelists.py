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
