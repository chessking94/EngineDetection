import json
import os


def get_conf(key):
    fname = r'C:\Users\eehunt\Repository\confidential.json'
    with open(fname, 'r') as t:
        key_data = json.load(t)
    val = key_data.get(key)
    return val


def get_config(filepath, key):
    filename = os.path.join(filepath, 'config.json')
    with open(filename, 'r') as t:
        key_data = json.load(t)
    val = key_data.get(key)
    return val


def calc_roi(typ, score, distribution):
    if len(distribution) > 0:
        z_score = (score - distribution[0][0])/distribution[0][1]
        roi = 50 + z_score*5
        flg = ''
        if typ == 'Event':
            if (roi) >= 70 or score >= distribution[0][2]:
                flg = '*'
        elif typ == 'Game':
            if (roi) >= 70 or score >= distribution[0][2]:
                flg = '*'
        # else:
        #     pass

        roi = '{:.1f}'.format(50 + z_score*5) + flg
    else:
        roi = ''

    return roi
