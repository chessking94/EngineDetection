import json
import os

import queries as q


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


def parse_stats(compare_stats, conn):
    def_srcname, def_srcid = 'Control', 3
    def_tcname, def_tcid = 'Classical', 5

    srcid = None
    src_name = compare_stats.get('sourceName')
    srcid = q.get_srcid(conn, src_name)
    if srcid is None:
        srcid, src_name = def_srcid, def_srcname

    tcid = None
    tc_name = compare_stats.get('timeControlName')
    tcid = q.get_tcid(conn, tc_name)
    if tcid is None:
        tcid, tc_name = def_tcid, def_tcname

    rid = None
    ratingid = compare_stats.get('ratingID')
    rid = ratingid if ratingid is not None else rid

    rtn_dict = {
        'srcid': srcid,
        'srcname': src_name,
        'tcid': tcid,
        'tcname': tc_name,
        'rid': rid
    }

    return rtn_dict
