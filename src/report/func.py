import queries as q


def parse_stats(compare_stats, conn):
    def_srcname, def_srcid = 'Control', 3
    def_tcname, def_tcid = 'Classical', 5

    scrid = None
    scrname = compare_stats.get('scoreName')
    scrid = q.get_scid(conn, scrname)

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
        'scoreId': scrid,
        'scoreName': scrname,
        'srcId': srcid,
        'srcName': src_name,
        'tcId': tcid,
        'tcName': tc_name,
        'rId': rid
    }

    return rtn_dict
