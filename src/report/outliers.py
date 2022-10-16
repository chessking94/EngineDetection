import pandas as pd

import queries as qry


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


def format_cpl(typ, stat, rating, score, conn, color=None):
    qry_text = qry.cpl_outlier(agg=typ, stat=stat, rating=rating, color=color)
    distribution = pd.read_sql(qry_text, conn).values.tolist()
    if len(distribution) > 0:
        z_score = (score - distribution[0][0])/distribution[0][1]
        flg = ''
        if z_score <= -4 or score <= distribution[0][2]:
            flg = '*'
        val = '{:.4f}'.format(score) + flg
    else:
        val = score

    return val


def format_evm(typ, rating, score, dec, conn, color=None):
    qry_text = qry.evm_outlier(agg=typ, rating=rating, color=color)
    distribution = pd.read_sql(qry_text, conn).values.tolist()
    flg = ''
    if len(distribution) > 0:
        z_score = (score - distribution[0][0])/distribution[0][1]
        if z_score >= 4 or score >= distribution[0][2]:
            flg = '*'
    val = '{:3.{prec}f}'.format(score, prec=dec) + '%' + flg

    return val
