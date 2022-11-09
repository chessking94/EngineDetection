import logging
import math

import numpy as np
import pandas as pd

from scipy.spatial.distance import mahalanobis
from scipy.stats import chi2

import queries as qry


def format_cpl(typ, stat, rating, score, conn, colorid=None):
    qry_text = qry.cpl_outlier(agg=typ, stat=stat, rating=rating, colorid=colorid)
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


def format_evm(typ, rating, score, dec, conn, colorid=None):
    qry_text = qry.evm_outlier(agg=typ, rating=rating, colorid=colorid)
    distribution = pd.read_sql(qry_text, conn).values.tolist()
    flg = ''
    if len(distribution) > 0:
        z_score = (score - distribution[0][0])/distribution[0][1]
        if z_score >= 4 or score >= distribution[0][2]:
            flg = '*'
    val = '{:3.{prec}f}'.format(score, prec=dec) + '%' + flg

    return val


def get_mah_pval(conn, test_arr, srcid, agg, rating, tcid, colorid=0, egid=0):
    aggid = qry.get_aggid(conn, agg)
    t1_avg = qry.get_statavg(conn, srcid, aggid, rating, tcid, colorid, egid, 'T1')
    cpl_avg = qry.get_statavg(conn, srcid, aggid, rating, tcid, colorid, egid, 'ScACPL')
    score_avg = qry.get_statavg(conn, srcid, aggid, rating, tcid, colorid, egid, 'Score')
    t1_var = qry.get_statcovar(conn, srcid, aggid, rating, tcid, colorid, egid, 'T1', 'T1')
    cpl_var = qry.get_statcovar(conn, srcid, aggid, rating, tcid, colorid, egid, 'ScACPL', 'ScACPL')
    score_var = qry.get_statcovar(conn, srcid, aggid, rating, tcid, colorid, egid, 'Score', 'Score')
    t1_cpl_cov = qry.get_statcovar(conn, srcid, aggid, rating, tcid, colorid, egid, 'T1', 'ScACPL')
    t1_score_cov = qry.get_statcovar(conn, srcid, aggid, rating, tcid, colorid, egid, 'T1', 'Score')
    cpl_score_cov = qry.get_statcovar(conn, srcid, aggid, rating, tcid, colorid, egid, 'ScACPL', 'Score')

    means = [t1_avg, cpl_avg, score_avg]
    cov_matrix = [
        [t1_var, t1_cpl_cov, t1_score_cov],
        [t1_cpl_cov, cpl_var, cpl_score_cov],
        [t1_score_cov, cpl_score_cov, score_var]
    ]
    mach = mahalanobis(means, test_arr, np.linalg.inv(cov_matrix))
    machsq = mach*mach
    chisq = 1 - chi2.cdf(machsq, df=3)
    sig_lvl = 0.001

    flg = ''
    if chisq <= sig_lvl:
        flg = '*'
    rtn = '{:.2f}'.format(chisq*100) + '%' + flg
    return rtn


def calc_zscore(score, distribution):
    if len(distribution) > 0:
        z_score = (score - distribution[0][0])/distribution[0][1]
    else:
        z_score = None

    return z_score


def calc_comp_roi(z_arr):
    weights = [0.2, 0.35, 0.45]
    if len(z_arr) != len(weights):
        logging.critical('Number of weights does not match number of z-scores')
        raise SystemExit
    if sum(weights) != 1:
        logging.critical('Z-score weights do not sum to 1')
        raise SystemExit

    n = sum([x*y for x, y in zip(z_arr, weights)])
    d = 0
    for i in weights:
        d = d + i*i
    d = math.sqrt(d)
    if np.isnan(d) or d == 0:
        roi = ''
    else:
        z = n/d

        roi = 5*z + 50
        flg = ''
        if (roi) >= 70:
            flg = '*'

        roi = '{:.1f}'.format(roi) + flg
    return roi
