import json

import pandas as pd


def get_conf(key):
    fname = r'C:\Users\eehunt\Repository\confidential.json'
    with open(fname, 'r') as t:
        key_data = json.load(t)
    val = key_data.get(key)
    return val


# game level
def game_qry(src, tctype, rating, color):
    qry = f"""
SELECT
T1, T2, T3, T4, T5,
ACPL, ScACPL,
SDCPL, ScSDCPL,
Score

FROM fact.Game

WHERE SourceID = {src}
AND TimeControlID = {tctype}
AND RatingID = {rating}
AND ColorID = {color}
AND MovesAnalyzed > 0
"""
    return qry


# event level
def event_qry(src, tctype, rating):
    qry = f'''
SELECT
T1, T2, T3, T4, T5,
ACPL, ScACPL,
SDCPL, ScSDCPL,
Score

FROM fact.Event

WHERE SourceID = {src}
AND TimeControlID = {tctype}
AND RatingID = {rating}
AND MovesAnalyzed > 0
'''
    return qry


# eval level
def eval_qry(fld, src, tctype, rating, evalgroup, color):
    qry = f"""
SELECT
{fld}

FROM lake.vwEvaluationSummary

WHERE SourceID = {src}
AND ColorID = {color}
AND TimeControlID = {tctype}
AND RatingID = {rating}
AND EvaluationGroupID = {evalgroup}
AND ACPL > 0
"""
    return qry


# TODO: Figure this thing out
def eval_score(tctype, rating, evalgroup, color):
    qry = f"""
SELECT
PointsGained,
TotalPoints

FROM vwControlEvalSummary
WHERE TimeControlType = {tctype}
AND RatingGroup = {rating}
AND GroupID = {evalgroup}
AND Color = '{color}'
"""
    return qry


def get_aggid(conn, agg):
    qry = f"""
SELECT
AggregationID

FROM dim.Aggregations

WHERE AggregationName = '{agg}'
"""
    idval = int(pd.read_sql(qry, conn).values[0][0])
    return idval


def get_srcid(conn, src):
    qry = f"""
SELECT
SourceID

FROM dim.Sources

WHERE SourceName = '{src}'
"""
    idval = int(pd.read_sql(qry, conn).values[0][0])
    return idval


def get_fldid(conn, fld):
    qry = f"""
SELECT
MeasurementID

FROM dim.Measurements

WHERE MeasurementName = '{fld}'
"""
    idval = int(pd.read_sql(qry, conn).values[0][0])
    return idval


def get_tcid(conn, tc):
    qry = f"""
SELECT
TimeControlID

FROM dim.TimeControls

WHERE TimeControlName = '{tc}'
"""
    idval = int(pd.read_sql(qry, conn).values[0][0])
    return idval


def get_colorid(conn, color):
    qry = f"""
SELECT
ColorID

FROM dim.Colors

WHERE Color = '{color}'
"""
    idval = int(pd.read_sql(qry, conn).values[0][0])
    return idval


def get_flddict(conn):
    qry = 'SELECT MeasurementID, MeasurementName FROM dim.Measurements'
    rs = pd.read_sql(qry, conn)
    return dict(zip(rs['MeasurementName'], rs['MeasurementID']))


def check_cov(conn, srcid, aggid, rating, tcid, colorid, egid, mid1, mid2):
    rtn = False
    qry = f'''
SELECT
Covariance

FROM stat.Covariances

WHERE SourceID = {srcid}
AND AggregationID = {aggid}
AND RatingID = {rating}
AND TimeControlID = {tcid}
AND ColorID = {colorid}
AND EvaluationGroupID = {egid}
AND MeasurementID1 = {mid1}
AND MeasurementID2 = {mid2}
'''
    rs = pd.read_sql(qry, conn)
    if len(rs) > 0:
        rtn = True
    return rtn
