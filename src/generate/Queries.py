import json


def get_conf(key):
    fname = r'C:\Users\eehunt\Repository\confidential.json'
    with open(fname, 'r') as t:
        key_data = json.load(t)
    val = key_data.get(key)
    return val


# game level
def game_qry(fld, src, tctype, rating, color):
    qry = f"""
SELECT
{fld}

FROM vw{src}GameSummary

WHERE TimeControlType = '{tctype}'
AND RatingGroup = {rating}
AND Color = '{color}'
"""
    return qry


# event level
def event_qry(fld, tctype, rating):
    qry = f"""
SELECT
{fld}

FROM vwControlEventSummary

WHERE TimeControlType = '{tctype}'
AND RatingGroup = {rating}
"""
    return qry


# eval level
def eval_qry(fld, src, tctype, rating, evalgroup, color):
    qry = f"""
SELECT
{fld}

FROM vw{src}EvalSummary

WHERE TimeControlType = '{tctype}'
AND RatingGroup = {rating}
AND GroupID = {evalgroup}
AND Color = '{color}'
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
