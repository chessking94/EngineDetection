import json


def get_conf(key):
    fname = r'C:\Users\eehunt\Repository\confidential.json'
    with open(fname, 'r') as t:
        key_data = json.load(t)
    val = key_data.get(key)
    return val


# game level
def game_acpl(src, tctype, rating, color):
    qry = f"""
SELECT
ACPL

FROM vw{src}GameSummary

WHERE TimeControlType = '{tctype}'
AND RatingGroup = {rating}
AND Color = '{color}'
"""
    return qry


def game_sdcpl(src, tctype, rating, color):
    qry = f"""
SELECT
SDCPL

FROM vw{src}GameSummary

WHERE TimeControlType = '{tctype}'
AND RatingGroup = {rating}
AND Color = '{color}'
"""
    return qry


def game_tx(src, tctype, rating, color, N):
    qry = f"""
SELECT
T{N}

FROM vw{src}GameSummary

WHERE TimeControlType = '{tctype}'
AND RatingGroup = {rating}
AND Color = '{color}'
"""
    return qry


def game_score(src, tctype, rating, color):
    qry = f"""
SELECT
Score

FROM vw{src}GameSummary

WHERE TimeControlType = '{tctype}'
AND RatingGroup = {rating}
AND Color = '{color}'
"""
    return qry


# event level
def event_acpl(tctype, rating):
    qry = f"""
SELECT
ACPL

FROM vwControlEventSummary

WHERE TimeControlType = '{tctype}'
AND RatingGroup = {rating}
"""
    return qry


def event_sdcpl(tctype, rating):
    qry = f"""
SELECT
SDCPL

FROM vwControlEventSummary

WHERE TimeControlType = '{tctype}'
AND RatingGroup = {rating}
"""
    return qry


def event_tx(tctype, rating, N):
    qry = f"""
SELECT
T{N}

FROM vwControlEventSummary

WHERE TimeControlType = '{tctype}'
AND RatingGroup = {rating}
"""
    return qry


def event_score(tctype, rating):
    qry = f"""
SELECT
Score

FROM vwControlEventSummary

WHERE TimeControlType = '{tctype}'
AND RatingGroup = {rating}
"""
    return qry


# eval level
def eval_acpl(src, tctype, rating, evalgroup, color):
    qry = f"""
SELECT
ACPL

FROM vw{src}EvalSummary

WHERE TimeControlType = '{tctype}'
AND RatingGroup = {rating}
AND GroupID = {evalgroup}
AND Color = '{color}'
AND ACPL > 0"
"""
    return qry


def eval_tx(src, tctype, rating, evalgroup, color, N):
    qry = f"""
SELECT
T{N}

FROM vw{src}EvalSummary

WHERE TimeControlType = '{tctype}'
AND RatingGroup = {rating}
AND GroupID = {evalgroup}
AND Color = '{color}'
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
