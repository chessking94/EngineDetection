import pandas as pd


# game level
def game_qry(src, tctype, rating, color):
    qry = f'''
SELECT
fg.T1, fg.T2, fg.T3, fg.T4, fg.T5,
fg.ACPL, fg.ScACPL,
fg.SDCPL, fg.ScSDCPL,
gs.WinProbabilityLost, gs.ContextualMoveQuality

FROM ChessWarehouse.fact.Game AS fg
INNER JOIN ChessWarehouse.fact.vwGameScoresPivot AS gs ON fg.GameID = gs.GameID
    AND fg.ColorID = gs.ColorID

WHERE fg.SourceID = {src}
AND fg.TimeControlID = {tctype}
AND fg.RatingID = {rating}
AND fg.ColorID = {color}
AND fg.MovesAnalyzed > 0
'''
    return qry


# event level
def event_qry(src, tctype, rating):
    qry = f'''
SELECT
fe.T1, fe.T2, fe.T3, fe.T4, fe.T5,
fe.ACPL, fe.ScACPL,
fe.SDCPL, fe.ScSDCPL,
es.WinProbabilityLost, es.ContextualMoveQuality

FROM ChessWarehouse.fact.Event AS fe
INNER JOIN ChessWarehouse.fact.vwEventScoresPivot AS es ON fe.EventID = es.EventID
    AND fe.PlayerID = es.PlayerID
    AND fe.TimeControlID = es.TimeControlID

WHERE fe.SourceID = {src}
AND fe.TimeControlID = {tctype}
AND fe.RatingID = {rating}
AND fe.MovesAnalyzed > 0
'''
    return qry


# eval level
def eval_qry(fields, src, tcid, rating, evalgroup, color):
    """ `fields` is unused right now, but query performance is better when written this way """
    qry = f'''
SELECT
    m.CP_Loss AS ACPL
    ,m.ScACPL

FROM ChessWarehouse.lake.Moves AS m
INNER JOIN ChessWarehouse.stat.MoveScores AS ms ON m.GameID = ms.GameID
    AND m.MoveNumber = ms.MoveNumber
    AND m.ColorID = ms.ColorID
INNER JOIN ChessWarehouse.lake.Games AS g ON m.GameID = g.GameID
INNER JOIN ChessWarehouse.dim.Sources AS s ON g.SourceID = s.SourceID
INNER JOIN ChessWarehouse.dim.TimeControlDetail AS td ON g.TimeControlDetailID = td.TimeControlDetailID
INNER JOIN ChessWarehouse.dim.TimeControls AS tc ON td.TimeControlID = tc.TimeControlID

WHERE ms.ScoreID = 4
AND g.SourceID = {src}
AND m.ColorID = {color}
AND tc.TimeControlID = {tcid}
AND g.WhiteRatingID = {rating}
AND m.T1_EvaluationGroupID = {evalgroup}
AND m.CP_Loss > 0
AND m.MoveScored = 1
AND (CASE WHEN m.ColorID = 1 THEN g.WhiteBerserk ELSE g.BlackBerserk END) = 0
'''
    return qry


# TODO: Figure this thing out
def eval_score(tctype, rating, evalgroup, color):
    qry = f"""
SELECT
    PointsGained
    ,TotalPoints

FROM vwControlEvalSummary

WHERE TimeControlType = {tctype}
AND RatingGroup = {rating}
AND GroupID = {evalgroup}
AND Color = '{color}'
"""
    return qry


def get_aggid(engine, agg):
    qry = 'SELECT AggregationID FROM ChessWarehouse.dim.Aggregations WHERE AggregationName = ?'
    idval = int(pd.read_sql(qry, engine, params=(agg,)).values[0][0])
    return idval


def get_srcid(engine, src):
    qry = 'SELECT SourceID FROM ChessWarehouse.dim.Sources WHERE SourceName = ?'
    idval = int(pd.read_sql(qry, engine, params=(src,)).values[0][0])
    return idval


def get_flddict(engine):
    qry = 'SELECT MeasurementID, MeasurementName FROM ChessWarehouse.dim.Measurements'
    rs = pd.read_sql(qry, engine)
    return dict(zip(rs['MeasurementName'], rs['MeasurementID']))


def get_tcdict(engine):
    qry = 'SELECT TimeControlID, TimeControlName FROM ChessWarehouse.dim.TimeControls'
    rs = pd.read_sql(qry, engine)
    return dict(zip(rs['TimeControlName'], rs['TimeControlID']))


def get_colordict(engine):
    qry = 'SELECT ColorID, Color FROM ChessWarehouse.dim.Colors'
    rs = pd.read_sql(qry, engine)
    return dict(zip(rs['Color'], rs['ColorID']))


def check_cov(engine, srcid, aggid, rating, tcid, colorid, egid, mid1, mid2):
    rtn = False
    qry = '''
SELECT
Covariance

FROM ChessWarehouse.stat.Covariances

WHERE SourceID = ?
AND AggregationID = ?
AND RatingID = ?
AND TimeControlID = ?
AND ColorID = ?
AND EvaluationGroupID = ?
AND MeasurementID1 = ?
AND MeasurementID2 = ?
'''
    rs = pd.read_sql(qry, engine, params=(srcid, aggid, rating, tcid, colorid, egid, mid1, mid2))
    if len(rs) > 0:
        rtn = True
    return rtn


def parameter_stuff(typ, mdl, srcid, tcid, ratingid, egid, params):
    if typ == 'Count':
        if mdl == 'Evaluation':
            return f'SELECT SourceID, TimeControlID FROM ChessWarehouse.stat.EvalDistributionParameters WHERE SourceID = {srcid} AND TimeControlID = {tcid}'
        elif mdl == 'CP_Loss':
            return f'''
SELECT SourceID, TimeControlID, RatingID, EvaluationGroupID FROM ChessWarehouse.stat.CPLossDistributionParameters
WHERE SourceID = {srcid} AND TimeControlID = {tcid} AND RatingID = {ratingid} AND EvaluationGroupID = {egid}
'''
    elif typ == 'Insert':
        if mdl == 'Evaluation':
            sql_ins = 'INSERT INTO ChessWarehouse.stat.EvalDistributionParameters (SourceID, TimeControlID, p_0, p_1) '
            sql_ins = sql_ins + f"VALUES ({srcid}, {tcid}, {params[0]}, {params[1]})"
            return sql_ins
        elif mdl == 'CP_Loss':
            sql_ins = 'INSERT INTO ChessWarehouse.stat.CPLossDistributionParameters (SourceID, TimeControlID, RatingID, EvaluationGroupID, Alpha, Beta) '
            sql_ins = sql_ins + f'VALUES ({srcid}, {tcid}, {ratingid}, {egid}, {params[0]}, {params[1]})'
            return sql_ins
    elif typ == 'Update':
        if mdl == 'Evaluation':
            sql_upd = f'''
UPDATE new_p

SET new_p.p_0 = {params[0]}
    ,new_p.p_1 = {params[1]}
    ,new_p.Prev_p_0 = old_p.p_0
    ,new_p.Prev_p_1 = old_p.p_1
    ,new_p.UpdateDate = GETDATE()

FROM ChessWarehouse.stat.EvalDistributionParameters AS new_p
INNER JOIN ChessWarehouse.stat.EvalDistributionParameters AS old_p ON new_p.SourceID = old_p.SourceID
    AND new_p.TimeControlID = old_p.TimeControlID

WHERE new_p.SourceID = {srcid}
AND new_p.TimeControlID = {tcid}
'''
            return sql_upd
        elif mdl == 'CP_Loss':
            sql_upd = f'''
UPDATE new_p

SET new_p.Alpha = {params[0]}
    ,new_p.Beta = {params[1]}
    ,new_p.PrevAlpha = old_p.Alpha
    ,new_p.PrevBeta = old_p.Beta
    ,new_p.UpdateDate = GETDATE()

FROM ChessWarehouse.stat.CPLossDistributionParameters AS new_p
INNER JOIN ChessWarehouse.stat.CPLossDistributionParameters AS old_p ON new_p.SourceID = old_p.SourceID
    AND new_p.TimeControlID = old_p.TimeControlID
    AND new_p.RatingID = old_p.RatingID
    AND new_p.EvaluationGroupID = old_p.EvaluationGroupID

WHERE new_p.SourceID = {srcid}
AND new_p.TimeControlID = {tcid}
AND new_p.RatingID = {ratingid}
AND new_p.EvaluationGroupID = {egid}
'''
            return sql_upd
