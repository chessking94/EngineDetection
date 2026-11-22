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
def eval_qry(fld, src, tctype, rating, evalgroup, color):
    qry = f'''
SELECT
{fld}

FROM ChessWarehouse.lake.vwEvaluationSummary

WHERE SourceID = {src}
AND ColorID = {color}
AND TimeControlID = {tctype}
AND RatingID = {rating}
AND EvaluationGroupID = {evalgroup}
AND ACPL > 0
AND MoveScored = 1
AND Berserk = 0
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
    idval = int(pd.read_sql(qry, engine, params=[agg]).values[0][0])
    return idval


def get_srcid(engine, src):
    qry = 'SELECT SourceID FROM ChessWarehouse.dim.Sources WHERE SourceName = ?'
    idval = int(pd.read_sql(qry, engine, params=[src]).values[0][0])
    return idval


def get_fldid(engine, fld):
    qry = 'SELECT MeasurementID FROM ChessWarehouse.dim.Measurements WHERE MeasurementName = ?'
    idval = int(pd.read_sql(qry, engine, params=[fld]).values[0][0])
    return idval


def get_tcid(engine, tc):
    qry = 'SELECT TimeControlID FROM ChessWarehouse.dim.TimeControls WHERE TimeControlName = ?'
    idval = int(pd.read_sql(qry, engine, params=[tc]).values[0][0])
    return idval


def get_colorid(engine, color):
    qry = 'SELECT ColorID FROM ChessWarehouse.dim.Colors WHERE Color = ?'
    idval = int(pd.read_sql(qry, engine, params=[color]).values[0][0])
    return idval


def get_flddict(engine):
    qry = 'SELECT MeasurementID, MeasurementName FROM ChessWarehouse.dim.Measurements'
    rs = pd.read_sql(qry, engine)
    return dict(zip(rs['MeasurementName'], rs['MeasurementID']))


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
    rs = pd.read_sql(qry, engine, params=[srcid, aggid, rating, tcid, colorid, egid, mid1, mid2])
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
