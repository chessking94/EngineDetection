import pandas as pd


# game level
def game_qry(src, tctype, rating, color):
    qry = f"""
SELECT
fg.T1, fg.T2, fg.T3, fg.T4, fg.T5,
fg.ACPL, fg.ScACPL,
fg.SDCPL, fg.ScSDCPL,
gs.WinProbabilityLost, gs.EvaluationGroupComparison

FROM ChessWarehouse.fact.Game fg
JOIN ChessWarehouse.fact.vwGameScoresPivot gs ON
    fg.GameID = gs.GameID AND
    fg.ColorID = gs.ColorID

WHERE fg.SourceID = {src}
AND fg.TimeControlID = {tctype}
AND fg.RatingID = {rating}
AND fg.ColorID = {color}
AND fg.MovesAnalyzed > 0
"""
    return qry


# event level
def event_qry(src, tctype, rating):
    qry = f'''
SELECT
fe.T1, fe.T2, fe.T3, fe.T4, fe.T5,
fe.ACPL, fe.ScACPL,
fe.SDCPL, fe.ScSDCPL,
es.WinProbabilityLost, es.EvaluationGroupComparison

FROM ChessWarehouse.fact.Event fe
JOIN ChessWarehouse.fact.vwEventScoresPivot es ON
    fe.EventID = es.EventID AND
    fe.PlayerID = es.PlayerID AND
    fe.TimeControlID = es.TimeControlID

WHERE fe.SourceID = {src}
AND fe.TimeControlID = {tctype}
AND fe.RatingID = {rating}
AND fe.MovesAnalyzed > 0
'''
    return qry


# eval level
def eval_qry(fld, src, tctype, rating, evalgroup, color):
    qry = f"""
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


def get_aggid(engine, agg):
    qry = f"SELECT AggregationID FROM ChessWarehouse.dim.Aggregations WHERE AggregationName = '{agg}'"
    idval = int(pd.read_sql(qry, engine).values[0][0])
    return idval


def get_srcid(engine, src):
    qry = f"SELECT SourceID FROM ChessWarehouse.dim.Sources WHERE SourceName = '{src}'"
    idval = int(pd.read_sql(qry, engine).values[0][0])
    return idval


def get_fldid(engine, fld):
    qry = f"SELECT MeasurementID FROM ChessWarehouse.dim.Measurements WHERE MeasurementName = '{fld}'"
    idval = int(pd.read_sql(qry, engine).values[0][0])
    return idval


def get_tcid(engine, tc):
    qry = f"SELECT TimeControlID FROM ChessWarehouse.dim.TimeControls WHERE TimeControlName = '{tc}'"
    idval = int(pd.read_sql(qry, engine).values[0][0])
    return idval


def get_colorid(engine, color):
    qry = f"SELECT ColorID FROM ChessWarehouse.dim.Colors WHERE Color = '{color}'"
    idval = int(pd.read_sql(qry, engine).values[0][0])
    return idval


def get_flddict(engine):
    qry = 'SELECT MeasurementID, MeasurementName FROM ChessWarehouse.dim.Measurements'
    rs = pd.read_sql(qry, engine)
    return dict(zip(rs['MeasurementName'], rs['MeasurementID']))


def check_cov(engine, srcid, aggid, rating, tcid, colorid, egid, mid1, mid2):
    rtn = False
    qry = f'''
SELECT
Covariance

FROM ChessWarehouse.stat.Covariances

WHERE SourceID = {srcid}
AND AggregationID = {aggid}
AND RatingID = {rating}
AND TimeControlID = {tcid}
AND ColorID = {colorid}
AND EvaluationGroupID = {egid}
AND MeasurementID1 = {mid1}
AND MeasurementID2 = {mid2}
'''
    rs = pd.read_sql(qry, engine)
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
            sql_ins = 'INSERT INTO ChessWarehouse.stat.EvalDistributionParameters (SourceID, TimeControlID, Mean, StandardDeviation) '
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
SET new_p.Mean = {params[0]},
new_p.StandardDeviation = {params[1]},
new_p.PrevMean = old_p.Mean,
new_p.PrevStandardDeviation = old_p.StandardDeviation,
new_p.UpdateDate = GETDATE()

FROM ChessWarehouse.stat.EvalDistributionParameters new_p
JOIN ChessWarehouse.stat.EvalDistributionParameters old_p ON
    new_p.SourceID = old_p.SourceID AND
    new_p.TimeControlID = old_p.TimeControlID

WHERE new_p.SourceID = {srcid}
AND new_p.TimeControlID = {tcid}
'''
            return sql_upd
        elif mdl == 'CP_Loss':
            sql_upd = f'''
UPDATE new_p
SET new_p.Alpha = {params[0]},
new_p.Beta = {params[1]},
new_p.PrevAlpha = old_p.Alpha,
new_p.PrevBeta = old_p.Beta,
new_p.UpdateDate = GETDATE()

FROM ChessWarehouse.stat.CPLossDistributionParameters new_p
JOIN ChessWarehouse.stat.CPLossDistributionParameters old_p ON
    new_p.SourceID = old_p.SourceID AND
    new_p.TimeControlID = old_p.TimeControlID AND
    new_p.RatingID = old_p.RatingID AND
    new_p.EvaluationGroupID = old_p.EvaluationGroupID

WHERE new_p.SourceID = {srcid}
AND new_p.TimeControlID = {tcid}
AND new_p.RatingID = {ratingid}
AND new_p.EvaluationGroupID = {egid}
'''
            return sql_upd
