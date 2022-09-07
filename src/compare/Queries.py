import json


def get_conf(key):
    fname = r'C:\Users\eehunt\Repository\confidential.json'
    with open(fname, 'r') as t:
        key_data = json.load(t)
    val = key_data.get(key)
    return val


def construct_test(typ, lastname, firstname, tmnt, roundnum, color, startdate, enddate, result):
    qry = f'''
SELECT
CASE WHEN m.Color = 'White' THEN g.WhiteLast ELSE g.BlackLast END AS LastName,
CASE WHEN m.Color = 'White' THEN g.WhiteFirst ELSE g.BlackFirst END AS FirstName,
COUNT(DISTINCT g.GameID) AS GameCount,
COUNT(m.MoveID) AS MoveCount,
AVG(CONVERT(float, m.CP_Loss)) AS ACPL,
ISNULL(STDEV(CONVERT(float, m.CP_Loss)), 0) AS SDCPL,
1.00*SUM(CASE WHEN m.Move_Rank <= 1 THEN 1 ELSE 0 END)/COUNT(m.MoveID) AS T1,
1.00*SUM(CASE WHEN m.Move_Rank <= 2 THEN 1 ELSE 0 END)/COUNT(m.MoveID) AS T2,
1.00*SUM(CASE WHEN m.Move_Rank <= 3 THEN 1 ELSE 0 END)/COUNT(m.MoveID) AS T3,
1.00*SUM(CASE WHEN m.Move_Rank <= 4 THEN 1 ELSE 0 END)/COUNT(m.MoveID) AS T4,
1.00*SUM(CASE WHEN m.Move_Rank <= 5 THEN 1 ELSE 0 END)/COUNT(m.MoveID) AS T5,
100*SUM(v.Score)/SUM(gp.ScoreWeight*s.Points) AS Score

FROM {typ}Moves m
JOIN {typ}Games g ON m.GameID = g.GameID
JOIN vw{typ}MoveScores v ON m.MoveID = v.MoveID
JOIN GamePhases gp ON v.PhaseID = gp.PhaseID
JOIN ScoreReference s ON v.BestEvalGroup = s.BestEvalGroup AND v.PlayedEvalGroup = s.PlayedEvalGroup AND v.ACPL_Group = s.ACPL_Group

WHERE m.IsTheory = 0
AND m.IsTablebase = 0
AND m.CP_Loss IS NOT NULL
AND ISNUMERIC(m.T1_Eval) = 1
AND ISNUMERIC(m.Move_Eval) = 1
AND ABS(CONVERT(float, m.T1_Eval)) < CAST((SELECT SettingValue FROM DynamicSettings WHERE SettingID = 3) AS float)
AND ABS(CONVERT(float, m.Move_Eval)) < CAST((SELECT SettingValue FROM DynamicSettings WHERE SettingID = 3) AS float)
'''

    if lastname:
        qry = qry + f'''AND (CASE WHEN m.Color = 'White' THEN g.WhiteLast ELSE g.BlackLast END) = '{lastname}'
'''
    if firstname:
        qry = qry + f'''AND (CASE WHEN m.Color = 'White' THEN g.WhiteFirst ELSE g.BlackFirst END) = '{firstname}'
'''
    if tmnt:
        qry = qry + f'''AND g.Tournament = '{tmnt}'
'''
    if roundnum:
        qry = qry + f'''AND g.RoundNum = '{roundnum}'
'''
    if color:
        qry = qry + f'''AND m.Color = '{color}'
'''
    if startdate:
        qry = qry + f'''AND g.GameDate >= '{startdate}'
'''
    if enddate:
        qry = qry + f'''AND g.GameDate <= '{enddate}'
'''
    if result:
        qry = qry + f'''AND g.Result = '{result}'
'''

    qry = qry + '''
GROUP BY
CASE WHEN m.Color = 'White' THEN g.WhiteLast ELSE g.BlackLast END,
CASE WHEN m.Color = 'White' THEN g.WhiteFirst ELSE g.BlackFirst END
'''
    return qry
