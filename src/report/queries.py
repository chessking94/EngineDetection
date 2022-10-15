def event_avgrating(event):
    qry = f"""
SELECT
ROUND(AVG((WhiteElo + BlackElo)/2), 0) AS AvgGameRating,
ROUND(MIN((WhiteElo + BlackElo)/2), 0) AS AvgGameRatingMin,
ROUND(MAX((WhiteElo + BlackElo)/2), 0) AS AvgGameRatingMax

FROM ControlGames

WHERE Tournament = '{event}'
"""
    return qry


def event_playergames(event):
    qry = f"""
SELECT
CASE
    WHEN NULLIF(TRIM(CASE WHEN m.Color = 'White' THEN g.WhiteFirst ELSE g.BlackFirst END), '') IS NULL
        THEN (CASE WHEN m.Color = 'White' THEN g.WhiteLast ELSE g.BlackLast END)
    ELSE (CASE WHEN m.Color = 'White' THEN g.WhiteFirst ELSE g.BlackFirst END) + ' ' + (CASE WHEN m.Color = 'White' THEN g.WhiteLast ELSE g.BlackLast END)
END AS Name,
AVG(CASE WHEN m.Color = 'White' THEN g.WhiteElo ELSE g.BlackElo END) Rating,
COUNT(v.MoveID) AS ScoredMoves

FROM vwControlMoveScores v
JOIN ControlMoves m ON v.MoveID = m.MoveID
JOIN ControlGames g ON m.GameID = g.GameID

WHERE g.Tournament = '{event}'
AND m.IsTheory = 0
AND m.IsTablebase = 0
AND ISNUMERIC(m.T1_Eval) = 1
AND ISNUMERIC(m.Move_Eval) = 1
AND ABS(CONVERT(float, m.T1_Eval)) < CAST((SELECT SettingValue FROM DynamicSettings WHERE SettingID = 3) AS float)

GROUP BY
CASE
    WHEN NULLIF(TRIM(CASE WHEN m.Color = 'White' THEN g.WhiteFirst ELSE g.BlackFirst END), '') IS NULL
        THEN (CASE WHEN m.Color = 'White' THEN g.WhiteLast ELSE g.BlackLast END)
    ELSE (CASE WHEN m.Color = 'White' THEN g.WhiteFirst ELSE g.BlackFirst END) + ' ' + (CASE WHEN m.Color = 'White' THEN g.WhiteLast ELSE g.BlackLast END)
END

ORDER BY 1
"""
    return qry


def event_playeropp(player, event):
    qry = f"""
SELECT
g.GameID,
CASE WHEN ISNUMERIC(g.RoundNum) = 0 THEN 0 ELSE FLOOR(CONVERT(decimal(5, 3), g.RoundNum)) END AS RoundNum,
CASE WHEN (CASE WHEN NULLIF(g.WhiteFirst, '') IS NULL THEN '' ELSE g.WhiteFirst + ' ' END) + g.WhiteLast = '{player}' THEN 'w' ELSE 'b' END AS Color,
CASE
    WHEN (CASE WHEN NULLIF(g.WhiteFirst, '') IS NULL THEN '' ELSE g.WhiteFirst + ' ' END) + g.WhiteLast = '{player}' AND g.Result = 1 THEN 'W'
    WHEN (CASE WHEN NULLIF(g.BlackFirst, '') IS NULL THEN '' ELSE g.BlackFirst + ' ' END) + g.BlackLast = '{player}' AND g.Result = 0 THEN 'W'
    WHEN (CASE WHEN NULLIF(g.WhiteFirst, '') IS NULL THEN '' ELSE g.WhiteFirst + ' ' END) + g.WhiteLast = '{player}' AND g.Result = 0 THEN 'L'
    WHEN (CASE WHEN NULLIF(g.BlackFirst, '') IS NULL THEN '' ELSE g.BlackFirst + ' ' END) + g.BlackLast = '{player}' AND g.Result = 1 THEN 'L'
    ELSE 'D'
END AS Result,
CASE
    WHEN (CASE WHEN NULLIF(g.WhiteFirst, '') IS NULL THEN '' ELSE g.WhiteFirst + ' ' END) + g.WhiteLast = '{player}' THEN (
        CASE WHEN NULLIF(TRIM(g.BlackFirst), '') IS NULL THEN g.BlackLast ELSE g.BlackFirst + ' ' +  g.BlackLast END)
    ELSE (CASE WHEN NULLIF(TRIM(g.WhiteFirst), '') IS NULL THEN g.WhiteLast ELSE g.WhiteFirst + ' ' +  g.WhiteLast END)
END AS OppName,
AVG(CASE WHEN (CASE WHEN NULLIF(g.WhiteFirst, '') IS NULL THEN '' ELSE g.WhiteFirst + ' ' END) + g.WhiteLast = '{player}' THEN g.BlackElo ELSE g.WhiteElo END) AS OppRating,
SUM(CASE WHEN m.Move_Rank = 1 THEN 1 ELSE 0 END) AS EVM,
COUNT(v.MoveID) AS ScoredMoves,
AVG(CAST(m.CP_Loss AS decimal(5,2))) AS ACPL,
ISNULL(STDEV(CAST(m.CP_Loss AS decimal(5,2))), 0) AS SDCPL,
CASE WHEN ISNULL(100*SUM(v.Score)/NULLIF(SUM(v.MaxScore), 0), 100) > 100 THEN 100 ELSE ISNULL(100*SUM(v.Score)/NULLIF(SUM(v.MaxScore), 0), 100) END AS Score

FROM vwControlMoveScores v
JOIN ControlMoves m ON v.MoveID = m.MoveID
JOIN ControlGames g ON m.GameID = g.GameID

WHERE g.Tournament = '{event}'
AND ((CASE WHEN NULLIF(g.WhiteFirst, '') IS NULL THEN '' ELSE g.WhiteFirst + ' ' END) + g.WhiteLast = '{player}'
    OR (CASE WHEN NULLIF(g.BlackFirst, '') IS NULL THEN '' ELSE g.BlackFirst + ' ' END) + g.BlackLast = '{player}')
AND (CASE
        WHEN NULLIF(TRIM(CASE WHEN m.Color = 'White' THEN g.WhiteFirst ELSE g.BlackFirst END), '') IS NULL
            THEN (CASE WHEN m.Color = 'White' THEN g.WhiteLast ELSE g.BlackLast END)
        ELSE (CASE WHEN m.Color = 'White' THEN g.WhiteFirst ELSE g.BlackFirst END) + ' ' + (CASE WHEN m.Color = 'White' THEN g.WhiteLast ELSE g.BlackLast END)
    END
) = '{player}'
AND m.IsTheory = 0
AND m.IsTablebase = 0
AND ISNUMERIC(m.T1_Eval) = 1
AND ISNUMERIC(m.Move_Eval) = 1
AND ABS(CONVERT(float, m.T1_Eval)) < CAST((SELECT SettingValue FROM DynamicSettings WHERE SettingID = 3) AS float)

GROUP BY
g.GameID,
CASE WHEN ISNUMERIC(g.RoundNum) = 0 THEN 0 ELSE FLOOR(CONVERT(decimal(5, 3), g.RoundNum)) END,
CASE WHEN (CASE WHEN NULLIF(g.WhiteFirst, '') IS NULL THEN '' ELSE g.WhiteFirst + ' ' END) + g.WhiteLast = '{player}' THEN 'w' ELSE 'b' END,
CASE
    WHEN (CASE WHEN NULLIF(g.WhiteFirst, '') IS NULL THEN '' ELSE g.WhiteFirst + ' ' END) + g.WhiteLast = '{player}' AND g.Result = 1 THEN 'W'
    WHEN (CASE WHEN NULLIF(g.BlackFirst, '') IS NULL THEN '' ELSE g.BlackFirst + ' ' END) + g.BlackLast = '{player}' AND g.Result = 0 THEN 'W'
    WHEN (CASE WHEN NULLIF(g.WhiteFirst, '') IS NULL THEN '' ELSE g.WhiteFirst + ' ' END) + g.WhiteLast = '{player}' AND g.Result = 0 THEN 'L'
    WHEN (CASE WHEN NULLIF(g.BlackFirst, '') IS NULL THEN '' ELSE g.BlackFirst + ' ' END) + g.BlackLast = '{player}' AND g.Result = 1 THEN 'L'
    ELSE 'D'
END,
CASE
    WHEN (CASE WHEN NULLIF(g.WhiteFirst, '') IS NULL THEN '' ELSE g.WhiteFirst + ' ' END) + g.WhiteLast = '{player}' THEN (
        CASE WHEN NULLIF(TRIM(g.BlackFirst), '') IS NULL THEN g.BlackLast ELSE g.BlackFirst + ' ' +  g.BlackLast END)
    ELSE (CASE WHEN NULLIF(TRIM(g.WhiteFirst), '') IS NULL THEN g.WhiteLast ELSE g.WhiteFirst + ' ' +  g.WhiteLast END)
END

ORDER BY 2
"""
    return qry


def event_playersummary(event):
    qry = f"""
SELECT
CASE
    WHEN NULLIF(TRIM(CASE WHEN m.Color = 'White' THEN g.WhiteFirst ELSE g.BlackFirst END), '') IS NULL
        THEN (CASE WHEN m.Color = 'White' THEN g.WhiteLast ELSE g.BlackLast END)
    ELSE (CASE WHEN m.Color = 'White' THEN g.WhiteFirst ELSE g.BlackFirst END) + ' ' + (CASE WHEN m.Color = 'White' THEN g.WhiteLast ELSE g.BlackLast END)
END AS Name,
AVG(CASE WHEN m.Color = 'White' THEN g.WhiteElo ELSE g.BlackElo END) Rating,
e.Record,
e.GamesPlayed,
e.Perf,
SUM(CASE WHEN m.Move_Rank = 1 THEN 1 ELSE 0	END) AS EVM,
COUNT(v.MoveID) AS ScoredMoves,
AVG(CAST(m.CP_Loss AS decimal(5,2))) AS ACPL,
ISNULL(STDEV(CAST(m.CP_Loss AS decimal(5,2))), 0) AS SDCPL,
CASE WHEN ISNULL(100*SUM(v.Score)/NULLIF(SUM(v.MaxScore), 0), 100) > 100 THEN 100 ELSE ISNULL(100*SUM(v.Score)/NULLIF(SUM(v.MaxScore), 0), 100) END AS Score,
opp.OppEVM,
opp.OppScoredMoves,
opp.OppACPL,
opp.OppSDCPL,
opp.OppScore

FROM vwControlMoveScores v
JOIN ControlMoves m ON v.MoveID = m.MoveID
JOIN ControlGames g ON m.GameID = g.GameID
JOIN (
    SELECT
    e.Tournament,
    e.LastName,
    e.FirstName,
    SUM(e.ColorResult) AS Record,
    COUNT(e.GameID) AS GamesPlayed,
    dbo.fn_CalcPerfRating(AVG(e.OppElo), SUM(e.ColorResult)/COUNT(e.GameID)) - AVG(e.Elo) AS Perf
    FROM vwControlEventBreakdown e
    GROUP BY
    e.Tournament,
    e.LastName,
    e.FirstName
) e ON
    (CASE WHEN m.Color = 'White' THEN g.WhiteFirst ELSE g.BlackFirst END) = e.FirstName AND
    (CASE WHEN m.Color = 'White' THEN g.WhiteLast ELSE g.BlackLast END) = e.LastName AND
    g.Tournament = e.Tournament
JOIN (
    SELECT
    CASE WHEN m.Color = 'White' THEN g.BlackFirst ELSE g.WhiteFirst END AS FirstName,
    CASE WHEN m.Color = 'White' THEN g.BlackLast ELSE g.WhiteLast END AS LastName,
    g.Tournament,
    SUM(CASE WHEN m.Move_Rank = 1 THEN 1 ELSE 0	END) AS OppEVM,
    COUNT(v.MoveID) AS OppScoredMoves,
    AVG(CAST(m.CP_Loss AS decimal(5,2))) AS OppACPL,
    ISNULL(STDEV(CAST(m.CP_Loss AS decimal(5,2))), 0) AS OppSDCPL,
    CASE WHEN ISNULL(100*SUM(v.Score)/NULLIF(SUM(v.MaxScore), 0), 100) > 100 THEN 100 ELSE ISNULL(100*SUM(v.Score)/NULLIF(SUM(v.MaxScore), 0), 100) END AS OppScore
    FROM vwControlMoveScores v
    JOIN ControlMoves m ON v.MoveID = m.MoveID
    JOIN ControlGames g ON m.GameID = g.GameID
    WHERE m.IsTheory = 0
    AND m.IsTablebase = 0
    AND ISNUMERIC(m.T1_Eval) = 1
    AND ISNUMERIC(m.Move_Eval) = 1
    AND ABS(CONVERT(float, m.T1_Eval)) < CAST((SELECT SettingValue FROM DynamicSettings WHERE SettingID = 3) AS float)
    GROUP BY
    CASE WHEN m.Color = 'White' THEN g.BlackFirst ELSE g.WhiteFirst END,
    CASE WHEN m.Color = 'White' THEN g.BlackLast ELSE g.WhiteLast END,
    g.Tournament
) opp ON
    (CASE WHEN m.Color = 'White' THEN g.WhiteFirst ELSE g.BlackFirst END) = opp.FirstName AND
    (CASE WHEN m.Color = 'White' THEN g.WhiteLast ELSE g.BlackLast END) = opp.LastName AND
    g.Tournament = opp.Tournament

WHERE g.Tournament = '{event}'
AND m.IsTheory = 0
AND m.IsTablebase = 0
AND ISNUMERIC(m.T1_Eval) = 1
AND ISNUMERIC(m.Move_Eval) = 1
AND ABS(CONVERT(float, m.T1_Eval)) < CAST((SELECT SettingValue FROM DynamicSettings WHERE SettingID = 3) AS float)

GROUP BY
CASE
    WHEN NULLIF(TRIM(CASE WHEN m.Color = 'White' THEN g.WhiteFirst ELSE g.BlackFirst END), '') IS NULL
        THEN (CASE WHEN m.Color = 'White' THEN g.WhiteLast ELSE g.BlackLast END)
    ELSE (CASE WHEN m.Color = 'White' THEN g.WhiteFirst ELSE g.BlackFirst END) + ' ' + (CASE WHEN m.Color = 'White' THEN g.WhiteLast ELSE g.BlackLast END)
END,
e.Record,
e.GamesPlayed,
e.Perf,
opp.OppEVM,
opp.OppScoredMoves,
opp.OppACPL,
opp.OppSDCPL,
opp.OppScore

ORDER BY 1
"""
    return qry


def event_scoredmoves(event):
    qry = f"""
SELECT
ROUND(AVG(CASE WHEN m.Color = 'White' THEN g.WhiteElo ELSE g.BlackElo END), 0) AS AvgScoredRating,
COUNT(m.MoveID) AS ScoredMoves,
SUM(CASE WHEN m.Move_Rank <= 1 THEN 1 ELSE 0 END) AS T1,
SUM(CASE WHEN m.Move_Rank <= 2 THEN 1 ELSE 0 END) AS T2,
SUM(CASE WHEN m.Move_Rank <= 3 THEN 1 ELSE 0 END) AS T3,
SUM(CASE WHEN m.Move_Rank <= 4 THEN 1 ELSE 0 END) AS T4,
SUM(CASE WHEN m.Move_Rank <= 5 THEN 1 ELSE 0 END) AS T5,
AVG(CAST(m.CP_Loss AS decimal(5,2))) AS ACPL,
ISNULL(STDEV(CAST(m.CP_Loss AS decimal(5,2))), 0) AS SDCPL

FROM ControlMoves m
JOIN ControlGames g ON m.GameID = g.GameID

WHERE g.Tournament = '{event}'
AND m.IsTheory = 0
AND m.IsTablebase = 0
AND ISNUMERIC(m.T1_Eval) = 1
AND ISNUMERIC(m.Move_Eval) = 1
AND ABS(CONVERT(float, m.T1_Eval)) < CAST((SELECT SettingValue FROM DynamicSettings WHERE SettingID = 3) AS float)
"""
    return qry


def event_summary(event):
    qry = f"""
SELECT
Tournament AS Event,
CONVERT(varchar(10), MIN(GameDate), 101) + ' - ' + CONVERT(varchar(10), MAX(GameDate), 101) AS EventDate,
MAX(CASE WHEN ISNUMERIC(RoundNum) = 0 THEN 0 ELSE FLOOR(CONVERT(decimal(5, 3), RoundNum)) END) AS Rounds,
(COUNT(DISTINCT WhiteLast + WhiteFirst) + COUNT(DISTINCT BlackLast + BlackFirst))/2 AS Players

FROM ControlGames

WHERE Tournament = '{event}'

GROUP BY
Tournament
"""
    return qry


def event_totalmoves(event):
    qry = f"""
SELECT
COUNT(m.MoveID) AS TotalMoves

FROM ControlMoves m
JOIN ControlGames g ON m.GameID = g.GameID

WHERE g.Tournament = '{event}'
"""
    return qry


def event_totalscore(event):
    qry = f"""
SELECT
CASE WHEN ISNULL(100*SUM(v.Score)/NULLIF(SUM(v.MaxScore), 0), 100) > 100 THEN 100 ELSE ISNULL(100*SUM(v.Score)/NULLIF(SUM(v.MaxScore), 0), 100) END AS Score

FROM vwControlMoveScores v
JOIN ControlMoves m ON v.MoveID = m.MoveID
JOIN ControlGames g ON v.GameID = g.GameID

WHERE g.Tournament = '{event}'
AND m.IsTheory = 0
AND m.IsTablebase = 0
AND ISNUMERIC(m.T1_Eval) = 1
AND ISNUMERIC(m.Move_Eval) = 1
AND ABS(CONVERT(float, m.T1_Eval)) < CAST((SELECT SettingValue FROM DynamicSettings WHERE SettingID = 3) AS float)
"""
    return qry


def game_trace(gameid, color):
    qry = f"""
SELECT
m.MoveNumber,
CASE
    WHEN m.IsTheory = 1 THEN 'b'
    WHEN m.IsTablebase = 1 THEN 't'
    WHEN ISNUMERIC(m.T1_Eval) = 0 OR ABS(CAST(m.T1_Eval AS decimal(5,2))) > CAST(ds.SettingValue AS decimal(5,2)) THEN 'e'
    WHEN m.Move_Rank = 1 THEN 'M'
    ELSE '0'
END AS MoveTrace

FROM ControlMoves m
CROSS JOIN DynamicSettings ds

WHERE m.GameID = {gameid}
AND m.Color = '{color}'
AND ds.SettingID = 3

ORDER BY 1
"""
    return qry


def max_eval():
    qry = """
SELECT
CAST(SettingValue AS decimal(5,2))*100 AS Max_Eval

FROM DynamicSettings

WHERE SettingID = 3
"""
    return qry


def player_avgrating(name, startdate, enddate):
    qry = f"""
SELECT
AVG(r.OppElo) AS AvgOppRating,
MIN(r.OppElo) AS MinOppRating,
MAX(r.OppElo) AS MaxOppRating,
AVG(r.Elo) AS Elo

FROM (
    SELECT
    NULLIF(NULLIF(WhiteElo, ''), 0) AS Elo,
    NULLIF(NULLIF(BlackElo, ''), 0) AS OppElo

    FROM ControlGames

    WHERE WhiteFirst = '{name[0]}'
    AND WhiteLast = '{name[1]}'
    AND GameDate BETWEEN '{startdate}' AND '{enddate}'

    UNION ALL

    SELECT
    NULLIF(NULLIF(BlackElo, ''), 0) AS Elo,
    NULLIF(NULLIF(WhiteElo, ''), 0) AS OppElo

    FROM ControlGames
    WHERE BlackFirst = '{name[0]}'
    AND BlackLast = '{name[1]}'
    AND GameDate BETWEEN '{startdate}' AND '{enddate}'
) r
"""
    return qry


def player_playergames(name, startdate, enddate):
    qry = f"""
SELECT
CASE
    WHEN NULLIF(TRIM(CASE WHEN m.Color = 'White' THEN g.WhiteFirst ELSE g.BlackFirst END), '') IS NULL
        THEN (CASE WHEN m.Color = 'White' THEN g.WhiteLast ELSE g.BlackLast END)
    ELSE (CASE WHEN m.Color = 'White' THEN g.WhiteFirst ELSE g.BlackFirst END) + ' ' + (CASE WHEN m.Color = 'White' THEN g.WhiteLast ELSE g.BlackLast END)
END AS Name,
AVG(CASE WHEN m.Color = 'White' THEN g.WhiteElo ELSE g.BlackElo END) Rating,
COUNT(v.MoveID) AS ScoredMoves

FROM vwControlMoveScores v
JOIN ControlMoves m ON v.MoveID = m.MoveID
JOIN ControlGames g ON m.GameID = g.GameID

WHERE (CASE WHEN m.Color = 'White' THEN g.WhiteFirst ELSE g.BlackFirst END) = '{name[0]}'
AND (CASE WHEN m.Color = 'White' THEN g.WhiteLast ELSE g.BlackLast END) = '{name[1]}'
AND g.GameDate BETWEEN '{startdate}' AND '{enddate}'
AND m.IsTheory = 0
AND m.IsTablebase = 0
AND ISNUMERIC(m.T1_Eval) = 1
AND ISNUMERIC(m.Move_Eval) = 1
AND ABS(CONVERT(float, m.T1_Eval)) < CAST((SELECT SettingValue FROM DynamicSettings WHERE SettingID = 3) AS float)

GROUP BY
CASE
    WHEN NULLIF(TRIM(CASE WHEN m.Color = 'White' THEN g.WhiteFirst ELSE g.BlackFirst END), '') IS NULL
        THEN (CASE WHEN m.Color = 'White' THEN g.WhiteLast ELSE g.BlackLast END)
    ELSE (CASE WHEN m.Color = 'White' THEN g.WhiteFirst ELSE g.BlackFirst END) + ' ' + (CASE WHEN m.Color = 'White' THEN g.WhiteLast ELSE g.BlackLast END)
END

ORDER BY 1
"""
    return qry


def player_playeropp(player, startdate, enddate):
    qry = f"""
SELECT
g.GameID,
CASE WHEN ISNUMERIC(g.RoundNum) = 0 THEN 0 ELSE FLOOR(CONVERT(decimal(5, 3), g.RoundNum)) END AS RoundNum,
CASE WHEN (CASE WHEN NULLIF(g.WhiteFirst, '') IS NULL THEN '' ELSE g.WhiteFirst + ' ' END) + g.WhiteLast = '{player}' THEN 'w' ELSE 'b' END AS Color,
CASE
    WHEN (CASE WHEN NULLIF(g.WhiteFirst, '') IS NULL THEN '' ELSE g.WhiteFirst + ' ' END) + g.WhiteLast = '{player}' AND g.Result = 1 THEN 'W'
    WHEN (CASE WHEN NULLIF(g.BlackFirst, '') IS NULL THEN '' ELSE g.BlackFirst + ' ' END) + g.BlackLast = '{player}' AND g.Result = 0 THEN 'W'
    WHEN (CASE WHEN NULLIF(g.WhiteFirst, '') IS NULL THEN '' ELSE g.WhiteFirst + ' ' END) + g.WhiteLast = '{player}' AND g.Result = 0 THEN 'L'
    WHEN (CASE WHEN NULLIF(g.BlackFirst, '') IS NULL THEN '' ELSE g.BlackFirst + ' ' END) + g.BlackLast = '{player}' AND g.Result = 1 THEN 'L'
    ELSE 'D'
END AS Result,
CASE
    WHEN (CASE WHEN NULLIF(g.WhiteFirst, '') IS NULL THEN '' ELSE g.WhiteFirst + ' ' END) + g.WhiteLast = '{player}' THEN (
        CASE WHEN NULLIF(TRIM(g.BlackFirst), '') IS NULL THEN g.BlackLast ELSE g.BlackFirst + ' ' +  g.BlackLast END)
    ELSE (CASE WHEN NULLIF(TRIM(g.WhiteFirst), '') IS NULL THEN g.WhiteLast ELSE g.WhiteFirst + ' ' +  g.WhiteLast END)
END AS OppName,
AVG(CASE WHEN (CASE WHEN NULLIF(g.WhiteFirst, '') IS NULL THEN '' ELSE g.WhiteFirst + ' ' END) + g.WhiteLast = '{player}' THEN g.BlackElo ELSE g.WhiteElo END) AS OppRating,
SUM(CASE WHEN m.Move_Rank = 1 THEN 1 ELSE 0 END) AS EVM,
COUNT(v.MoveID) AS ScoredMoves,
AVG(CAST(m.CP_Loss AS decimal(5,2))) AS ACPL,
ISNULL(STDEV(CAST(m.CP_Loss AS decimal(5,2))), 0) AS SDCPL,
CASE WHEN ISNULL(100*SUM(v.Score)/NULLIF(SUM(v.MaxScore), 0), 100) > 100 THEN 100 ELSE ISNULL(100*SUM(v.Score)/NULLIF(SUM(v.MaxScore), 0), 100) END AS Score

FROM vwControlMoveScores v
JOIN ControlMoves m ON v.MoveID = m.MoveID
JOIN ControlGames g ON m.GameID = g.GameID

WHERE g.GameDate BETWEEN '{startdate}' AND '{enddate}'
AND ((CASE WHEN NULLIF(g.WhiteFirst, '') IS NULL THEN '' ELSE g.WhiteFirst + ' ' END) + g.WhiteLast = '{player}'
    OR (CASE WHEN NULLIF(g.BlackFirst, '') IS NULL THEN '' ELSE g.BlackFirst + ' ' END) + g.BlackLast = '{player}')
AND (CASE
        WHEN NULLIF(TRIM(CASE WHEN m.Color = 'White' THEN g.WhiteFirst ELSE g.BlackFirst END), '') IS NULL
            THEN (CASE WHEN m.Color = 'White' THEN g.WhiteLast ELSE g.BlackLast END)
        ELSE (CASE WHEN m.Color = 'White' THEN g.WhiteFirst ELSE g.BlackFirst END) + ' ' + (CASE WHEN m.Color = 'White' THEN g.WhiteLast ELSE g.BlackLast END)
    END
) = '{player}'
AND m.IsTheory = 0
AND m.IsTablebase = 0
AND ISNUMERIC(m.T1_Eval) = 1
AND ISNUMERIC(m.Move_Eval) = 1
AND ABS(CONVERT(float, m.T1_Eval)) < CAST((SELECT SettingValue FROM DynamicSettings WHERE SettingID = 3) AS float)

GROUP BY
g.GameID,
CASE WHEN ISNUMERIC(g.RoundNum) = 0 THEN 0 ELSE FLOOR(CONVERT(decimal(5, 3), g.RoundNum)) END,
CASE WHEN (CASE WHEN NULLIF(g.WhiteFirst, '') IS NULL THEN '' ELSE g.WhiteFirst + ' ' END) + g.WhiteLast = '{player}' THEN 'w' ELSE 'b' END,
CASE
    WHEN (CASE WHEN NULLIF(g.WhiteFirst, '') IS NULL THEN '' ELSE g.WhiteFirst + ' ' END) + g.WhiteLast = '{player}' AND g.Result = 1 THEN 'W'
    WHEN (CASE WHEN NULLIF(g.BlackFirst, '') IS NULL THEN '' ELSE g.BlackFirst + ' ' END) + g.BlackLast = '{player}' AND g.Result = 0 THEN 'W'
    WHEN (CASE WHEN NULLIF(g.WhiteFirst, '') IS NULL THEN '' ELSE g.WhiteFirst + ' ' END) + g.WhiteLast = '{player}' AND g.Result = 0 THEN 'L'
    WHEN (CASE WHEN NULLIF(g.BlackFirst, '') IS NULL THEN '' ELSE g.BlackFirst + ' ' END) + g.BlackLast = '{player}' AND g.Result = 1 THEN 'L'
    ELSE 'D'
END,
CASE
    WHEN (CASE WHEN NULLIF(g.WhiteFirst, '') IS NULL THEN '' ELSE g.WhiteFirst + ' ' END) + g.WhiteLast = '{player}' THEN (
        CASE WHEN NULLIF(TRIM(g.BlackFirst), '') IS NULL THEN g.BlackLast ELSE g.BlackFirst + ' ' +  g.BlackLast END)
    ELSE (CASE WHEN NULLIF(TRIM(g.WhiteFirst), '') IS NULL THEN g.WhiteLast ELSE g.WhiteFirst + ' ' +  g.WhiteLast END)
END

ORDER BY 1
"""
    return qry


def player_playersummary(name, startdate, enddate):
    qry = f"""
SELECT
(CASE WHEN m.Color = 'White' THEN g.WhiteFirst ELSE g.BlackFirst END) + ' ' + (CASE WHEN m.Color = 'White' THEN g.WhiteLast ELSE g.BlackLast END) AS Name,
AVG(CASE WHEN m.Color = 'White' THEN g.WhiteElo ELSE g.BlackElo END) Rating,
e.Record,
e.GamesPlayed,
e.Perf,
SUM(CASE WHEN m.Move_Rank = 1 THEN 1 ELSE 0	END) AS EVM,
COUNT(v.MoveID) AS ScoredMoves,
AVG(CAST(m.CP_Loss AS decimal(5,2))) AS ACPL,
ISNULL(STDEV(CAST(m.CP_Loss AS decimal(5,2))), 0) AS SDCPL,
CASE WHEN ISNULL(100*SUM(v.Score)/NULLIF(SUM(v.MaxScore), 0), 100) > 100 THEN 100 ELSE ISNULL(100*SUM(v.Score)/NULLIF(SUM(v.MaxScore), 0), 100) END AS Score,
opp.OppEVM,
opp.OppScoredMoves,
opp.OppACPL,
opp.OppSDCPL,
opp.OppScore

FROM vwControlMoveScores v
JOIN ControlMoves m ON v.MoveID = m.MoveID
JOIN ControlGames g ON m.GameID = g.GameID
JOIN (
    SELECT
    CASE WHEN WhiteFirst = '{name[0]}' AND WhiteLast = '{name[1]}' THEN WhiteLast ELSE BlackLast END AS LastName,
    CASE WHEN WhiteFirst = '{name[0]}' AND WhiteLast = '{name[1]}' THEN WhiteFirst ELSE BlackFirst END AS FirstName,
    SUM(CASE WHEN BlackFirst = '{name[0]}' AND BlackLast = '{name[1]}' THEN 1 - Result ELSE Result END) AS Record,
    COUNT(GameID) AS GamesPlayed,
    dbo.fn_CalcPerfRating(
        AVG(CASE WHEN WhiteFirst = '{name[0]}' AND WhiteLast = '{name[1]}' THEN BlackElo ELSE WhiteElo END),
        SUM(CASE WHEN BlackFirst = '{name[0]}' AND BlackLast = '{name[1]}' THEN 1 - Result ELSE Result END)/COUNT(GameID)
    ) - AVG(CASE WHEN WhiteFirst = '{name[0]}' AND WhiteLast = '{name[1]}' THEN WhiteElo ELSE BlackElo END) AS Perf
    FROM ControlGames
    WHERE ((WhiteFirst = '{name[0]}' AND WhiteLast = '{name[1]}') OR (BlackFirst = '{name[0]}' AND BlackLast = '{name[1]}'))
    AND GameDate BETWEEN '{startdate}' AND '{enddate}'
    GROUP BY
    CASE WHEN WhiteFirst = '{name[0]}' AND WhiteLast = '{name[1]}' THEN WhiteLast ELSE BlackLast END,
    CASE WHEN WhiteFirst = '{name[0]}' AND WhiteLast = '{name[1]}' THEN WhiteFirst ELSE BlackFirst END
) e ON
    (CASE WHEN m.Color = 'White' THEN g.WhiteFirst ELSE g.BlackFirst END) = e.FirstName AND
    (CASE WHEN m.Color = 'White' THEN g.WhiteLast ELSE g.BlackLast END) = e.LastName
JOIN (
    SELECT
    CASE WHEN m.Color = 'White' THEN g.BlackFirst ELSE g.WhiteFirst END AS FirstName,
    CASE WHEN m.Color = 'White' THEN g.BlackLast ELSE g.WhiteLast END AS LastName,
    SUM(CASE WHEN m.Move_Rank = 1 THEN 1 ELSE 0	END) AS OppEVM,
    COUNT(v.MoveID) AS OppScoredMoves,
    AVG(CAST(m.CP_Loss AS decimal(5,2))) AS OppACPL,
    ISNULL(STDEV(CAST(m.CP_Loss AS decimal(5,2))), 0) AS OppSDCPL,
    CASE WHEN ISNULL(100*SUM(v.Score)/NULLIF(SUM(v.MaxScore), 0), 100) > 100 THEN 100 ELSE ISNULL(100*SUM(v.Score)/NULLIF(SUM(v.MaxScore), 0), 100) END AS OppScore
    FROM vwControlMoveScores v
    JOIN ControlMoves m ON v.MoveID = m.MoveID
    JOIN ControlGames g ON m.GameID = g.GameID
    WHERE (
        (g.WhiteFirst = '{name[0]}' AND g.WhiteLast = '{name[1]}' AND m.Color = 'Black') OR
        (g.BlackFirst = '{name[0]}' AND g.BlackLast = '{name[1]}' AND m.Color = 'White')
    )
    AND g.GameDate BETWEEN '{startdate}' AND '{enddate}'
    AND m.IsTheory = 0
    AND m.IsTablebase = 0
    AND ISNUMERIC(m.T1_Eval) = 1
    AND ISNUMERIC(m.Move_Eval) = 1
    AND ABS(CONVERT(float, m.T1_Eval)) < CAST((SELECT SettingValue FROM DynamicSettings WHERE SettingID = 3) AS float)
    GROUP BY
    CASE WHEN m.Color = 'White' THEN g.BlackFirst ELSE g.WhiteFirst END,
    CASE WHEN m.Color = 'White' THEN g.BlackLast ELSE g.WhiteLast END
) opp ON
    (CASE WHEN m.Color = 'White' THEN g.WhiteFirst ELSE g.BlackFirst END) = opp.FirstName AND
    (CASE WHEN m.Color = 'White' THEN g.WhiteLast ELSE g.BlackLast END) = opp.LastName

WHERE (CASE WHEN m.Color = 'White' THEN g.WhiteFirst ELSE g.BlackFirst END) = '{name[0]}'
AND (CASE WHEN m.Color = 'White' THEN g.WhiteLast ELSE g.BlackLast END) = '{name[1]}'
AND g.GameDate BETWEEN '{startdate}' AND '{enddate}'
AND m.IsTheory = 0
AND m.IsTablebase = 0
AND ISNUMERIC(m.T1_Eval) = 1
AND ISNUMERIC(m.Move_Eval) = 1
AND ABS(CONVERT(float, m.T1_Eval)) < CAST((SELECT SettingValue FROM DynamicSettings WHERE SettingID = 3) AS float)

GROUP BY
(CASE WHEN m.Color = 'White' THEN g.WhiteFirst ELSE g.BlackFirst END) + ' ' + (CASE WHEN m.Color = 'White' THEN g.WhiteLast ELSE g.BlackLast END),
e.Record,
e.GamesPlayed,
e.Perf,
opp.OppEVM,
opp.OppScoredMoves,
opp.OppACPL,
opp.OppSDCPL,
opp.OppScore
"""
    return qry


def player_scoredmoves(name, startdate, enddate):
    qry = f"""
SELECT
NULL AS AvgScoredRating,
COUNT(m.MoveID) AS ScoredMoves,
SUM(CASE WHEN m.Move_Rank <= 1 THEN 1 ELSE 0 END) AS T1,
SUM(CASE WHEN m.Move_Rank <= 2 THEN 1 ELSE 0 END) AS T2,
SUM(CASE WHEN m.Move_Rank <= 3 THEN 1 ELSE 0 END) AS T3,
SUM(CASE WHEN m.Move_Rank <= 4 THEN 1 ELSE 0 END) AS T4,
SUM(CASE WHEN m.Move_Rank <= 5 THEN 1 ELSE 0 END) AS T5,
AVG(CAST(m.CP_Loss AS decimal(5,2))) AS ACPL,
ISNULL(STDEV(CAST(m.CP_Loss AS decimal(5,2))), 0) AS SDCPL

FROM ControlMoves m
JOIN ControlGames g ON m.GameID = g.GameID

WHERE (CASE WHEN m.Color = 'White' THEN g.WhiteFirst ELSE g.BlackFirst END) = '{name[0]}'
AND (CASE WHEN m.Color = 'White' THEN g.WhiteLast ELSE g.BlackLast END) = '{name[1]}'
AND g.GameDate BETWEEN '{startdate}' AND '{enddate}'
AND m.IsTheory = 0
AND m.IsTablebase = 0
AND ISNUMERIC(m.T1_Eval) = 1
AND ISNUMERIC(m.Move_Eval) = 1
AND ABS(CONVERT(float, m.T1_Eval)) < CAST((SELECT SettingValue FROM DynamicSettings WHERE SettingID = 3) AS float)
"""
    return qry


def player_totalmoves(name, startdate, enddate):
    qry = f"""
SELECT
COUNT(m.MoveID) AS TotalMoves

FROM ControlMoves m
JOIN ControlGames g ON m.GameID = g.GameID

WHERE (CASE WHEN m.Color = 'White' THEN g.WhiteFirst ELSE g.BlackFirst END) = '{name[0]}'
AND (CASE WHEN m.Color = 'White' THEN g.WhiteLast ELSE g.BlackLast END) = '{name[1]}'
AND g.GameDate BETWEEN '{startdate}' AND '{enddate}'
"""
    return qry


def player_totalscore(name, startdate, enddate):
    qry = f"""
SELECT
CASE WHEN ISNULL(100*SUM(v.Score)/NULLIF(SUM(v.MaxScore), 0), 100) > 100 THEN 100 ELSE ISNULL(100*SUM(v.Score)/NULLIF(SUM(v.MaxScore), 0), 100) END AS Score

FROM vwControlMoveScores v
JOIN ControlMoves m ON v.MoveID = m.MoveID
JOIN ControlGames g ON v.GameID = g.GameID

WHERE (CASE WHEN m.Color = 'White' THEN g.WhiteFirst ELSE g.BlackFirst END) = '{name[0]}'
AND (CASE WHEN m.Color = 'White' THEN g.WhiteLast ELSE g.BlackLast END) = '{name[1]}'
AND g.GameDate BETWEEN '{startdate}' AND '{enddate}'
AND m.IsTheory = 0
AND m.IsTablebase = 0
AND ISNUMERIC(m.T1_Eval) = 1
AND ISNUMERIC(m.Move_Eval) = 1
AND ABS(CONVERT(float, m.T1_Eval)) < CAST((SELECT SettingValue FROM DynamicSettings WHERE SettingID = 3) AS float)
"""
    return qry


def roi_calc(agg, src, tc, rating):
    qry = f"""
SELECT
Average,
StandardDeviation,
MaxValue

FROM StatisticsSummary

WHERE Aggregation = '{agg}'
AND Field = 'Score'
AND Source = '{src}'
AND TimeControlType = '{tc}'
AND Rating = {rating}
"""
    return qry
