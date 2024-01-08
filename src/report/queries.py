import pandas as pd


def get_aggid(conn, agg):
    qry = f"""
SELECT
AggregationID

FROM ChessWarehouse.dim.Aggregations

WHERE AggregationName = '{agg}'
"""
    df = pd.read_sql(qry, conn)
    if len(df) == 0:
        idval = None
    else:
        idval = int(df.values[0][0])
    return idval


def get_evid(conn, srcid, event):
    qry = f"""
SELECT
EventID

FROM ChessWarehouse.dim.Events

WHERE SourceID = {srcid}
AND EventName = '{event}'
"""
    df = pd.read_sql(qry, conn)
    if len(df) == 0:
        idval = None
    else:
        idval = int(df.values[0][0])
    return idval


def get_plid(conn, srcid, lname, fname):
    qry = f"""
SELECT
PlayerID

FROM ChessWarehouse.dim.Players

WHERE SourceID = {srcid}
AND LastName = '{lname}'
AND FirstName = '{fname}'
"""
    df = pd.read_sql(qry, conn)
    if len(df) == 0:
        idval = None
    else:
        idval = int(df.values[0][0])
    return idval


def get_scid(conn, scorename):
    qry = f"""
SELECT
ScoreID

FROM ChessWarehouse.dim.Scores

WHERE ScoreName = '{scorename}'
"""
    df = pd.read_sql(qry, conn)
    if len(df) == 0:
        idval = None
    else:
        idval = int(df.values[0][0])
    return idval


def get_srcid(conn, src):
    qry = f"""
SELECT
SourceID

FROM ChessWarehouse.dim.Sources

WHERE SourceName = '{src}'
"""
    df = pd.read_sql(qry, conn)
    if len(df) == 0:
        idval = None
    else:
        idval = int(df.values[0][0])
    return idval


def get_tcid(conn, tc):
    qry = f"""
SELECT
TimeControlID

FROM ChessWarehouse.dim.TimeControls

WHERE TimeControlName = '{tc}'
"""
    df = pd.read_sql(qry, conn)
    if len(df) == 0:
        idval = None
    else:
        idval = int(df.values[0][0])
    return idval


def event_avgrating(eventid):
    qry = f"""
SELECT
ROUND(AVG((WhiteElo + BlackElo)/2), 0) AS AvgGameRating,
ROUND(MIN((WhiteElo + BlackElo)/2), 0) AS AvgGameRatingMin,
ROUND(MAX((WhiteElo + BlackElo)/2), 0) AS AvgGameRatingMax

FROM ChessWarehouse.lake.Games

WHERE EventID = {eventid}
"""
    return qry


def event_playergames(eventid):
    qry = f"""
SELECT
CASE WHEN c.Color = 'White' THEN g.WhitePlayerID ELSE g.BlackPlayerID END AS PlayerID,
CASE
    WHEN NULLIF(TRIM(CASE WHEN c.Color = 'White' THEN wp.FirstName ELSE bp.FirstName END), '') IS NULL
        THEN (CASE WHEN c.Color = 'White' THEN wp.LastName ELSE bp.LastName END)
    ELSE (CASE WHEN c.Color = 'White' THEN wp.FirstName ELSE bp.FirstName END) + ' ' + (CASE WHEN c.Color = 'White' THEN wp.LastName ELSE bp.LastName END)
END AS Name,
AVG(CASE WHEN c.Color = 'White' THEN g.WhiteElo ELSE g.BlackElo END) Rating,
COUNT(m.MoveNumber) AS ScoredMoves

FROM ChessWarehouse.lake.Moves m
JOIN ChessWarehouse.lake.Games g ON
    m.GameID = g.GameID
JOIN ChessWarehouse.dim.Colors c ON
    m.ColorID = c.ColorID
JOIN ChessWarehouse.dim.Players wp ON
    g.WhitePlayerID = wp.PlayerID
JOIN ChessWarehouse.dim.Players bp ON
    g.BlackPlayerID = bp.PlayerID

WHERE g.EventID = {eventid}
AND m.MoveScored = 1

GROUP BY
CASE WHEN c.Color = 'White' THEN g.WhitePlayerID ELSE g.BlackPlayerID END,
CASE
    WHEN NULLIF(TRIM(CASE WHEN c.Color = 'White' THEN wp.FirstName ELSE bp.FirstName END), '') IS NULL
        THEN (CASE WHEN c.Color = 'White' THEN wp.LastName ELSE bp.LastName END)
    ELSE (CASE WHEN c.Color = 'White' THEN wp.FirstName ELSE bp.FirstName END) + ' ' + (CASE WHEN c.Color = 'White' THEN wp.LastName ELSE bp.LastName END)
END

ORDER BY 2
"""
    return qry


def event_playeropp(playerid, eventid, scoreid):
    qry = f"""
SELECT
g.GameID,
g.RoundNum,
CASE WHEN g.WhitePlayerID = {playerid} THEN 'w' ELSE 'b' END AS Color,
CASE
    WHEN g.WhitePlayerID = {playerid} AND g.Result = 1 THEN 'W'
    WHEN g.BlackPlayerID = {playerid} AND g.Result = 0 THEN 'W'
    WHEN g.WhitePlayerID = {playerid} AND g.Result = 0 THEN 'L'
    WHEN g.BlackPlayerID = {playerid} AND g.Result = 1 THEN 'L'
    ELSE 'D'
END AS Result,
CASE
    WHEN g.WhitePlayerID = {playerid} THEN (CASE WHEN NULLIF(TRIM(bp.FirstName), '') IS NULL THEN bp.LastName ELSE bp.FirstName + ' ' +  bp.LastName END)
    ELSE (CASE WHEN NULLIF(TRIM(wp.FirstName), '') IS NULL THEN wp.LastName ELSE wp.FirstName + ' ' +  wp.LastName END)
END AS OppName,
AVG(CASE WHEN g.WhitePlayerID = {playerid} THEN g.BlackElo ELSE g.WhiteElo END) AS OppRating,
SUM(CASE WHEN m.Move_Rank = 1 THEN 1 ELSE 0 END) AS EVM,
COUNT(m.MoveNumber) AS ScoredMoves,
AVG(m.ScACPL) AS ACPL,
ISNULL(STDEV(m.ScACPL), 0) AS SDCPL,
CASE
    WHEN ISNULL(100*SUM(ms.ScoreValue)/NULLIF(SUM(ms.MaxScoreValue), 0), 100) > 100 THEN 100
    ELSE ISNULL(100*SUM(ms.ScoreValue)/NULLIF(SUM(ms.MaxScoreValue), 0), 100)
END AS Score

FROM ChessWarehouse.lake.Moves m
JOIN ChessWarehouse.stat.MoveScores ms ON
    m.GameID = ms.GameID AND
    m.MoveNumber = ms.MoveNumber AND
    m.ColorID = ms.ColorID
JOIN ChessWarehouse.lake.Games g ON
    m.GameID = g.GameID
JOIN ChessWarehouse.dim.Colors c ON
    m.ColorID = c.ColorID
JOIN ChessWarehouse.dim.Players wp ON
    g.WhitePlayerID = wp.PlayerID
JOIN ChessWarehouse.dim.Players bp ON
    g.BlackPlayerID = bp.PlayerID

WHERE g.EventID = {eventid}
AND (g.WhitePlayerID = {playerid} OR g.BlackPlayerID = {playerid})
AND (CASE WHEN c.Color = 'White' THEN g.WhitePlayerID ELSE g.BlackPlayerID END) = {playerid}
AND ms.ScoreID = {scoreid}
AND m.MoveScored = 1

GROUP BY
g.GameID,
g.RoundNum,
CASE WHEN g.WhitePlayerID = {playerid} THEN 'w' ELSE 'b' END,
CASE
    WHEN g.WhitePlayerID = {playerid} AND g.Result = 1 THEN 'W'
    WHEN g.BlackPlayerID = {playerid} AND g.Result = 0 THEN 'W'
    WHEN g.WhitePlayerID = {playerid} AND g.Result = 0 THEN 'L'
    WHEN g.BlackPlayerID = {playerid} AND g.Result = 1 THEN 'L'
    ELSE 'D'
END,
CASE
    WHEN g.WhitePlayerID = {playerid} THEN (CASE WHEN NULLIF(TRIM(bp.FirstName), '') IS NULL THEN bp.LastName ELSE bp.FirstName + ' ' +  bp.LastName END)
    ELSE (CASE WHEN NULLIF(TRIM(wp.FirstName), '') IS NULL THEN wp.LastName ELSE wp.FirstName + ' ' +  wp.LastName END)
END

ORDER BY 2
"""
    return qry


def event_playersummary(eventid, scoreid):
    qry = f"""
SELECT
CASE
    WHEN NULLIF(TRIM(CASE WHEN c.Color = 'White' THEN wp.FirstName ELSE bp.FirstName END), '') IS NULL
        THEN (CASE WHEN c.Color = 'White' THEN wp.LastName ELSE bp.LastName END)
    ELSE (CASE WHEN c.Color = 'White' THEN wp.FirstName ELSE bp.FirstName END) + ' ' + (CASE WHEN c.Color = 'White' THEN wp.LastName ELSE bp.LastName END)
END AS Name,
AVG(CASE WHEN c.Color = 'White' THEN g.WhiteElo ELSE g.BlackElo END) Rating,
e.Record,
e.GamesPlayed,
e.Perf,
SUM(CASE WHEN m.Move_Rank = 1 THEN 1 ELSE 0	END) AS EVM,
SUM(CASE WHEN m.CP_Loss >= 2 THEN 1 ELSE 0 END) AS Blunders,
COUNT(m.MoveNumber) AS ScoredMoves,
AVG(m.ScACPL) AS ACPL,
ISNULL(STDEV(m.ScACPL), 0) AS SDCPL,
CASE
    WHEN ISNULL(100*SUM(ms.ScoreValue)/NULLIF(SUM(ms.MaxScoreValue), 0), 100) > 100 THEN 100
    ELSE ISNULL(100*SUM(ms.ScoreValue)/NULLIF(SUM(ms.MaxScoreValue), 0), 100)
END AS Score,
opp.OppEVM,
opp.OppBlunders,
opp.OppScoredMoves,
opp.OppACPL,
opp.OppSDCPL,
opp.OppScore

FROM ChessWarehouse.lake.Moves m
JOIN ChessWarehouse.stat.MoveScores ms ON
    m.GameID = ms.GameID AND
    m.MoveNumber = ms.MoveNumber AND
    m.ColorID = ms.ColorID
JOIN ChessWarehouse.lake.Games g ON
    m.GameID = g.GameID
JOIN ChessWarehouse.dim.Colors c ON
    m.ColorID = c.ColorID
JOIN ChessWarehouse.dim.Players wp ON
    g.WhitePlayerID = wp.PlayerID
JOIN ChessWarehouse.dim.Players bp ON
    g.BlackPlayerID = bp.PlayerID
JOIN (
    SELECT
    EventID,
    PlayerID,
    SUM(ColorResult) AS Record,
    COUNT(GameID) AS GamesPlayed,
    dbo.GetPerfRating(AVG(OppElo), SUM(ColorResult)/COUNT(GameID)) - AVG(Elo) AS Perf

    FROM ChessWarehouse.lake.vwEventBreakdown

    GROUP BY
    EventID,
    PlayerID
) e ON
    (CASE WHEN c.Color = 'White' THEN g.WhitePlayerID ELSE g.BlackPlayerID END) = e.PlayerID AND
    g.EventID = e.EventID
LEFT JOIN (
    SELECT
    CASE WHEN c.Color = 'White' THEN g.BlackPlayerID ELSE g.WhitePlayerID END AS OppPlayerID,
    g.EventID,
    SUM(CASE WHEN m.Move_Rank = 1 THEN 1 ELSE 0	END) AS OppEVM,
    SUM(CASE WHEN m.CP_Loss >= 2 THEN 1 ELSE 0 END) AS OppBlunders,
    COUNT(m.MoveNumber) AS OppScoredMoves,
    AVG(m.ScACPL) AS OppACPL,
    ISNULL(STDEV(m.ScACPL), 0) AS OppSDCPL,
    CASE
        WHEN ISNULL(100*SUM(ms.ScoreValue)/NULLIF(SUM(ms.MaxScoreValue), 0), 100) > 100 THEN 100
        ELSE ISNULL(100*SUM(ms.ScoreValue)/NULLIF(SUM(ms.MaxScoreValue), 0), 100)
    END AS OppScore

    FROM ChessWarehouse.lake.Moves m
    JOIN ChessWarehouse.stat.MoveScores ms ON
        m.GameID = ms.GameID AND
        m.MoveNumber = ms.MoveNumber AND
        m.ColorID = ms.ColorID
    JOIN ChessWarehouse.lake.Games g ON
        m.GameID = g.GameID
    JOIN ChessWarehouse.dim.Colors c ON
        m.ColorID = c.ColorID

    WHERE ms.ScoreID = {scoreid}
    AND m.MoveScored = 1

    GROUP BY
    CASE WHEN c.Color = 'White' THEN g.BlackPlayerID ELSE g.WhitePlayerID END,
    g.EventID
) opp ON
    (CASE WHEN c.Color = 'White' THEN g.WhitePlayerID ELSE g.BlackPlayerID END) = opp.OppPlayerID AND
    g.EventID = opp.EventID

WHERE g.EventID = {eventid}
AND ms.ScoreID = {scoreid}
AND m.MoveScored = 1

GROUP BY
CASE
    WHEN NULLIF(TRIM(CASE WHEN c.Color = 'White' THEN wp.FirstName ELSE bp.FirstName END), '') IS NULL
        THEN (CASE WHEN c.Color = 'White' THEN wp.LastName ELSE bp.LastName END)
    ELSE (CASE WHEN c.Color = 'White' THEN wp.FirstName ELSE bp.FirstName END) + ' ' + (CASE WHEN c.Color = 'White' THEN wp.LastName ELSE bp.LastName END)
END,
e.Record,
e.GamesPlayed,
e.Perf,
opp.OppEVM,
opp.OppBlunders,
opp.OppScoredMoves,
opp.OppACPL,
opp.OppSDCPL,
opp.OppScore

ORDER BY 1
"""
    return qry


def event_scoredmoves(eventid):
    qry = f"""
SELECT
ROUND(AVG(CASE WHEN c.Color = 'White' THEN g.WhiteElo ELSE g.BlackElo END), 0) AS AvgScoredRating,
COUNT(m.MoveNumber) AS ScoredMoves,
SUM(CASE WHEN m.Move_Rank <= 1 THEN 1 ELSE 0 END) AS T1,
SUM(CASE WHEN m.Move_Rank <= 2 THEN 1 ELSE 0 END) AS T2,
SUM(CASE WHEN m.Move_Rank <= 3 THEN 1 ELSE 0 END) AS T3,
SUM(CASE WHEN m.Move_Rank <= 4 THEN 1 ELSE 0 END) AS T4,
SUM(CASE WHEN m.Move_Rank <= 5 THEN 1 ELSE 0 END) AS T5,
AVG(m.ScACPL) AS ACPL,
ISNULL(STDEV(m.ScACPL), 0) AS SDCPL,
SUM(CASE WHEN m.CP_Loss > 2 THEN 1 ELSE 0 END) AS Blunders

FROM ChessWarehouse.lake.Moves m
JOIN ChessWarehouse.lake.Games g ON
    m.GameID = g.GameID
JOIN ChessWarehouse.dim.Colors c ON
    m.ColorID = c.ColorID

WHERE g.EventID = {eventid}
AND m.MoveScored = 1
"""
    return qry


def event_summary(event):
    qry = f"""
SELECT
e.EventName,
CONVERT(varchar(10), MIN(g.GameDate), 101) + ' - ' + CONVERT(varchar(10), MAX(g.GameDate), 101) AS EventDate,
MAX(g.RoundNum) AS Rounds,
(COUNT(DISTINCT g.WhitePlayerID) + COUNT(DISTINCT g.BlackPlayerID))/2 AS Players

FROM ChessWarehouse.lake.Games g
JOIN ChessWarehouse.dim.Events e ON
    g.EventID = e.EventID

WHERE e.EventName = '{event}'

GROUP BY
e.EventName
"""
    return qry


def event_totalmoves(eventid):
    qry = f"""
SELECT
'Total' AS TraceKey,
'Total Moves' AS TraceDescription,
COUNT(m.MoveNumber) AS MoveCount

FROM ChessWarehouse.lake.Moves m
JOIN ChessWarehouse.lake.Games g ON
    m.GameID = g.GameID

WHERE g.EventID = {eventid}

UNION

SELECT
t.TraceKey,
t.TraceDescription,
COUNT(m.MoveNumber) AS MoveCount

FROM ChessWarehouse.lake.Moves m
JOIN ChessWarehouse.lake.Games g ON
    m.GameID = g.GameID
JOIN ChessWarehouse.dim.Traces t ON
    m.TraceKey = t.TraceKey

WHERE g.EventID = {eventid}
AND t.TraceKey NOT IN ('0', 'M')

GROUP BY
t.TraceKey,
t.TraceDescription

ORDER BY
MoveCount DESC
"""
    return qry


def event_totalscore(eventid, scoreid):
    qry = f"""
SELECT
CASE
    WHEN ISNULL(100*SUM(ms.ScoreValue)/NULLIF(SUM(ms.MaxScoreValue), 0), 100) > 100 THEN 100
    ELSE ISNULL(100*SUM(ms.ScoreValue)/NULLIF(SUM(ms.MaxScoreValue), 0), 100)
END AS Score

FROM ChessWarehouse.lake.Moves m
JOIN ChessWarehouse.stat.MoveScores ms ON
    m.GameID = ms.GameID AND
    m.MoveNumber = ms.MoveNumber AND
    m.ColorID = ms.ColorID
JOIN ChessWarehouse.lake.Games g ON
    m.GameID = g.GameID

WHERE g.EventID = {eventid}
AND ms.ScoreID = {scoreid}
AND m.MoveScored = 1
"""
    return qry


def game_trace(gameid, colorid):
    qry = f"""
SELECT
m.MoveNumber,
m.TraceKey AS MoveTrace

FROM ChessWarehouse.lake.Moves m

WHERE m.GameID = {gameid}
AND m.ColorID = {colorid}

ORDER BY 1
"""
    return qry


def max_eval():
    qry = """
SELECT
CAST(Value AS decimal(5,2))*100 AS Max_Eval

FROM ChessWarehouse.dbo.Settings

WHERE ID = 3
"""
    return qry


def player_avgrating(playerid, startdate, enddate):
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

    FROM ChessWarehouse.lake.Games

    WHERE WhitePlayerID = '{playerid}'
    AND GameDate BETWEEN '{startdate}' AND '{enddate}'

    UNION ALL

    SELECT
    NULLIF(NULLIF(BlackElo, ''), 0) AS Elo,
    NULLIF(NULLIF(WhiteElo, ''), 0) AS OppElo

    FROM ChessWarehouse.lake.Games

    WHERE BlackPlayerID = '{playerid}'
    AND GameDate BETWEEN '{startdate}' AND '{enddate}'
) r
"""
    return qry


def player_playergames(playerid, startdate, enddate):
    qry = f"""
SELECT
CASE WHEN c.Color = 'White' THEN g.WhitePlayerID ELSE g.BlackPlayerID END AS PlayerID,
CASE
    WHEN NULLIF(TRIM(CASE WHEN c.Color = 'White' THEN wp.FirstName ELSE bp.FirstName END), '') IS NULL
        THEN (CASE WHEN c.Color = 'White' THEN wp.LastName ELSE bp.LastName END)
    ELSE (CASE WHEN c.Color = 'White' THEN wp.FirstName ELSE bp.FirstName END) + ' ' + (CASE WHEN c.Color = 'White' THEN wp.LastName ELSE bp.LastName END)
END AS Name,
AVG(CASE WHEN c.Color = 'White' THEN g.WhiteElo ELSE g.BlackElo END) Rating,
COUNT(m.MoveNumber) AS ScoredMoves

FROM ChessWarehouse.lake.Moves m
JOIN ChessWarehouse.lake.Games g ON
    m.GameID = g.GameID
JOIN ChessWarehouse.dim.Colors c ON
    m.ColorID = c.ColorID
JOIN ChessWarehouse.dim.Players wp ON
    g.WhitePlayerID = wp.PlayerID
JOIN ChessWarehouse.dim.Players bp ON
    g.BlackPlayerID = bp.PlayerID

WHERE (CASE WHEN c.Color = 'White' THEN g.WhitePlayerID ELSE g.BlackPlayerID END) = '{playerid}'
AND g.GameDate BETWEEN '{startdate}' AND '{enddate}'
AND m.MoveScored = 1

GROUP BY
CASE WHEN c.Color = 'White' THEN g.WhitePlayerID ELSE g.BlackPlayerID END,
CASE
    WHEN NULLIF(TRIM(CASE WHEN c.Color = 'White' THEN wp.FirstName ELSE bp.FirstName END), '') IS NULL
        THEN (CASE WHEN c.Color = 'White' THEN wp.LastName ELSE bp.LastName END)
    ELSE (CASE WHEN c.Color = 'White' THEN wp.FirstName ELSE bp.FirstName END) + ' ' + (CASE WHEN c.Color = 'White' THEN wp.LastName ELSE bp.LastName END)
END

ORDER BY 2
"""
    return qry


def player_playeropp(playerid, startdate, enddate, scoreid):
    qry = f"""
SELECT
g.GameID,
g.RoundNum,
CASE WHEN g.WhitePlayerID = {playerid} THEN 'w' ELSE 'b' END AS Color,
CASE
    WHEN g.WhitePlayerID = {playerid} AND g.Result = 1 THEN 'W'
    WHEN g.BlackPlayerID = {playerid} AND g.Result = 0 THEN 'W'
    WHEN g.WhitePlayerID = {playerid} AND g.Result = 0 THEN 'L'
    WHEN g.BlackPlayerID = {playerid} AND g.Result = 1 THEN 'L'
    ELSE 'D'
END AS Result,
CASE
    WHEN g.WhitePlayerID = {playerid} THEN (CASE WHEN NULLIF(bp.FirstName, '') IS NULL THEN bp.LastName ELSE bp.FirstName + ' ' +  bp.LastName END)
    ELSE (CASE WHEN NULLIF(wp.FirstName, '') IS NULL THEN wp.LastName ELSE wp.FirstName + ' ' +  wp.LastName END)
END AS OppName,
AVG(CASE WHEN g.WhitePlayerID = {playerid} THEN g.BlackElo ELSE g.WhiteElo END) AS OppRating,
SUM(CASE WHEN m.Move_Rank = 1 THEN 1 ELSE 0 END) AS EVM,
COUNT(m.MoveNumber) AS ScoredMoves,
AVG(m.ScACPL) AS ACPL,
ISNULL(STDEV(m.ScACPL), 0) AS SDCPL,
CASE
    WHEN ISNULL(100*SUM(ms.ScoreValue)/NULLIF(SUM(ms.MaxScoreValue), 0), 100) > 100 THEN 100
    ELSE ISNULL(100*SUM(ms.ScoreValue)/NULLIF(SUM(ms.MaxScoreValue), 0), 100)
END AS Score

FROM ChessWarehouse.lake.Moves m
JOIN ChessWarehouse.stat.MoveScores ms ON
    m.GameID = ms.GameID AND
    m.MoveNumber = ms.MoveNumber AND
    m.ColorID = ms.ColorID
JOIN ChessWarehouse.lake.Games g ON
    m.GameID = g.GameID
JOIN ChessWarehouse.dim.Colors c ON
    m.ColorID = c.ColorID
JOIN ChessWarehouse.dim.Players wp ON
    g.WhitePlayerID = wp.PlayerID
JOIN ChessWarehouse.dim.Players bp	ON
    g.BlackPlayerID = bp.PlayerID

WHERE g.GameDate BETWEEN '{startdate}' AND '{enddate}'
AND (g.WhitePlayerID = {playerid} OR g.BlackPlayerID = {playerid})
AND (CASE WHEN c.Color = 'White' THEN g.WhitePlayerID ELSE g.BlackPlayerID END) = {playerid}
AND ms.ScoreID = {scoreid}
AND m.MoveScored = 1

GROUP BY
g.GameID,
g.RoundNum,
CASE WHEN g.WhitePlayerID = {playerid} THEN 'w' ELSE 'b' END,
CASE
    WHEN g.WhitePlayerID = {playerid} AND g.Result = 1 THEN 'W'
    WHEN g.BlackPlayerID = {playerid} AND g.Result = 0 THEN 'W'
    WHEN g.WhitePlayerID = {playerid} AND g.Result = 0 THEN 'L'
    WHEN g.BlackPlayerID = {playerid} AND g.Result = 1 THEN 'L'
    ELSE 'D'
END,
CASE
    WHEN g.WhitePlayerID = {playerid} THEN (CASE WHEN NULLIF(bp.FirstName, '') IS NULL THEN bp.LastName ELSE bp.FirstName + ' ' +  bp.LastName END)
    ELSE (CASE WHEN NULLIF(wp.FirstName, '') IS NULL THEN wp.LastName ELSE wp.FirstName + ' ' +  wp.LastName END)
END

ORDER BY 1
"""
    return qry


def player_playersummary(playerid, startdate, enddate, scoreid):
    qry = f"""
SELECT
(CASE WHEN c.Color = 'White' THEN wp.FirstName ELSE bp.FirstName END) + ' ' + (CASE WHEN c.Color = 'White' THEN wp.LastName ELSE bp.LastName END) AS Name,
AVG(CASE WHEN c.Color = 'White' THEN g.WhiteElo ELSE g.BlackElo END) Rating,
e.Record,
e.GamesPlayed,
e.Perf,
SUM(CASE WHEN m.Move_Rank = 1 THEN 1 ELSE 0	END) AS EVM,
SUM(CASE WHEN m.CP_Loss >= 2 THEN 1 ELSE 0 END) AS Blunders,
COUNT(m.MoveNumber) AS ScoredMoves,
AVG(m.ScACPL) AS ACPL,
ISNULL(STDEV(m.ScACPL), 0) AS SDCPL,
CASE
    WHEN ISNULL(100*SUM(ms.ScoreValue)/NULLIF(SUM(ms.MaxScoreValue), 0), 100) > 100 THEN 100
    ELSE ISNULL(100*SUM(ms.ScoreValue)/NULLIF(SUM(ms.MaxScoreValue), 0), 100)
END AS Score,
opp.OppEVM,
opp.OppBlunders,
opp.OppScoredMoves,
opp.OppACPL,
opp.OppSDCPL,
opp.OppScore

FROM ChessWarehouse.lake.Moves m
JOIN ChessWarehouse.stat.MoveScores ms ON
    m.GameID = ms.GameID AND
    m.MoveNumber = ms.MoveNumber AND
    m.ColorID = ms.ColorID
JOIN ChessWarehouse.lake.Games g ON
    m.GameID = g.GameID
JOIN ChessWarehouse.dim.Players wp ON
    g.WhitePlayerID = wp.PlayerID
JOIN ChessWarehouse.dim.Players bp ON
    g.BlackPlayerID = bp.PlayerID
JOIN ChessWarehouse.dim.Colors c ON
    m.ColorID = c.ColorID
JOIN (
    SELECT
    CASE WHEN WhitePlayerID = '{playerid}' THEN WhitePlayerID ELSE BlackPlayerID END AS PlayerID,
    SUM(CASE WHEN BlackPlayerID = '{playerid}' THEN 1 - Result ELSE Result END) AS Record,
    COUNT(GameID) AS GamesPlayed,
    dbo.GetPerfRating(
        AVG(CASE WHEN WhitePlayerID = '{playerid}' THEN BlackElo ELSE WhiteElo END),
        SUM(CASE WHEN BlackPlayerID = '{playerid}' THEN 1 - Result ELSE Result END)/COUNT(GameID)
    ) - AVG(CASE WHEN WhitePlayerID = '{playerid}' THEN WhiteElo ELSE BlackElo END) AS Perf

    FROM ChessWarehouse.lake.Games

    WHERE (WhitePlayerID = '{playerid}' OR BlackPlayerID = '{playerid}')
    AND GameDate BETWEEN '{startdate}' AND '{enddate}'

    GROUP BY
    CASE WHEN WhitePlayerID = '{playerid}' THEN WhitePlayerID ELSE BlackPlayerID END
) e ON
    (CASE WHEN c.Color = 'White' THEN g.WhitePlayerID ELSE g.BlackPlayerID END) = e.PlayerID
JOIN (
    SELECT
    CASE WHEN c.Color = 'White' THEN g.BlackPlayerID ELSE g.WhitePlayerID END AS OppPlayerID,
    SUM(CASE WHEN m.Move_Rank = 1 THEN 1 ELSE 0	END) AS OppEVM,
    SUM(CASE WHEN m.CP_Loss >= 2 THEN 1 ELSE 0 END) AS OppBlunders,
    COUNT(m.MoveNumber) AS OppScoredMoves,
    AVG(m.ScACPL) AS OppACPL,
    ISNULL(STDEV(m.ScACPL), 0) AS OppSDCPL,
    CASE
        WHEN ISNULL(100*SUM(ms.ScoreValue)/NULLIF(SUM(ms.MaxScoreValue), 0), 100) > 100 THEN 100
        ELSE ISNULL(100*SUM(ms.ScoreValue)/NULLIF(SUM(ms.MaxScoreValue), 0), 100)
    END AS OppScore

    FROM ChessWarehouse.lake.Moves m
    JOIN ChessWarehouse.stat.MoveScores ms ON
        m.GameID = ms.GameID AND
        m.MoveNumber = ms.MoveNumber AND
        m.ColorID = ms.ColorID
    JOIN ChessWarehouse.lake.Games g ON
        m.GameID = g.GameID
    JOIN ChessWarehouse.dim.Colors c ON
        m.ColorID = c.ColorID

    WHERE (
        (g.WhitePlayerID = '{playerid}' AND c.Color = 'Black') OR
        (g.BlackPlayerID = '{playerid}' AND c.Color = 'White')
    )
    AND g.GameDate BETWEEN '{startdate}' AND '{enddate}'
    AND ms.ScoreID = {scoreid}
    AND m.MoveScored = 1

    GROUP BY
    CASE WHEN c.Color = 'White' THEN g.BlackPlayerID ELSE g.WhitePlayerID END
) opp ON
    (CASE WHEN c.Color = 'White' THEN g.WhitePlayerID ELSE g.BlackPlayerID END) = opp.OppPlayerID

WHERE (CASE WHEN c.Color = 'White' THEN g.WhitePlayerID ELSE g.BlackPlayerID END) = '{playerid}'
AND g.GameDate BETWEEN '{startdate}' AND '{enddate}'
AND ms.ScoreID = {scoreid}
AND m.MoveScored = 1

GROUP BY
(CASE WHEN c.Color = 'White' THEN wp.FirstName ELSE bp.FirstName END) + ' ' + (CASE WHEN c.Color = 'White' THEN wp.LastName ELSE bp.LastName END),
e.Record,
e.GamesPlayed,
e.Perf,
opp.OppEVM,
opp.OppBlunders,
opp.OppScoredMoves,
opp.OppACPL,
opp.OppSDCPL,
opp.OppScore
"""
    return qry


def player_scoredmoves(playerid, startdate, enddate):
    qry = f"""
SELECT
NULL AS AvgScoredRating,
COUNT(m.MoveNumber) AS ScoredMoves,
SUM(CASE WHEN m.Move_Rank <= 1 THEN 1 ELSE 0 END) AS T1,
SUM(CASE WHEN m.Move_Rank <= 2 THEN 1 ELSE 0 END) AS T2,
SUM(CASE WHEN m.Move_Rank <= 3 THEN 1 ELSE 0 END) AS T3,
SUM(CASE WHEN m.Move_Rank <= 4 THEN 1 ELSE 0 END) AS T4,
SUM(CASE WHEN m.Move_Rank <= 5 THEN 1 ELSE 0 END) AS T5,
AVG(m.ScACPL) AS ACPL,
ISNULL(STDEV(m.ScACPL), 0) AS SDCPL,
SUM(CASE WHEN m.CP_Loss > 2 THEN 1 ELSE 0 END) AS Blunders

FROM ChessWarehouse.lake.Moves m
JOIN ChessWarehouse.lake.Games g ON
    m.GameID = g.GameID
JOIN ChessWarehouse.dim.Colors c ON
    m.ColorID = c.ColorID

WHERE (CASE WHEN c.Color = 'White' THEN g.WhitePlayerID ELSE g.BlackPlayerID END) = '{playerid}'
AND g.GameDate BETWEEN '{startdate}' AND '{enddate}'
AND m.MoveScored = 1
"""
    return qry


def player_totalmoves(playerid, startdate, enddate):
    qry = f"""
SELECT
'Total' AS TraceKey,
'Total Moves' AS TraceDescription,
COUNT(m.MoveNumber) AS MoveCount

FROM ChessWarehouse.lake.Moves m
JOIN ChessWarehouse.lake.Games g ON
    m.GameID = g.GameID
JOIN ChessWarehouse.dim.Colors c ON
    m.ColorID = c.ColorID

WHERE (CASE WHEN c.Color = 'White' THEN g.WhitePlayerID ELSE g.BlackPlayerID END) = '{playerid}'
AND g.GameDate BETWEEN '{startdate}' AND '{enddate}'

UNION

SELECT
t.TraceKey,
t.TraceDescription,
COUNT(m.MoveNumber) AS MoveCount

FROM ChessWarehouse.lake.Moves m
JOIN ChessWarehouse.lake.Games g ON
    m.GameID = g.GameID
JOIN ChessWarehouse.dim.Traces t ON
    m.TraceKey = t.TraceKey
JOIN ChessWarehouse.dim.Colors c ON
    m.ColorID = c.ColorID

WHERE (CASE WHEN c.Color = 'White' THEN g.WhitePlayerID ELSE g.BlackPlayerID END) = '{playerid}'
AND g.GameDate BETWEEN '{startdate}' AND '{enddate}'
AND t.TraceKey NOT IN ('0', 'M')

GROUP BY
t.TraceKey,
t.TraceDescription

ORDER BY
COUNT(m.MoveNumber) DESC
"""
    return qry


def player_totalscore(playerid, startdate, enddate, scoreid):
    qry = f"""
SELECT
CASE
    WHEN ISNULL(100*SUM(ms.ScoreValue)/NULLIF(SUM(ms.MaxScoreValue), 0), 100) > 100 THEN 100
    ELSE ISNULL(100*SUM(ms.ScoreValue)/NULLIF(SUM(ms.MaxScoreValue), 0), 100)
END AS Score

FROM ChessWarehouse.lake.Moves m
JOIN ChessWarehouse.stat.MoveScores ms ON
    m.GameID = ms.GameID AND
    m.MoveNumber = ms.MoveNumber AND
    m.ColorID = ms.ColorID
JOIN ChessWarehouse.lake.Games g ON
    m.GameID = g.GameID
JOIN ChessWarehouse.dim.Colors c ON
    m.ColorID = c.ColorID

WHERE (CASE WHEN c.Color = 'White' THEN g.WhitePlayerID ELSE g.BlackPlayerID END) = '{playerid}'
AND g.GameDate BETWEEN '{startdate}' AND '{enddate}'
AND ms.ScoreID = {scoreid}
AND m.MoveScored = 1
"""
    return qry


def cpl_outlier(srcid, tcid, agg, stat, rating, colorid=None):
    if agg == 'Event':
        srcid, tcid = 3, 5
    qry = f"""
SELECT
ss.Average,
ss.StandardDeviation,
ss.MinValue

FROM ChessWarehouse.stat.StatisticsSummary ss
JOIN ChessWarehouse.dim.Aggregations agg ON
    ss.AggregationID = agg.AggregationID
JOIN ChessWarehouse.dim.Measurements ms ON
    ss.MeasurementID = ms.MeasurementID
LEFT JOIN ChessWarehouse.dim.Colors c ON
    ss.ColorID = c.ColorID

WHERE ss.SourceID = {srcid}
AND ss.TimeControlID = {tcid}
AND agg.AggregationName = '{agg}'
AND ms.MeasurementName = '{stat}'
AND ss.RatingID = {rating}
"""
    if colorid:
        qry = qry + f'AND c.ColorID = {colorid}'
    return qry


def evm_outlier(srcid, tcid, agg, rating, colorid=None):
    if agg == 'Event':
        srcid, tcid = 3, 5
    qry = f"""
SELECT
100*ss.Average AS Average,
100*ss.StandardDevIation AS StandardDeviation,
100*ss.MaxValue AS MaxValue

FROM ChessWarehouse.stat.StatisticsSummary ss
JOIN ChessWarehouse.dim.Aggregations agg ON
    ss.AggregationID = agg.AggregationID
JOIN ChessWarehouse.dim.Measurements ms ON
    ss.MeasurementID = ms.MeasurementID
LEFT JOIN ChessWarehouse.dim.Colors c ON
    ss.ColorID = c.ColorID

WHERE ss.SourceID = {srcid}
AND ss.TimeControlID = {tcid}
AND ms.MeasurementName = 'T1'
AND agg.AggregationName = '{agg}'
AND ss.RatingID = {rating}
"""
    if colorid:
        qry = qry + f'AND c.ColorID = {colorid}'
    return qry


def get_statavg(conn, srcid, aggid, rating, tcid, colorid, egid, typ):
    qry = f"""
SELECT
ss.Average

FROM ChessWarehouse.stat.StatisticsSummary ss
JOIN ChessWarehouse.dim.Measurements m ON
    ss.MeasurementID = m.MeasurementID

WHERE ss.SourceID = {srcid}
AND ss.AggregationID = {aggid}
AND ss.RatingID = {rating}
AND ss.TimeControlID = {tcid}
AND ss.ColorID = {colorid}
AND ss.EvaluationGroupID = {egid}
AND m.MeasurementName = '{typ}'
"""
    val = pd.read_sql(qry, conn).values[0][0]
    return val


def get_statcovar(conn, srcid, aggid, rating, tcid, colorid, egid, typ1, typ2):
    qry = f"""
SELECT
cv.Covariance

FROM ChessWarehouse.stat.Covariances cv
JOIN ChessWarehouse.dim.Measurements m1 ON
    cv.MeasurementID1 = m1.MeasurementID
JOIN ChessWarehouse.dim.Measurements m2 ON
    cv.MeasurementID2 = m2.MeasurementID

WHERE cv.SourceID = {srcid}
AND cv.AggregationID = {aggid}
AND cv.RatingID = {rating}
AND cv.TimeControlID = {tcid}
AND cv.ColorID = {colorid}
AND cv.EvaluationGroupID = {egid}
AND m1.MeasurementName = '{typ1}'
AND m2.MeasurementName = '{typ2}'
"""
    val = pd.read_sql(qry, conn).values[0][0]
    return val


def zscore_data(agg, srcid, tcid, rating, colorid=None):
    qry = f"""
SELECT
ms.MeasurementName,
ss.Average,
ss.StandardDeviation

FROM ChessWarehouse.stat.StatisticsSummary ss
JOIN ChessWarehouse.dim.Sources s ON
    ss.SourceID = s.SourceID
JOIN ChessWarehouse.dim.Aggregations agg ON
    ss.AggregationID = agg.AggregationID
JOIN ChessWarehouse.dim.Measurements ms ON
    ss.MeasurementID = ms.MeasurementID
JOIN ChessWarehouse.dim.TimeControls tc ON
    ss.TimeControlID = tc.TimeControlID
LEFT JOIN ChessWarehouse.dim.Colors c ON
    ss.ColorID = c.ColorID

WHERE agg.AggregationName = '{agg}'
AND ms.MeasurementName IN ('T1', 'ScACPL', 'Score')
AND ss.SourceID = {srcid}
AND ss.TimeControlID = {tcid}
AND ss.RatingID = {rating}
"""
    if colorid:
        qry = qry + f'AND c.ColorID = {colorid}'
    return qry
