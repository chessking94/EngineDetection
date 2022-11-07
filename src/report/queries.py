import pandas as pd


def get_aggid(conn, agg):
    qry = f"""
SELECT
AggregationID

FROM dim.Aggregations

WHERE AggregationName = '{agg}'
"""
    idval = int(pd.read_sql(qry, conn).values[0][0])
    return idval


def get_evid(conn, srcid, event):
    qry = f"""
SELECT
EventID

FROM dim.Events

WHERE SourceID = {srcid}
AND EventName = '{event}'
"""
    idval = int(pd.read_sql(qry, conn).values[0][0])
    return idval


def get_plid(conn, srcid, lname, fname):
    qry = f"""
SELECT
PlayerID

FROM dim.Players

WHERE SourceID = {srcid}
AND LastName = '{lname}'
AND FirstName = '{fname}'
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


def event_avgrating(eventid):
    qry = f"""
SELECT
ROUND(AVG((WhiteElo + BlackElo)/2), 0) AS AvgGameRating,
ROUND(MIN((WhiteElo + BlackElo)/2), 0) AS AvgGameRatingMin,
ROUND(MAX((WhiteElo + BlackElo)/2), 0) AS AvgGameRatingMax

FROM lake.Games

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

FROM lake.Moves m
JOIN lake.Games g ON
    m.GameID = g.GameID
JOIN dim.Colors c ON
    m.ColorID = c.ColorID
JOIN dim.Players wp ON
    g.WhitePlayerID = wp.PlayerID
JOIN dim.Players bp ON
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


def event_playeropp(playerid, eventid):
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
CASE WHEN ISNULL(100*SUM(m.Score)/NULLIF(SUM(m.MaxScore), 0), 100) > 100 THEN 100 ELSE ISNULL(100*SUM(m.Score)/NULLIF(SUM(m.MaxScore), 0), 100) END AS Score

FROM lake.Moves m
JOIN lake.Games g ON
    m.GameID = g.GameID
JOIN dim.Colors c ON
    m.ColorID = c.ColorID
JOIN dim.Players wp ON
    g.WhitePlayerID = wp.PlayerID
JOIN dim.Players bp ON
    g.BlackPlayerID = bp.PlayerID

WHERE g.EventID = {eventid}
AND (g.WhitePlayerID = {playerid} OR g.BlackPlayerID = {playerid})
AND (CASE WHEN c.Color = 'White' THEN g.WhitePlayerID ELSE g.BlackPlayerID END) = {playerid}
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


def event_playersummary(eventid):
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
CASE WHEN ISNULL(100*SUM(m.Score)/NULLIF(SUM(m.MaxScore), 0), 100) > 100 THEN 100 ELSE ISNULL(100*SUM(m.Score)/NULLIF(SUM(m.MaxScore), 0), 100) END AS Score,
opp.OppEVM,
opp.OppBlunders,
opp.OppScoredMoves,
opp.OppACPL,
opp.OppSDCPL,
opp.OppScore

FROM lake.Moves m
JOIN lake.Games g ON
    m.GameID = g.GameID
JOIN dim.Colors c ON
    m.ColorID = c.ColorID
JOIN dim.Players wp ON
    g.WhitePlayerID = wp.PlayerID
JOIN dim.Players bp ON
    g.BlackPlayerID = bp.PlayerID
JOIN (
    SELECT
    EventID,
    PlayerID,
    SUM(ColorResult) AS Record,
    COUNT(GameID) AS GamesPlayed,
    dbo.GetPerfRating(AVG(OppElo), SUM(ColorResult)/COUNT(GameID)) - AVG(Elo) AS Perf

    FROM lake.vwEventBreakdown

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
    CASE WHEN ISNULL(100*SUM(m.Score)/NULLIF(SUM(m.MaxScore), 0), 100) > 100 THEN 100 ELSE ISNULL(100*SUM(m.Score)/NULLIF(SUM(m.MaxScore), 0), 100) END AS OppScore

    FROM lake.Moves m
    JOIN lake.Games g ON
        m.GameID = g.GameID
    JOIN dim.Colors c ON
        m.ColorID = c.ColorID

    WHERE m.MoveScored = 1

    GROUP BY
    CASE WHEN c.Color = 'White' THEN g.BlackPlayerID ELSE g.WhitePlayerID END,
    g.EventID
) opp ON
    (CASE WHEN c.Color = 'White' THEN g.WhitePlayerID ELSE g.BlackPlayerID END) = opp.OppPlayerID AND
    g.EventID = opp.EventID

WHERE g.EventID = {eventid}
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

FROM lake.Moves m
JOIN lake.Games g ON
    m.GameID = g.GameID
JOIN dim.Colors c ON
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

FROM lake.Games g
JOIN dim.Events e ON
    g.EventID = e.EventID

WHERE e.EventName = '{event}'

GROUP BY
e.EventName
"""
    return qry


def event_totalmoves(eventid):
    qry = f"""
SELECT
COUNT(m.MoveNumber) AS TotalMoves

FROM lake.Moves m
JOIN lake.Games g ON
    m.GameID = g.GameID

WHERE g.EventID = {eventid}
"""
    return qry


def event_totalscore(eventid):
    qry = f"""
SELECT
CASE WHEN ISNULL(100*SUM(m.Score)/NULLIF(SUM(m.MaxScore), 0), 100) > 100 THEN 100 ELSE ISNULL(100*SUM(m.Score)/NULLIF(SUM(m.MaxScore), 0), 100) END AS Score

FROM lake.Moves m
JOIN lake.Games g ON
    m.GameID = g.GameID

WHERE g.EventID = {eventid}
AND m.MoveScored = 1
"""
    return qry


def game_trace(gameid, colorid):
    qry = f"""
SELECT
m.MoveNumber,
CASE
    WHEN m.IsTheory = 1 THEN 'b'
    WHEN m.IsTablebase = 1 THEN 't'
    WHEN m.T1_Eval_POV IS NULL OR ABS(m.T1_Eval_POV) > CAST(s.Value AS decimal(5,2)) THEN 'e'
    WHEN m.Move_Rank = 1 THEN 'M'
    ELSE '0'
END AS MoveTrace

FROM lake.Moves m
JOIN dim.Colors c ON
    m.ColorID = c.ColorID
CROSS JOIN Settings s

WHERE m.GameID = {gameid}
AND c.ColorID = '{colorid}'
AND s.ID = 3

ORDER BY 1
"""
    return qry


def max_eval():
    qry = """
SELECT
CAST(Value AS decimal(5,2))*100 AS Max_Eval

FROM Settings

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

    FROM lake.Games

    WHERE WhitePlayerID = '{playerid}'
    AND GameDate BETWEEN '{startdate}' AND '{enddate}'

    UNION ALL

    SELECT
    NULLIF(NULLIF(BlackElo, ''), 0) AS Elo,
    NULLIF(NULLIF(WhiteElo, ''), 0) AS OppElo

    FROM lake.Games
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

FROM lake.Moves m
JOIN lake.Games g ON
    m.GameID = g.GameID
JOIN dim.Colors c ON
    m.ColorID = c.ColorID
JOIN dim.Players wp ON
    g.WhitePlayerID = wp.PlayerID
JOIN dim.Players bp ON
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


def player_playeropp(playerid, startdate, enddate):
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
CASE WHEN ISNULL(100*SUM(m.Score)/NULLIF(SUM(m.MaxScore), 0), 100) > 100 THEN 100 ELSE ISNULL(100*SUM(m.Score)/NULLIF(SUM(m.MaxScore), 0), 100) END AS Score

FROM lake.Moves m
JOIN lake.Games g ON
    m.GameID = g.GameID
JOIN dim.Colors c ON
    m.ColorID = c.ColorID
JOIN dim.Players wp ON
    g.WhitePlayerID = wp.PlayerID
JOIN dim.Players bp	ON
    g.BlackPlayerID = bp.PlayerID

WHERE g.GameDate BETWEEN '{startdate}' AND '{enddate}'
AND (g.WhitePlayerID = {playerid} OR g.BlackPlayerID = {playerid})
AND (CASE WHEN c.Color = 'White' THEN g.WhitePlayerID ELSE g.BlackPlayerID END) = {playerid}
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


def player_playersummary(playerid, startdate, enddate):
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
CASE WHEN ISNULL(100*SUM(m.Score)/NULLIF(SUM(m.MaxScore), 0), 100) > 100 THEN 100 ELSE ISNULL(100*SUM(m.Score)/NULLIF(SUM(m.MaxScore), 0), 100) END AS Score,
opp.OppEVM,
opp.OppBlunders,
opp.OppScoredMoves,
opp.OppACPL,
opp.OppSDCPL,
opp.OppScore

FROM lake.Moves m
JOIN lake.Games g ON
    m.GameID = g.GameID
JOIN dim.Players wp ON
    g.WhitePlayerID = wp.PlayerID
JOIN dim.Players bp ON
    g.BlackPlayerID = bp.PlayerID
JOIN dim.Colors c ON
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

    FROM lake.Games

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
    CASE WHEN ISNULL(100*SUM(m.Score)/NULLIF(SUM(m.MaxScore), 0), 100) > 100 THEN 100 ELSE ISNULL(100*SUM(m.Score)/NULLIF(SUM(m.MaxScore), 0), 100) END AS OppScore

    FROM lake.Moves m
    JOIN lake.Games g ON
        m.GameID = g.GameID
    JOIN dim.Colors c ON
        m.ColorID = c.ColorID

    WHERE (
        (g.WhitePlayerID = '{playerid}' AND c.Color = 'Black') OR
        (g.BlackPlayerID = '{playerid}' AND c.Color = 'White')
    )
    AND g.GameDate BETWEEN '{startdate}' AND '{enddate}'
    AND m.MoveScored = 1

    GROUP BY
    CASE WHEN c.Color = 'White' THEN g.BlackPlayerID ELSE g.WhitePlayerID END
) opp ON
    (CASE WHEN c.Color = 'White' THEN g.WhitePlayerID ELSE g.BlackPlayerID END) = opp.OppPlayerID

WHERE (CASE WHEN c.Color = 'White' THEN g.WhitePlayerID ELSE g.BlackPlayerID END) = '{playerid}'
AND g.GameDate BETWEEN '{startdate}' AND '{enddate}'
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

FROM lake.Moves m
JOIN lake.Games g ON
    m.GameID = g.GameID
JOIN dim.Colors c ON
    m.ColorID = c.ColorID

WHERE (CASE WHEN c.Color = 'White' THEN g.WhitePlayerID ELSE g.BlackPlayerID END) = '{playerid}'
AND g.GameDate BETWEEN '{startdate}' AND '{enddate}'
AND m.MoveScored = 1
"""
    return qry


def player_totalmoves(playerid, startdate, enddate):
    qry = f"""
SELECT
COUNT(m.MoveNumber) AS TotalMoves

FROM lake.Moves m
JOIN lake.Games g ON
    m.GameID = g.GameID
JOIN dim.Colors c ON
    m.ColorID = c.ColorID

WHERE (CASE WHEN c.Color = 'White' THEN g.WhitePlayerID ELSE g.BlackPlayerID END) = '{playerid}'
AND g.GameDate BETWEEN '{startdate}' AND '{enddate}'
"""
    return qry


def player_totalscore(playerid, startdate, enddate):
    qry = f"""
SELECT
CASE WHEN ISNULL(100*SUM(m.Score)/NULLIF(SUM(m.MaxScore), 0), 100) > 100 THEN 100 ELSE ISNULL(100*SUM(m.Score)/NULLIF(SUM(m.MaxScore), 0), 100) END AS Score

FROM lake.Moves m
JOIN lake.Games g ON
    m.GameID = g.GameID
JOIN dim.Colors c ON
    m.ColorID = c.ColorID

WHERE (CASE WHEN c.Color = 'White' THEN g.WhitePlayerID ELSE g.BlackPlayerID END) = '{playerid}'
AND g.GameDate BETWEEN '{startdate}' AND '{enddate}'
AND m.MoveScored = 1
"""
    return qry


def roi_calc(agg, src, tc, rating, colorid=None):
    qry = f"""
SELECT
ss.Average,
ss.StandardDeviation,
ss.MaxValue

FROM stat.StatisticsSummary ss
JOIN dim.Sources s ON
    ss.SourceID = s.SourceID
JOIN dim.Aggregations agg ON
    ss.AggregationID = agg.AggregationID
JOIN dim.Measurements ms ON
    ss.MeasurementID = ms.MeasurementID
JOIN dim.TimeControls tc ON
    ss.TimeControlID = tc.TimeControlID
LEFT JOIN dim.Colors c ON
    ss.ColorID = c.ColorID

WHERE agg.AggregationName = '{agg}'
AND ms.MeasurementName = 'Score'
AND s.SourceName = '{src}'
AND tc.TimeControlName = '{tc}'
AND ss.RatingID = {rating}
"""
    if colorid:
        qry = qry + f"AND c.ColorID = {colorid}"
    return qry


def cpl_outlier(agg, stat, rating, colorid=None):
    qry = f"""
SELECT
ss.Average,
ss.StandardDeviation,
ss.MinValue

FROM stat.StatisticsSummary ss
JOIN dim.Sources s ON
    ss.SourceID = s.SourceID
JOIN dim.Aggregations agg ON
    ss.AggregationID = agg.AggregationID
JOIN dim.Measurements ms ON
    ss.MeasurementID = ms.MeasurementID
JOIN dim.TimeControls tc ON
    ss.TimeControlID = tc.TimeControlID
LEFT JOIN dim.Colors c ON
    ss.ColorID = c.ColorID

WHERE s.SourceName = 'Control'
AND tc.TimeControlName = 'Classical'
AND agg.AggregationName = '{agg}'
AND ms.MeasurementName = '{stat}'
AND ss.RatingID = {rating}
"""
    if colorid:
        qry = qry + f"AND c.ColorID = {colorid}"
    return qry


def evm_outlier(agg, rating, colorid=None):
    qry = f"""
SELECT
100*ss.Average AS Average,
100*ss.StandardDevIation AS StandardDeviation,
100*ss.MaxValue AS MaxValue

FROM stat.StatisticsSummary ss
JOIN dim.Sources s ON
    ss.SourceID = s.SourceID
JOIN dim.Aggregations agg ON
    ss.AggregationID = agg.AggregationID
JOIN dim.Measurements ms ON
    ss.MeasurementID = ms.MeasurementID
JOIN dim.TimeControls tc ON
    ss.TimeControlID = tc.TimeControlID
LEFT JOIN dim.Colors c ON
    ss.ColorID = c.ColorID

WHERE s.SourceName = 'Control'
AND tc.TimeControlName = 'Classical'
AND ms.MeasurementName = 'T1'
AND agg.AggregationName = '{agg}'
AND ss.RatingID = {rating}
"""
    if colorid:
        qry = qry + f"AND c.ColorID = '{colorid}'"
    return qry


def get_statavg(conn, srcid, aggid, rating, tcid, colorid, egid, typ):
    qry = f"""
SELECT
ss.Average

FROM stat.StatisticsSummary ss
JOIN dim.Measurements m ON
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

FROM stat.Covariances cv
JOIN dim.Measurements m1 ON
    cv.MeasurementID1 = m1.MeasurementID
JOIN dim.Measurements m2 ON
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
    # print(qry)
    val = pd.read_sql(qry, conn).values[0][0]
    return val
